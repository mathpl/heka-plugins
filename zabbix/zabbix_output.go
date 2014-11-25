package plugins

import (
	"bytes"
	"fmt"
	"os"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

// Output plugin that sends messages via TCP using the Heka protocol.
type ZabbixOutput struct {
	conf        *ZabbixOutputConfig
	key_filter  map[string]*HostActiveChecks
	zabbix_conn ZabbixConn
}

type HostActiveChecks struct {
	Keys map[string]int
}

// ConfigStruct for ZabbixOutputstruct plugin.
type ZabbixOutputConfig struct {
	// Zabbix server address
	Address string `toml:"address"`
	// Maximum interval between each send
	TickerInterval uint `toml:"ticker_interval"`
	// Time between each update from the zabbix server for key filtering
	ZabbixChecksPollInterval uint `toml:"zabbix_checks_poll_interval"`
	// Maximum key count retained when zabbix doesn't respond
	MaxKeyCount uint `toml:"max_key_count"`
	// This many keys will trigger a send
	SendKeyCount uint `toml:"send_key_count"`
	// Encoder to use
	Encoder string `toml:"encoder"`
	// Read deadline in ms
	ReceiveTimeout uint `toml:"receive_timeout"`
	// Write deadline in ms
	SendTimeout uint `toml:"send_timeout"`
	// Override hostname
	OverrideHostname string `toml:"override_hostname"`
}

func (zo *ZabbixOutput) ConfigStruct() interface{} {
	return &ZabbixOutputConfig{
		Encoder:                  "OpentsdbToZabbix",
		TickerInterval:           uint(15),
		ZabbixChecksPollInterval: uint(300),
		ReceiveTimeout:           uint(3),
		SendTimeout:              uint(1),
		SendKeyCount:             uint(1000),
		MaxKeyCount:              uint(2000),
	}
}

func (zo *ZabbixOutput) Init(config interface{}) (err error) {
	zo.conf = config.(*ZabbixOutputConfig)

	zo.zabbix_conn, err = NewZabbixConn(zo.conf.Address, zo.conf.ReceiveTimeout, zo.conf.SendTimeout)
	zo.key_filter = make(map[string]*HostActiveChecks)
	if zo.conf.OverrideHostname != "" {
		zo.key_filter[zo.conf.OverrideHostname] = nil
	} else {
		var host string
		host, err = os.Hostname()
		zo.key_filter[host] = nil
	}

	// A bit of config validation
	if zo.conf.MaxKeyCount < zo.conf.SendKeyCount || zo.conf.SendKeyCount < 1 {
		err = fmt.Errorf("Invalid combinason of send_key_count and max_key_count: %d must be <= %d", zo.conf.SendKeyCount, zo.conf.MaxKeyCount)
	}

	if zo.conf.ZabbixChecksPollInterval <= zo.conf.ReceiveTimeout/1000 {
		err = fmt.Errorf("Invalid combinason of zabbix_checks_poll_interval and receive_timeout: %d must > %d", zo.conf.SendKeyCount, zo.conf.MaxKeyCount)
	}

	return
}

func (zo *ZabbixOutput) SendRecords(records [][]byte) (data_left [][]byte, err error) {
	defer zo.zabbix_conn.cleanupConn()

	//FIXME: Proper json encoding
	msgHeader := []byte("{\"request\":\"agent data\",\"data\":[")
	msgHeaderLength := len(msgHeader)

	msgClose := []byte("]}")
	msgCloseLength := len(msgClose)

	data_left = records

	for len(data_left) > 0 {
		length := 0
		if len(data_left) >= int(zo.conf.SendKeyCount) {
			length = int(zo.conf.SendKeyCount)
		} else {
			length = len(data_left)
		}

		joinedRecords := bytes.Join(data_left[:length], []byte(","))
		msgArray := make([]byte, msgHeaderLength+length+msgCloseLength)

		msgSlice := msgArray[0:0]
		msgSlice = append(msgSlice, msgHeader...)
		msgSlice = append(msgSlice, joinedRecords...)
		msgSlice = append(msgSlice, msgClose...)

		if err = zo.zabbix_conn.ZabbixSend(msgSlice); err != nil {
			return data_left, err
		}

		// Disconnect between batches
		zo.zabbix_conn.cleanupConn()

		// Move down the slice
		data_left = data_left[length:]
	}

	return
}

func (zo *ZabbixOutput) Filter(pack *PipelinePack) (discard bool, err error) {
	var (
		val   interface{}
		key   string
		host  string
		ok    bool
		found bool
	)

	discard = true

	if val, found = pack.Message.GetFieldValue("Key"); !found {
		err = fmt.Errorf("No Key in message")
		pack.Recycle()
		return
	}
	if key, ok = val.(string); !ok {
		err = fmt.Errorf("Unable to cast key to string")
		pack.Recycle()
		return
	}

	if val, found = pack.Message.GetFieldValue("Host"); !found {
		err = fmt.Errorf("No Host in message")
		pack.Recycle()
		return
	}
	if host, ok = val.(string); !ok {
		err = fmt.Errorf("Unable to cast host to string")
		pack.Recycle()
		return
	}

	// Check against active check filter
	if hac, found_host := zo.key_filter[host]; found_host && hac != nil {
		if _, found_key := hac.Keys[key]; found_key {
			discard = false
		}
	} else {
		// We have no data on current host, we'll need to fetch it!
		// Discard by default
		zo.key_filter[host] = nil
	}

	return
}

func (zo *ZabbixOutput) SendMetrics(or OutputRunner, data [][]byte) (new_slice [][]byte, err error) {
	new_slice = data
	if new_slice, err = zo.SendRecords(data); err != nil {
		// If we've hit the max key to send truncate the slice down starting with the oldest
		if len(new_slice) > int(zo.conf.MaxKeyCount) {
			copy(data, new_slice)
			remove_tail := zo.conf.MaxKeyCount - zo.conf.SendKeyCount
			or.LogError(fmt.Errorf("Truncated %d oldest metrics from in-memory buffer.", zo.conf.SendKeyCount))
			new_slice = data[:remove_tail]
		}
		return
	}

	return
}

func (zo *ZabbixOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok     = true
		pack   *PipelinePack
		inChan = or.InChan()
		ticker = or.Ticker()
	)

	updateFilter := make(chan bool, 1)
	go func() {
		updateFilter <- true
		for {
			time.Sleep(time.Duration(zo.conf.ZabbixChecksPollInterval) * time.Second)
			updateFilter <- true
		}
	}()

	dataArray := make([][]byte, zo.conf.MaxKeyCount)
	dataSlice := dataArray[0:0]
	for ok {
		select {
		case <-updateFilter:
			if !ok {
				break
			}

			// FIXME: Move to seperate goroutine so it's non-blocking
			for host, _ := range zo.key_filter {
				if hac, localErr := zo.zabbix_conn.FetchActiveChecks(host); localErr != nil {
					// Keep previous list if the server can't refresh the list of checks
					or.LogError(fmt.Errorf("Zabbix server unable to provide active check list for host %s: %s", host, localErr))
				} else {
					zo.key_filter[host] = &hac
					//fmt.Printf("%s\n%+V\n", host, hac.Keys)
				}
			}

		case pack, ok = <-inChan:
			if !ok {
				break
			}

			if discard, err := zo.Filter(pack); err != nil {
				or.LogError(err)
				pack.Recycle()
				continue
			} else if discard {
				pack.Recycle()
				continue
			}

			if msg, localErr := or.Encode(pack); localErr != nil {
				or.LogError(fmt.Errorf("Encoder failure: %s", localErr))
				pack.Recycle()
				continue
			} else {
				dataSlice = append(dataSlice, msg)
			}
			pack.Recycle()

			if len(dataSlice) >= int(zo.conf.SendKeyCount) {
				if dataSlice, err = zo.SendMetrics(or, dataSlice); err != nil {
					or.LogError(err)
				}
			}

		case <-ticker:
			if len(dataSlice) > 0 {
				if dataSlice, err = zo.SendMetrics(or, dataSlice); err != nil {
					or.LogError(err)
				}
			}
		}
	}

	return
}

func init() {
	RegisterPlugin("ZabbixOutput", func() interface{} {
		return new(ZabbixOutput)
	})
}
