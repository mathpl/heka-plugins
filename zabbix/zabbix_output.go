package plugins

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

// Output plugin that sends messages via TCP using the Heka protocol.
type ZabbixOutput struct {
	conf        *ZabbixOutputConfig
	hostname    string
	key_filter  map[string]*hostActiveChecks
	zabbix_conn *ZabbixConn
}

type hostActiveChecks struct {
	keys map[string]uint16
}

// ConfigStruct for ZabbixOutputstruct plugin.
type ZabbixOutputConfig struct {
	// Zabbix server address
	Address string `toml:"address"`
	// Maximum interval between each send
	TickerInterval uint `toml:"ticker_interval"`
	// Time between each update from the zabbix server for key filtering
	ZabbixChecksPollInterval uint `toml:"zabbix_checks_poll_interval"`
	// Maximum key count between each send
	MaxKeyCount uint `toml:"max_key_count"`
	// Encoder to use
	Encoder string `toml:"encoder"`
	// Read deadline in ms
	ReceiveTimeout uint `toml:"receive_timeout"`
	// Override hostname
	OverrideHostname string `toml:"override_hostname"`
}

func (zo *ZabbixOutput) ConfigStruct() interface{} {
	return &ZabbixOutputConfig{
		Encoder:                  "OpentsdbToZabbix",
		TickerInterval:           uint(15),
		ZabbixChecksPollInterval: uint(360),
		ReceiveTimeout:           uint(3),
	}
}

func (zo *ZabbixOutput) Init(config interface{}) (err error) {
	zo.conf = config.(*ZabbixOutputConfig)

	var zc ZabbixConn
	zc.addr, err = net.ResolveTCPAddr("tcp", zo.conf.Address)
	if zo.conf.OverrideHostname != "" {
		zo.hostname = zo.conf.OverrideHostname
	} else {
		zo.hostname, err = os.Hostname()
	}
	zo.key_filter = make(map[string]*hostActiveChecks)
	zc.receive_timeout = time.Duration(zo.conf.ReceiveTimeout) * time.Millisecond
	zo.zabbix_conn = &zc

	return
}

func (zo *ZabbixOutput) SendRecords(records [][]byte) (err error) {
	//FIXME: Proper json encoding
	msgHeader := []byte("{\"request\":\"agent data\",\"data\":[")
	msgHeaderLength := len(msgHeader)

	msgClose := []byte("]}")
	msgCloseLength := len(msgClose)

	joinedRecords := bytes.Join(records, []byte(","))
	msgArray := make([]byte, msgHeaderLength+len(joinedRecords)+msgCloseLength)

	msgSlice := msgArray[0:0]
	msgSlice = append(msgSlice, msgHeader...)
	msgSlice = append(msgSlice, joinedRecords...)
	msgSlice = append(msgSlice, msgClose...)

	//zo.connection.SetReadDeadline(time.Now())
	err = zo.zabbix_conn.ZabbixSend(msgSlice)

	return
}

func (zo *ZabbixOutput) FetchActiveChecks(hostList []string) (err error) {
	for _, host := range hostList {
		msg := fmt.Sprintf("{\"request\":\"active checks\",\"host\":\"%s\"}", host)
		data := []byte(msg)

		if err = zo.zabbix_conn.ZabbixSend(data); err != nil {
			return
		} else {
			var result []byte
			if result, err = zo.zabbix_conn.ZabbixReceive(); err != nil {
				return
			} else {
				// Parse json for key names
				var unmarshalledResult ActiveCheckResponseJson
				//Check what's the result on no keys
				fmt.Printf("%+V\n", result)
				err = json.Unmarshal(result, &unmarshalledResult)
				if err != nil {
					return
				}

				//Clean up current list
				key_filter := make(map[string]*hostActiveChecks, len(zo.key_filter))

				// Push key names for the current host
				for _, activeCheckKey := range unmarshalledResult.Data {
					var hac hostActiveChecks
					// Put 15 as delay if strconv doesn't work for now
					if delay, conv_err := strconv.ParseInt(activeCheckKey.Delay, 10, 8); conv_err != nil {
						hac.keys[activeCheckKey.Key] = uint16(delay)
					} else {
						hac.keys[activeCheckKey.Key] = 15
					}
					key_filter[host] = &hac
				}

				zo.key_filter = key_filter
			}
		}
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

	hostList := make([]string, 1)
	hostList[0] = zo.hostname

	dataArray := make([][]byte, zo.conf.MaxKeyCount)
	dataSlice := dataArray[0:0]
	for ok {
		select {
		case <-updateFilter:
			if !ok {
				break
			}

			if localErr := zo.FetchActiveChecks(hostList); localErr != nil {
				// FIXME: What to do when zabbix doesn't answer?
				or.LogError(fmt.Errorf("Zabbix server enable to provide active check list: %s", localErr))
			}
		case pack, ok = <-inChan:
			if !ok {
				break
			}

			var (
				val   interface{}
				key   string
				host  string
				found bool
			)

			if val, found = pack.Message.GetFieldValue("Key"); !found {
				or.LogError(fmt.Errorf("No Key in message"))
				pack.Recycle()
				continue
			}
			key, _ = val.(string)

			if val, found = pack.Message.GetFieldValue("Host"); !found {
				or.LogError(fmt.Errorf("No Host in message"))
				pack.Recycle()
				continue
			}
			host, _ = val.(string)

			// Check against active check filter
			found = false
			if hac, found_host := zo.key_filter[host]; found_host {
				if _, found_key := hac.keys[key]; found_key {
					found = true
				}
			}

			if !found {
				fmt.Printf("Discarding: %s:%s\n", host, key)
				break
			}

			//FIXME: Add additional hostnames in list
			if msg, localErr := or.Encode(pack); localErr == nil {
				dataSlice = append(dataSlice, msg)
			} else {
				or.LogError(fmt.Errorf("Encoder failure: %s", localErr))
			}

			if len(dataSlice) >= int(zo.conf.MaxKeyCount) {
				if localErr := zo.SendRecords(dataSlice); localErr == nil {
					//FIXME: Overflow control
					dataSlice = dataArray[0:0]
				} else {
					// FIXME: What to do when zabbix doesn't answer?
					or.LogError(fmt.Errorf("Zabbix server to accept data: %s", localErr))
				}
			}
			pack.Recycle()

		case <-ticker:
			if len(dataSlice) > 0 {
				// Remove trailling coma
				if localErr := zo.SendRecords(dataSlice); localErr == nil {
					//FIXME: Overflow control
					dataSlice = dataArray[0:0]
				} else {
					// FIXME: What to do when zabbix doesn't answer?
					or.LogError(fmt.Errorf("Zabbix server to accept data: %s", localErr))
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
