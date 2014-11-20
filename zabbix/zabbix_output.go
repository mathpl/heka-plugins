package plugins

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

// Output plugin that sends messages via TCP using the Heka protocol.
type ZabbixOutput struct {
	conf       *ZabbixOutputConfig
	address    *net.TCPAddr
	connection net.Conn
	hostname   string
	key_filter map[string][]string
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
	// Read deadline
	ReadDeadline uint `toml:"read_deadline"`
	// Override hostname
	OverrideHostname string `toml:"override_hostname"`
}

func (zo *ZabbixOutput) ConfigStruct() interface{} {
	return &ZabbixOutputConfig{
		Encoder:                  "OpentsdbToZabbix",
		TickerInterval:           uint(15),
		ZabbixChecksPollInterval: uint(360),
		ReadDeadline:             uint(3),
	}
}

func (zo *ZabbixOutput) Init(config interface{}) (err error) {
	zo.conf = config.(*ZabbixOutputConfig)

	zo.address, err = net.ResolveTCPAddr("tcp", zo.conf.Address)
	if zo.conf.OverrideHostname != "" {
		zo.hostname = zo.conf.OverrideHostname
	} else {
		zo.hostname, err = os.Hostname()
	}
	zo.key_filter = make(map[string][]string)

	return
}

func (zo *ZabbixOutput) connect() (err error) {
	dialer := &net.Dialer{}
	zo.connection, err = dialer.Dial("tcp", zo.address.String())
	return err
}

func (zo *ZabbixOutput) cleanupConn() {
	if zo.connection != nil {
		zo.connection.Close()
		zo.connection = nil
	}
}

type activeCheckKeyJson struct {
	Key         string `json:"key"`
	Delay       string `json:"delay"`
	Lastlogsize string `json:"lastlogsize"`
	Mtime       string `json:"mtime"`
}

type activeCheckResponseJson struct {
	Response string               `json:"response"`
	Data     []activeCheckKeyJson `json:"Data"`
}

func (zo *ZabbixOutput) FetchActiveChecks(hostList []string) (err error) {
	if zo.connection == nil {
		if err = zo.connect(); err != nil {
			zo.connection = nil
			return
		}
	}

	for _, host := range hostList {
		msg := fmt.Sprintf("{\"request\":\"active checks\",\"host\":\"%s\"}", host)
		data := []byte(msg)

		if err = zo.zabbixSend(data); err != nil {
			return
		} else {
			var result []byte
			if result, err = zo.zabbixReceive(); err != nil {
				return
			} else {
				// Parse json for key names
				var unmarshalledResult activeCheckResponseJson
				err = json.Unmarshal(result, &unmarshalledResult)
				if err != nil {
					return
				}

				// Push key names for the current host
				zo.key_filter[host] = make([]string, len(unmarshalledResult.Data))
				for i, activeCheckKey := range unmarshalledResult.Data {
					zo.key_filter[host][i] = activeCheckKey.Key
				}
			}
		}
	}

	return
}

func (zo *ZabbixOutput) zabbixSend(data []byte) (err error) {
	zbxHeader := []byte("ZBXD\x01")
	// zabbix header + proto version + uint64 length
	zbxHeaderLength := len(zbxHeader) + 8

	dataLength := len(data)

	msgArray := make([]byte, zbxHeaderLength+dataLength)

	msgSlice := msgArray[0:0]
	msgSlice = append(msgSlice, zbxHeader...)

	byteBuff := make([]byte, 8)

	binary.LittleEndian.PutUint64(byteBuff, uint64(dataLength))
	msgSlice = append(msgSlice, byteBuff...)

	msgSlice = append(msgSlice, data...)

	var n int
	if n, err = zo.connection.Write(msgSlice); err != nil {
		zo.cleanupConn()
		err = fmt.Errorf("writing to %s: %s", zo.conf.Address, err)
	} else if n != len(msgSlice) {
		zo.cleanupConn()
		err = fmt.Errorf("truncated output to: %s", zo.conf.Address)
	}

	return
}

func (zo *ZabbixOutput) zabbixReceive() (result []byte, err error) {
	// Get the response!
	zo.connection.SetReadDeadline(time.Now().Add(time.Duration(zo.conf.ReadDeadline) * time.Second))

	// Fetch the header first to get the full length
	header := make([]byte, 13)
	//var header_length int
	if _, err = io.ReadFull(zo.connection, header); err != nil {
		return
	}

	// Check header content
	if string(header[:5]) != "ZBXD\x01" {
		err = fmt.Errorf("Unexpected response header from Zabbix: %s %d %s", string(header[:5]), len(header[:5]), header[:5])
		return
	}

	// Get length from zabbix protocol
	response_length := binary.LittleEndian.Uint64(header[5:13])

	// Get full reponse
	response := make([]byte, response_length)
	var n int
	if n, err = io.ReadFull(zo.connection, response); err != nil {
		return
	}

	if n != int(response_length) {
		err = fmt.Errorf("Unexpected response length from Zabbix header: %s", response_length)
		return
	}

	result = response

	return
}

func (zo *ZabbixOutput) SendRecords(records [][]byte) (err error) {
	if zo.connection == nil {
		if err = zo.connect(); err != nil {
			zo.connection = nil
			return
		}
	}

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
	err = zo.zabbixSend(msgSlice)
	zo.cleanupConn()

	return
}

func (zo *ZabbixOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok     = true
		pack   *PipelinePack
		inChan = or.InChan()
		ticker = or.Ticker()
	)

	defer func() {
		if zo.connection != nil {
			zo.connection.Close()
			zo.connection = nil
		}
	}()

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
			for _, filtered_key := range zo.key_filter[host] {
				if key == filtered_key {
					found = true
					break
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
