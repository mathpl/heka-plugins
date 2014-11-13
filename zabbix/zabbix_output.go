package plugins

import (
	"bufio"
	"bytes"
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
}

func (zo *ZabbixOutput) ConfigStruct() interface{} {
	return &ZabbixOutputConfig{
		Encoder:                  "OpentsdbToZabbix",
		TickerInterval:           uint(15),
		ZabbixChecksPollInterval: uint(360),
		ReadDeadline:             uint(5),
	}
}

func (zo *ZabbixOutput) Init(config interface{}) (err error) {
	zo.conf = config.(*ZabbixOutputConfig)

	zo.address, err = net.ResolveTCPAddr("tcp", zo.conf.Address)
	zo.hostname, err = os.Hostname()
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

func (zo *ZabbixOutput) FetchActiveChecks(hostList []string) (err error) {
	if zo.connection == nil {
		if err = zo.connect(); err != nil {
			zo.connection = nil
			return
		}
	}

	for _, host := range hostList {
		msg := fmt.Sprintf("{\"request\":\"active checks\",\"host\":\"%s\"", host)
		data := []byte(msg)
		var result string
		if result, err = zo.zabbixSend(data); err != nil {
			return
		} else {
			fmt.Print(result)
		}
	}

	return
}

func (zo *ZabbixOutput) zabbixSend(data []byte) (result string, err error) {
	zbxHeader := []byte("ZBXD\x01")
	// zabbix header + 2 uint
	zbxHeaderLength := len(zbxHeader) + 2

	dataLength := len(data)

	msgArray := make([]byte, zbxHeaderLength+dataLength)

	msgSlice := msgArray[0:0]
	msgSlice = append(msgSlice, zbxHeader...)
	msgSlice = append(msgSlice, byte(uint(dataLength)))
	msgSlice = append(msgSlice, byte(uint(0)))
	msgSlice = append(msgSlice, data...)

	var n int
	if n, err = zo.connection.Write(msgSlice); err != nil {
		zo.cleanupConn()
		err = fmt.Errorf("writing to %s: %s", zo.conf.Address, err)
	} else if n != len(msgSlice) {
		zo.cleanupConn()
		err = fmt.Errorf("truncated output to: %s", zo.conf.Address)
	} else {
		// Get the response!

		zo.connection.SetReadDeadline(time.Now().Add(time.Duration(zo.conf.ReadDeadline) * time.Second))

		buff := make([]byte, 10240)
		if _, err := bufio.NewReader(zo.connection).Read(buff); err == nil || err == io.EOF {
			result = string(buff)
		}
	}

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

	zo.connection.SetReadDeadline(time.Now())
	_, err = zo.zabbixSend(msgSlice)

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

			err = zo.FetchActiveChecks(hostList)
		case pack, ok = <-inChan:
			if !ok {
				break
			}

			if len(dataSlice) >= int(zo.conf.MaxKeyCount) {
				if err = zo.SendRecords(dataSlice); err == nil {
					//FIXME: Overflow control
					dataSlice = dataArray[0:0]
				} else {
					ok = false
				}
			}

			//FIXME: Add additional hostnames in list
			if msg, err := or.Encode(pack); err == nil {
				dataSlice = append(dataSlice, msg)
			} else {
				ok = false
			}

			pack.Recycle()

		case <-ticker:
			if len(dataSlice) > 0 {
				// Remove trailling coma
				if err = zo.SendRecords(dataSlice); err == nil {
					//FIXME: Overflow control
					dataSlice = dataArray[0:0]
				} else {
					ok = false
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
