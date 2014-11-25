package plugins

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type ActiveCheckKeyJson struct {
	Key string `json:"key"`

	//Zabbix 1.8 return these as string, 2+ as int.
	Delay       interface{} `json:"delay"`
	Lastlogsize interface{} `json:"lastlogsize"`
	Mtime       interface{} `json:"mtime"`
}

type ActiveCheckResponseJson struct {
	Response string               `json:"response"`
	Data     []ActiveCheckKeyJson `json:"data"`
}

type ZabbixConn struct {
	conn            net.Conn
	addr            *net.TCPAddr
	receive_timeout time.Duration
	send_timeout    time.Duration
}

func NewZabbixConn(addr string, receive_timeout uint, send_timeout uint) (zc ZabbixConn, err error) {
	zc.addr, err = net.ResolveTCPAddr("tcp", addr)
	zc.receive_timeout = time.Duration(receive_timeout) * time.Millisecond
	zc.send_timeout = time.Duration(send_timeout) * time.Millisecond
	return
}

func (zc *ZabbixConn) getConn() (err error) {
	if zc.conn == nil {
		dialer := net.Dialer{}
		if zc.conn, err = dialer.Dial("tcp", zc.addr.String()); err != nil {
			return
		}
	}

	return
}

func (zc *ZabbixConn) cleanupConn() {
	if zc.conn != nil {
		zc.conn.Close()
		zc.conn = nil
	}
}

func (zc *ZabbixConn) ZabbixSend(data []byte) (err error) {
	err = zc.getConn()
	if zc.conn == nil || err != nil {
		return
	}

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

	zc.conn.SetWriteDeadline(time.Now().Add(zc.send_timeout * time.Second))

	var n int
	if n, err = zc.conn.Write(msgSlice); n != len(msgSlice) {
		err = fmt.Errorf("Full message not send, only %d of %b bytes", n, len(msgSlice))
	}

	return
}

func (zc *ZabbixConn) ZabbixReceive() (result []byte, err error) {
	err = zc.getConn()
	if zc.conn == nil || err != nil {
		return
	}
	defer zc.cleanupConn()

	// Get the response!
	zc.conn.SetReadDeadline(time.Now().Add(zc.receive_timeout))

	// Fetch the header first to get the full length
	header := make([]byte, 13)
	//var header_length int
	var n int
	if n, err = io.ReadFull(zc.conn, header); err != nil {
		return
	}

	// Check header content
	if string(header[:5]) != "ZBXD\x01" {
		err = fmt.Errorf("Unexpected response header from Zabbix: %s %d %s", string(header[:5]), len(header[:5]), header[:5])
		return
	}

	// Get length from zabbix protocol
	response_length := binary.LittleEndian.Uint64(header[5:13])

	zc.conn.SetReadDeadline(time.Now().Add(zc.receive_timeout))

	// Get full reponse
	response := make([]byte, response_length)
	if n, err = io.ReadFull(zc.conn, response); err != nil {
		return
	}

	if n != int(response_length) {
		err = fmt.Errorf("Unexpected response length from Zabbix header: %s", response_length)
		return
	}

	result = response

	return
}

func (zc *ZabbixConn) FetchActiveChecks(host string) (hac HostActiveChecks, err error) {
	msg := fmt.Sprintf("{\"request\":\"active checks\",\"host\":\"%s\"}", host)
	data := []byte(msg)

	hac.Keys = make(map[string]int, 1)

	if err = zc.ZabbixSend(data); err != nil {
		return
	} else {
		var result []byte
		if result, err = zc.ZabbixReceive(); err != nil {
			return
		} else {
			// Parse json for key names
			var unmarshalledResult ActiveCheckResponseJson
			//Check what's the result on no keys
			err = json.Unmarshal(result, &unmarshalledResult)
			if err != nil {
				return
			}

			// Push key names for the current host
			for _, activeCheckKey := range unmarshalledResult.Data {
				if fDelay, ok := activeCheckKey.Delay.(float64); ok {
					hac.Keys[activeCheckKey.Key] = int(fDelay)
				} else if sDelay, ok := activeCheckKey.Delay.(string); ok {
					// Put 15 as delay if strconv doesn't work for now
					if delay, conv_err := strconv.ParseInt(sDelay, 10, 32); conv_err != nil {
						hac.Keys[activeCheckKey.Key] = 15
					} else {
						hac.Keys[activeCheckKey.Key] = int(delay)
					}
				}
			}
		}
	}

	return
}
