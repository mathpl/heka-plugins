package plugins

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

type ActiveCheckKeyJson struct {
	Key         string `json:"key"`
	Delay       string `json:"delay"`
	Lastlogsize string `json:"lastlogsize"`
	Mtime       string `json:"mtime"`
}

type ActiveCheckResponseJson struct {
	Response string               `json:"response"`
	Data     []ActiveCheckKeyJson `json:"Data"`
}

func (zo *ZabbixOutput) ZabbixSend(data []byte) (err error) {
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

func (zo *ZabbixOutput) ZabbixReceive() (result []byte, err error) {
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
