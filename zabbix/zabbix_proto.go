package plugins

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
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

type ZabbixConn struct {
	conn            net.Conn
	addr            *net.TCPAddr
	receive_timeout time.Duration
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
	defer zc.cleanupConn()

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
	if n, err = zc.conn.Write(msgSlice); err != nil {
		zc.cleanupConn()
		err = fmt.Errorf("writing to %s: %s", zc.addr.String(), err)
	} else if n != len(msgSlice) {
		zc.cleanupConn()
		err = fmt.Errorf("truncated output to: %s", zc.addr.String())
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
	if _, err = io.ReadFull(zc.conn, header); err != nil {
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
