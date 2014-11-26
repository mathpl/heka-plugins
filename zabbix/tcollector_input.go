/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#   Carlos Diaz-Padron (cpadron@mozilla.com,carlos@carlosdp.io)
#   Mathieu Payeur Levallois (math.pay@gmail.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"

	. "github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

// TcollectorInput _heavily_ based on TcpInput code.

// Input plugin implementation that listens for Heka protocol messages on a
// specified TCP socket. Creates a separate goroutine for each TCP connection.
type TcollectorInput struct {
	keepAliveDuration time.Duration
	listener          net.Listener
	name              string
	wg                sync.WaitGroup
	stopChan          chan bool
	ir                InputRunner
	h                 PluginHelper
	config            *TcollectorInputConfig
}

type TcollectorInputConfig struct {
	// Network type (e.g. "tcp", "tcp4", "tcp6", "unix" or "unixpacket"). Needs to match the input type.
	Net string
	// String representation of the address of the network connection on which
	// the listener should be listening (e.g. "127.0.0.1:5565").
	Address string
	// Set of message signer objects, keyed by signer id string.
	Signers map[string]Signer `toml:"signer"`
	// Name of configured decoder to receive the input
	Decoder string
	// Delimiter used to split the stream into messages
	Delimiter string
	// String indicating if the delimiter is at the start or end of the line,
	// only used for regexp delimiters
	DelimiterLocation string `toml:"delimiter_location"`
	// Set to true if TCP Keep Alive should be used.
	KeepAlive bool `toml:"keep_alive"`
	// Integer indicating seconds between keep alives.
	KeepAlivePeriod int `toml:"keep_alive_period"`
}

func (t *TcollectorInput) ConfigStruct() interface{} {
	config := &TcollectorInputConfig{Net: "tcp",
		Delimiter: "\n"}
	return config
}

func (t *TcollectorInput) Init(config interface{}) error {
	var err error
	t.config = config.(*TcollectorInputConfig)
	address, err := net.ResolveTCPAddr(t.config.Net, t.config.Address)
	if err != nil {
		return fmt.Errorf("ListenTCP failed: %s\n", err.Error())
	}
	t.listener, err = net.ListenTCP(t.config.Net, address)
	if err != nil {
		return fmt.Errorf("ListenTCP failed: %s\n", err.Error())
	}
	// We're already listening, make sure we clean up if init fails later on.
	closeIt := true
	defer func() {
		if closeIt {
			t.listener.Close()
		}
	}()
	if len(t.config.Delimiter) > 1 {
		return fmt.Errorf("invalid delimiter: %s", t.config.Delimiter)
	}
	if t.config.KeepAlivePeriod != 0 {
		t.keepAliveDuration = time.Duration(t.config.KeepAlivePeriod) * time.Second
	}
	closeIt = false
	return nil
}

func NetworkPayloadParserAndAnswer(conn net.Conn,
	parser StreamParser,
	ir InputRunner,
	signers map[string]Signer,
	dr DecoderRunner) (err error) {
	var (
		pack   *PipelinePack
		record []byte
	)

	for true {
		_, record, err = parser.Parse(conn)
		if err != nil {
			if err == io.ErrShortBuffer {
				ir.LogError(fmt.Errorf("record exceeded MAX_RECORD_SIZE %d", MAX_RECORD_SIZE))
				err = nil // non-fatal
			}
		}
		if len(record) == 0 {
			break
		} else if len(record) >= 7 && string(record[:7]) == "version" {
			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			if _, err = conn.Write([]byte("Roger, Heka here.\n")); err != nil {
				ir.LogError(fmt.Errorf("Unable to answer 'version' on socket: %s", err.Error()))
				err = nil // non-fatal
			}
			break
		}
		pack = <-ir.InChan()
		pack.Message.SetUuid(uuid.NewRandom())
		pack.Message.SetTimestamp(time.Now().UnixNano())
		pack.Message.SetType("NetworkInput")
		// Only TCP packets have a remote address.
		if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
			pack.Message.SetHostname(remoteAddr.String())
		}
		pack.Message.SetLogger(ir.Name())
		pack.Message.SetPayload(string(record))
		if dr == nil {
			ir.Inject(pack)
		} else {
			dr.InChan() <- pack
		}
	}
	return
}

// Listen on the provided TCP connection, extracting messages from the incoming
// data until the connection is closed or Stop is called on the input.
func (t *TcollectorInput) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		t.wg.Done()
	}()

	var (
		dr DecoderRunner
		ok bool
	)
	if t.config.Decoder != "" {
		raddr := conn.RemoteAddr().String()
		host, _, err := net.SplitHostPort(raddr)
		if err != nil {
			host = raddr
		}
		if dr, ok = t.h.DecoderRunner(t.config.Decoder,
			fmt.Sprintf("%s-%s-%s", t.name, host, t.config.Decoder)); !ok {
			t.ir.LogError(fmt.Errorf("Error getting decoder: %s", t.config.Decoder))
			return
		}
	}

	var (
		parser        StreamParser
		parseFunction NetworkParseFunction
	)

	tp := NewTokenParser()
	parser = tp
	parseFunction = NetworkPayloadParserAndAnswer
	if len(t.config.Delimiter) == 1 {
		tp.SetDelimiter(t.config.Delimiter[0])
	}

	var err error
	stopped := false
	for !stopped {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		select {
		case <-t.stopChan:
			stopped = true
		default:
			err = parseFunction(conn, parser, t.ir, t.config.Signers, dr)
			if err != nil {
				if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
					// keep the connection open, we are just checking to see if
					// we are shutting down: Issue #354
				} else {
					stopped = true
				}
			}
		}
	}
	// Stop the decoder, see Issue #713.
	if dr != nil {
		t.h.StopDecoderRunner(dr)
	}
}

func (t *TcollectorInput) Run(ir InputRunner, h PluginHelper) error {
	t.ir = ir
	t.h = h
	t.stopChan = make(chan bool)
	t.name = ir.Name()

	var conn net.Conn
	var e error
	for {
		if conn, e = t.listener.Accept(); e != nil {
			if e.(net.Error).Temporary() {
				t.ir.LogError(fmt.Errorf("TCP accept failed: %s", e))
				continue
			} else {
				break
			}
		}
		if t.config.KeepAlive {
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				return errors.New("KeepAlive only supported for TCP Connections.")
			}
			tcpConn.SetKeepAlive(t.config.KeepAlive)
			if t.keepAliveDuration != 0 {
				tcpConn.SetKeepAlivePeriod(t.keepAliveDuration)
			}
		}
		t.wg.Add(1)
		go t.handleConnection(conn)
	}
	t.wg.Wait()
	return nil
}

func (t *TcollectorInput) Stop() {
	if err := t.listener.Close(); err != nil {
		t.ir.LogError(fmt.Errorf("Error closing listener: %s", err))
	}
	close(t.stopChan)
}

func init() {
	RegisterPlugin("TcollectorInput", func() interface{} {
		return new(TcollectorInput)
	})
}
