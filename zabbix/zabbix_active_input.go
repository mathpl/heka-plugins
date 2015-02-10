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
	"fmt"
	"time"

	"code.google.com/p/go-uuid/uuid"

	"github.com/mathpl/active_zabbix"
	//. "github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

// ZabbixActiveInput _heavily_ based on TcpInput code.

// Input plugin implementation that listens for Heka protocol messages on a
// specified TCP socket. Creates a separate goroutine for each TCP connection.
type ZabbixActiveInput struct {
	zabbix_server active_zabbix.ZabbixActiveServer
	config        *ZabbixActiveInputConfig
	name          string
	stopChan      chan bool
	ir            InputRunner
	h             PluginHelper
}

type ZabbixActiveInputConfig struct {
	Address string `toml:"address"`
	// Name of configured decoder to receive the input
	Decoder string `toml:"decoder"`
	// Read deadline in ms
	ReceiveTimeout uint `toml:"receive_timeout"`
	// Write deadline in ms
	SendTimeout uint `toml:"send_timeout"`
}

func (z *ZabbixActiveInput) ConfigStruct() interface{} {
	config := &ZabbixActiveInputConfig{Address: "localhost:10051", Decoder: "ZabbixActiveDecoder",
		ReceiveTimeout: 5000, SendTimeout: 5000}
	return config
}

func (z *ZabbixActiveInput) Init(config interface{}) (err error) {
	z.config = config.(*ZabbixActiveInputConfig)
	z.zabbix_server, err = active_zabbix.NewZabbixActiveServer(z.config.Address, z.config.ReceiveTimeout, z.config.SendTimeout)
	return
}

func (z *ZabbixActiveInput) Run(ir InputRunner, h PluginHelper) error {
	z.ir = ir
	z.h = h
	z.name = ir.Name()
	z.stopChan = make(chan bool)

	var (
		dr DecoderRunner
		ok bool
	)
	if z.config.Decoder != "" {
		if dr, ok = z.h.DecoderRunner(z.config.Decoder,
			fmt.Sprintf("%s-%s-%s", z.name, z.config.Address, z.config.Decoder)); !ok {
			return fmt.Errorf("Error getting decoder: %s", z.config.Decoder)
		}
	}

	metric_chan := make(chan []byte, 1)
	go z.zabbix_server.Listen(metric_chan, z.stopChan)

	for {
		select {
		case <-z.stopChan:
			return nil
		case metric := <-metric_chan:
			pack := <-ir.InChan()
			pack.Message.SetUuid(uuid.NewRandom())
			pack.Message.SetTimestamp(time.Now().UnixNano())
			pack.Message.SetType("RawActiveZabbix")
			pack.Message.SetLogger(ir.Name())
			pack.MsgBytes = metric

			dr.InChan() <- pack
		}
	}

	return nil
}

func (z *ZabbixActiveInput) Stop() {
	close(z.stopChan)
}

func init() {
	RegisterPlugin("ZabbixActiveInput", func() interface{} {
		return new(ZabbixActiveInput)
	})
}
