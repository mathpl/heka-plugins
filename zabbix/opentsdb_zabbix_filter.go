/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mathieu Payeur Levallois (math.pay@gmail.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"fmt"
	"strings"

	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

// Heka Filter plugin that can accept specific message types, extract data
// from those messages, and from that data generate statsd messages in a
// StatsdInput exactly as if a statsd message has come from a networked statsd
// client.
type OpentsdbZabbixFilter struct {
	conf *OpentsdbZabbixFilterConfig
}

// StatFilter config struct.
type OpentsdbZabbixFilterConfig struct {
}

func (ozf *OpentsdbZabbixFilter) ConfigStruct() interface{} {
	return &OpentsdbZabbixFilterConfig{}
}

func (ozf *OpentsdbZabbixFilter) Init(config interface{}) (err error) {
	ozf.conf = config.(*OpentsdbZabbixFilterConfig)
	return
}

func (ozf *OpentsdbZabbixFilter) Run(fr FilterRunner, h PluginHelper) (err error) {
	var (
		pack *PipelinePack
	)

	for pack = range fr.InChan() {
		pack2 := h.PipelinePack(pack.MsgLoopCount)
		if pack2 == nil {
			fr.LogError(fmt.Errorf("exceeded MaxMsgLoops = %d",
				h.PipelineConfig().Globals.MaxMsgLoops))
			break
		}

		opentsdb_key, ok := pack.Message.GetFieldValue("Metric")
		if !ok {
			err = fmt.Errorf("Unable to find Field[\"Metric\"] field in message, make sure it's been decoded by OpenstdbRawDecoder.")
			continue
		}

		fields := pack.Message.GetFields()
		var host string
		var key_extension []string
		for _, field := range fields {
			k := field.GetName()
			v := field.GetValue()
			switch k {
			case "host":
				host = v.(string)
			case "Value":
				break
			case "Metric":
				if vs, ok := v.(string); ok {
					opentsdb_key = vs
				} else {
					err = fmt.Errorf("Unexpected Metric type %+V", v)
					break
				}
			default:
				if vs, ok := v.(string); ok {
					//FIXME: Less append, more correct sizing from start
					key_extension = append(key_extension, k, vs)
				} else {
					err = fmt.Errorf("Unexpected Tag type %+V", v)
					break
				}
			}
		}

		if host == "" {
			//FIXME: Add default in plugin
			err = fmt.Errorf("Unable to find host tag in message.")
			pack.Recycle()
			continue
		}

		if opentsdb_key == "" {
			err = fmt.Errorf("Unable to find Metric field in message.")
			pack.Recycle()
			continue
		}

		// Patch in zabbix data so we don't process it 2 times
		zabbix_key := fmt.Sprintf("%s.%s", opentsdb_key, strings.Join(key_extension, "."))
		var field *message.Field
		if field, err = message.NewField("ZabbixKey", zabbix_key, ""); err != nil {
			err = fmt.Errorf("Unable to add Zabbix Key: %s", err)
			pack.Recycle()
			continue
		}
		pack.Message.AddField(field)

		if field, err = message.NewField("Host", host, ""); err != nil {
			err = fmt.Errorf("Unable to add host: %s", err)
			pack.Recycle()
			continue
		}
		pack.Message.AddField(field)
	}

	return
}

func init() {
	RegisterPlugin("OpentsdbZabbixFilter", func() interface{} {
		return new(OpentsdbZabbixFilter)
	})
}
