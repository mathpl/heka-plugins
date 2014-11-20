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
	"sort"
	"strings"

	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

const ZABBIX_KEY_LENGTH_LIMIT = 255

type OpentsdbZabbixFilter struct {
	conf *OpentsdbZabbixFilterConfig
}

type OpentsdbZabbixFilterConfig struct {
	// Sort tag by name before adding them at the end of the opentsdb key
	// to create the Zabbix key
	SortTags bool
}

func (ozf *OpentsdbZabbixFilter) ConfigStruct() interface{} {
	return &OpentsdbZabbixFilterConfig{SortTags: true}
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
		var value string
		var key_extension []string
		for _, field := range fields {
			k := field.GetName()
			v := field.GetValue()
			switch k {
			case "host":
				host = v.(string)
			case "Value":
				switch vt := v.(type) {
				case string:
					value = vt
				case int:
				case int64:
					value = fmt.Sprintf("%d", vt)
				case float32:
				case float64:
					value = fmt.Sprintf("%f", vt)
				default:
					err = fmt.Errorf("Unexpected Value type %+V", v)
					break
				}
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
					key_part := fmt.Sprintf("%s.%s", k, vs)
					key_extension = append(key_extension, key_part)
				} else {
					err = fmt.Errorf("Unexpected Tag type %+V", v)
					break
				}
			}
		}

		if ozf.conf.SortTags {
			sort.Strings(key_extension)
		}

		if host == "" {
			//FIXME: Add default in plugin
			err = fmt.Errorf("Unable to find host tag in message.")
			pack2.Recycle()
			continue
		}

		if opentsdb_key == "" {
			err = fmt.Errorf("Unable to find Metric field in message.")
			pack2.Recycle()
			continue
		}

		zabbix_key := strings.Join(append([]string{opentsdb_key.(string)}, key_extension...), ".")
		if len(zabbix_key) > ZABBIX_KEY_LENGTH_LIMIT {
			err = fmt.Errorf("Zabbix Key length exceded: %s", zabbix_key)
			pack2.Recycle()
			continue
		}

		var field *message.Field
		if field, err = message.NewField("Key", zabbix_key, ""); err != nil {
			err = fmt.Errorf("Unable to add Zabbix Key: %s", err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		if field, err = message.NewField("Host", host, ""); err != nil {
			err = fmt.Errorf("Unable to add host: %s", err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		if field, err = message.NewField("Value", value, ""); err != nil {
			err = fmt.Errorf("Unable to add value: %s", err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		pack2.Message.SetType("zabbix")

		fr.Inject(pack2)
	}

	return
}

func init() {
	RegisterPlugin("OpentsdbZabbixFilter", func() interface{} {
		return new(OpentsdbZabbixFilter)
	})
}