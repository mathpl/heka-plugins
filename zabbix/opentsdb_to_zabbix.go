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

	"github.com/mozilla-services/heka/pipeline"
)

const ZABBIX_KEY_LENGTH_LIMIT = 255

// OpenTsdbToZabbixEncoder generates a Zabbix compatible output from
// an opentsdb decoded message.
type OpenTsdbToZabbixEncoder struct {
	config *OpenTsdbToZabbixEncoderConfig
}

type OpenTsdbToZabbixEncoderConfig struct {
}

func (oze *OpenTsdbToZabbixEncoder) ConfigStruct() interface{} {
	return &OpenTsdbToZabbixEncoderConfig{}
}

func (oze *OpenTsdbToZabbixEncoder) Init(config interface{}) (err error) {
	oze.config = config.(*OpenTsdbToZabbixEncoderConfig)

	return
}

func (oe *OpenTsdbToZabbixEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	opentsdb_key, ok := pack.Message.GetFieldValue("Metric")
	if !ok {
		err = fmt.Errorf("Unable to find Field[\"Metric\"] field in message, make sure it's been decoded by OpenstdbRawDecoder.")
		return nil, err
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
				return nil, err
			}
		case "Metric":
			if vs, ok := v.(string); ok {
				opentsdb_key = vs
			} else {
				err = fmt.Errorf("Unexpected Metric type %+V", v)
				return nil, err
			}
		default:
			if vs, ok := v.(string); ok {
				//FIXME: Less append, more correct sizing from start
				key_extension = append(key_extension, k, vs)
			} else {
				err = fmt.Errorf("Unexpected Tag type %+V", v)
				return nil, err
			}
		}
	}

	if host == "" {
		//FIXME: Add default in plugin
		err = fmt.Errorf("Unable to find host tag in message.")
		return nil, err
	}

	if opentsdb_key == "" {
		//FIXME: Add default in plugin
		err = fmt.Errorf("Unable to find Metric field in message.")
		return nil, err
	}

	if value == "" {
		//FIXME: Add default in plugin
		err = fmt.Errorf("Unable to find Value field in message.")
		return nil, err
	}

	zabbix_metrics := fmt.Sprintf("%s %s.%s %s\n", host, opentsdb_key, strings.Join(key_extension, "."), value)

	output = []byte(zabbix_metrics)
	return
}

func init() {
	pipeline.RegisterPlugin("OpenTsdbToZabbixEncoder", func() interface{} {
		return new(OpenTsdbToZabbixEncoder)
	})
}
