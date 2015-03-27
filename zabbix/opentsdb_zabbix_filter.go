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
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

const ZABBIX_KEY_LENGTH_LIMIT = 255

// Tag type to help processing a bit
type Tag struct {
	Key   string
	Value string
}

type Tags []*Tag

func (ts Tags) Len() int      { return len(ts) }
func (ts Tags) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }
func (ts Tags) MakeKey(mode string) string {
	if mode == "append" {
		var b bytes.Buffer
		l := len(ts)
		for i, t := range ts {
			if t.Key != "" {
				b.WriteString(t.Key)
				b.WriteString(".")
			}
			b.WriteString(t.Value)

			if i < l-1 {
				b.WriteString(".")
			}
		}
		key := b.String()
		if len(key) > 0 {
			key = "." + key
		}
		return key
	} else if mode == "parameter" {
		var b bytes.Buffer
		for _, t := range ts {
			if t.Key != "" {
				b.WriteString("[")
				b.WriteString(t.Key)
				b.WriteString("=")
				b.WriteString(t.Value)
				b.WriteString("]")
			}
		}

		return b.String()
	} else {
		return ""
	}
}

type ByKey struct{ Tags }

func (s ByKey) Less(i, j int) bool { return s.Tags[i].Key < s.Tags[j].Key }

type OpentsdbZabbixFilter struct {
	conf      *OpentsdbZabbixFilterConfig
	stripTags map[string]bool

	metricName       string
	valueName        string
	tagPrefix        string
	msgType          string
	tagDelimiterMode string

	replace         map[string]string
	replaceKey      map[string]string
	replaceTagName  map[string]string
	replaceTagValue map[string]string
}

type OpentsdbZabbixFilterConfig struct {
	// Sort tag by name before adding them at the end of the opentsdb key
	// to create the Zabbix key. Off will conserve tag order as received.
	SortTags bool `toml:"sort_tags"`

	// Field in message used for the metric name
	MetricField string `toml:"metric_field"`

	// Field in message used for the value name
	ValueField string `toml:"value_field"`

	// Prefix used in field for tags
	TagPrefix string `toml:"tag_prefix"`

	// Method to append tags on the key name: append or parameter
	// append: key.tag.tagv.tag.tav
	// parameters: key[tag=tagv][tag=tagv]
	TagDelimiterMode string `toml:"tag_delimiter_mode"`

	// Message type for outbound messages
	MessageType string `toml:"msg_type"`

	// Max key length we'll accept to generate
	MaxKeyLength int `toml:"max_key_length"`

	// Activate additional key shortening strategy when over MaxKeyLength
	AdaptativeKeyShortening bool `toml:"adaptative_key_shortening"`

	// Set what strategy to use when AdaptativeKeyShortening is enabled. Only split supported currently.
	AdaptativeKeyShorteningStrategy string `toml:"adaptative_key_shortening_strategy"`

	// Parameters for the splitvalue strategy
	AdaptativeKeyShorteningStrategySplitValue map[string]SplitValueStrategy `toml:"adaptative_key_shortening_strategy_split_value"`

	// List of tags to omit from Zabbix key creation
	StripTags []string `toml:"strip_tags"`

	// Various string.Replace maps applied on specific parts of the
	// Opentsdb data.
	Replace         [][]string `toml:"replace"`
	ReplaceKey      [][]string `toml:"replace_key"`
	ReplaceTagName  [][]string `toml:"replace_tag_name"`
	ReplaceTagValue [][]string `toml:"replace_tag_value"`
}

type SplitValueStrategy struct {
	MustMatchKey string `toml:"must_match_key"`
	Delimiter    string `toml:"delimiter"`
	KeepFields   []int  `toml:"keep_fields"`
}

func (ozf *OpentsdbZabbixFilter) ConfigStruct() interface{} {
	return &OpentsdbZabbixFilterConfig{SortTags: true,
		MaxKeyLength: ZABBIX_KEY_LENGTH_LIMIT}
}

func mergeReplaceMaps(m [][]string) (rm map[string]string) {
	rm = make(map[string]string)
	for _, r := range m {
		rm[r[0]] = r[1]
	}
	return
}

func (ozf *OpentsdbZabbixFilter) Init(config interface{}) (err error) {
	ozf.conf = config.(*OpentsdbZabbixFilterConfig)
	ozf.stripTags = make(map[string]bool, len(ozf.conf.StripTags))
	for _, tag := range ozf.conf.StripTags {
		ozf.stripTags[tag] = true
	}
	if ozf.conf.MaxKeyLength > ZABBIX_KEY_LENGTH_LIMIT {
		err = fmt.Errorf("Max key length higher than what Zabbix server would accept (%d).", ZABBIX_KEY_LENGTH_LIMIT)
	}
	if ozf.conf.AdaptativeKeyShortening && ozf.conf.AdaptativeKeyShorteningStrategy == "" {
		err = fmt.Errorf("AdaptativeKeyShortening enabled but no strategy is defined.")
	}
	ozf.msgType = ozf.conf.MessageType
	if ozf.msgType == "" {
		ozf.msgType = "zabbix"
	}
	ozf.metricName = ozf.conf.MetricField
	if ozf.metricName == "" {
		ozf.metricName = "data.name"
	}
	ozf.valueName = ozf.conf.ValueField
	if ozf.valueName == "" {
		ozf.valueName = "data.value"
	}
	ozf.tagPrefix = ozf.conf.TagPrefix
	if ozf.tagPrefix == "" {
		ozf.tagPrefix = "data.tags."
	}
	ozf.tagDelimiterMode = ozf.conf.TagDelimiterMode
	if ozf.tagDelimiterMode == "" {
		ozf.tagDelimiterMode = "parameter"
	}
	if ozf.tagDelimiterMode != "append" && ozf.tagDelimiterMode != "parameter" {
		err = fmt.Errorf("Invalid tag delimiter mode, only 'append' or 'parameter' allowed.")
		return
	}

	// Assemble the Replace maps
	ozf.replace = mergeReplaceMaps(ozf.conf.Replace)
	ozf.replaceKey = mergeReplaceMaps(ozf.conf.ReplaceKey)
	ozf.replaceTagName = mergeReplaceMaps(ozf.conf.ReplaceTagName)
	ozf.replaceTagValue = mergeReplaceMaps(ozf.conf.ReplaceTagValue)

	return
}

func applyReplaceMap(val string, m map[string]string) (s string) {
	s = val
	for src, to := range m {
		s = strings.Replace(s, src, to, -1)
	}
	return
}

func applyShorteningSplit(svs map[string]SplitValueStrategy, keyExtension Tags) Tags {
	for _, ss := range svs {
		for _, tag := range keyExtension {
			// We have a match or we don't need one
			if (ss.MustMatchKey != "" && strings.Contains(ss.MustMatchKey, tag.Key)) || ss.MustMatchKey == "" {
				split_val := strings.Split(tag.Value, ss.Delimiter)
				var v []string
				for _, i := range ss.KeepFields {
					v = append(v, split_val[i])
				}
				tag.Value = strings.Join(v, ss.Delimiter)
			}
		}
	}

	return keyExtension
}

func (ozf *OpentsdbZabbixFilter) applyShortening(keyExtension Tags) (shorter_keyExtension Tags, err error) {
	switch ozf.conf.AdaptativeKeyShorteningStrategy {
	case "split_value":
		shorter_keyExtension = applyShorteningSplit(ozf.conf.AdaptativeKeyShorteningStrategySplitValue, keyExtension)
	default:
		err = fmt.Errorf("No AdaptativeKeyShorteningStrategy matched.")
	}

	return
}

func (ozf *OpentsdbZabbixFilter) Run(fr FilterRunner, h PluginHelper) (err error) {
	var (
		pack *PipelinePack
	)

	for pack = range fr.InChan() {
		pack2 := h.PipelinePack(pack.MsgLoopCount)
		if pack2 == nil {
			fr.LogError(fmt.Errorf("exceeded MaxMsgLoops = %d", h.PipelineConfig().Globals.MaxMsgLoops))
			pack.Recycle()
			break
		}

		opentsdb_key, ok := pack.Message.GetFieldValue(ozf.metricName)
		if !ok {
			err = fmt.Errorf("Unable to find Field[\"Metric\"] field in message, make sure it's been decoded by OpenstdbRawDecoder.")
			fr.LogError(err)
			pack.Recycle()
			pack2.Recycle()
			continue
		}

		fields := pack.Message.GetFields()
		var host string
		var value string
		var keyExtension Tags
		tagPrefixLen := len(ozf.tagPrefix)
		//FIXME: Configurable field names. (data. prefix )
		for _, field := range fields {
			k := field.GetName()

			k = applyReplaceMap(k, ozf.replace)
			k = applyReplaceMap(k, ozf.replaceTagName)

			v := field.GetValue()
			switch k {
			case "host":
				host = v.(string)
			case ozf.valueName:
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
			case ozf.metricName:
				if vs, ok := v.(string); ok {
					k = applyReplaceMap(k, ozf.replace)
					k = applyReplaceMap(k, ozf.replaceKey)
					opentsdb_key = vs
				} else {
					err = fmt.Errorf("Unexpected Metric type %+V", v)
					break
				}
			default:
				if vs, ok := v.(string); ok {
					if strings.HasPrefix(k, ozf.tagPrefix) {
						k_tag := k[tagPrefixLen:]
						if _, found := ozf.stripTags[k_tag]; found {
							//Stripped tag, do not process
							continue
						}

						vs = applyReplaceMap(vs, ozf.replace)
						vs = applyReplaceMap(vs, ozf.replaceTagValue)

						t := &Tag{k_tag, vs}

						//FIXME: Less append, more correct sizing from start
						keyExtension = append(keyExtension, t)
					}
				}
			}
		}
		pack.Recycle()

		if err != nil {
			fr.LogError(err)
			pack2.Recycle()
			continue
		}

		if ozf.conf.SortTags {
			sort.Sort(ByKey{keyExtension})
		}

		if host == "" {
			//FIXME: Add default in plugin
			err = fmt.Errorf("Unable to find host tag in message.")
			fr.LogError(err)
			pack2.Recycle()
			continue
		}

		if opentsdb_key == "" {
			err = fmt.Errorf("Unable to find Metric field in message.")
			fr.LogError(err)
			pack2.Recycle()
			continue
		}

		zabbix_key := opentsdb_key.(string)
		suffix := keyExtension.MakeKey(ozf.tagDelimiterMode)
		if suffix != "" {
			zabbix_key = fmt.Sprintf("%s%s", zabbix_key, suffix)
		}

		if len(zabbix_key) > ZABBIX_KEY_LENGTH_LIMIT {
			if ozf.conf.AdaptativeKeyShortening {
				if keyExtension, err = ozf.applyShortening(keyExtension); err != nil {
					err = fmt.Errorf("Unable to apply AdaptativeKeyShorteningStrategy: %s", err)
					fr.LogError(err)
					pack2.Recycle()
					continue
				} else {
					zabbix_key = fmt.Sprintf("%s%s", opentsdb_key.(string), keyExtension.MakeKey(ozf.tagDelimiterMode))
				}
			}

			if !ozf.conf.AdaptativeKeyShortening || len(zabbix_key) > ZABBIX_KEY_LENGTH_LIMIT {
				err = fmt.Errorf("Zabbix Key length exceded: %s", zabbix_key)
				fr.LogError(err)
				pack2.Recycle()
				continue
			}
		}

		var field *message.Field
		if field, err = message.NewField("key", zabbix_key, ""); err != nil {
			err = fmt.Errorf("Unable to add Zabbix Key: %s", err)
			fr.LogError(err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		if field, err = message.NewField("host", host, ""); err != nil {
			err = fmt.Errorf("Unable to add host: %s", err)
			fr.LogError(err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		if field, err = message.NewField("value", value, ""); err != nil {
			err = fmt.Errorf("Unable to add value: %s", err)
			fr.LogError(err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		pack2.Message.SetType(ozf.msgType)
		pack2.Message.SetTimestamp(pack.Message.GetTimestamp())
		fr.Inject(pack2)
	}

	return
}

func init() {
	RegisterPlugin("OpentsdbZabbixFilter", func() interface{} {
		return new(OpentsdbZabbixFilter)
	})
}
