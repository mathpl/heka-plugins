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
func (ts Tags) MakeKey() string {
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

	return b.String()
}

type ByKey struct{ Tags }

func (s ByKey) Less(i, j int) bool { return s.Tags[i].Key < s.Tags[j].Key }

type OpentsdbZabbixFilter struct {
	conf      *OpentsdbZabbixFilterConfig
	stripTags map[string]bool

	replace         map[string]string
	replaceKey      map[string]string
	replaceTagName  map[string]string
	replaceTagValue map[string]string
}

type OpentsdbZabbixFilterConfig struct {
	// Sort tag by name before adding them at the end of the opentsdb key
	// to create the Zabbix key. Off will conserve tag order as received.
	SortTags bool `toml:"sort_tags"`

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

func applyShorteningSplit(svs map[string]SplitValueStrategy, key_extension Tags) Tags {
	for _, ss := range svs {
		for _, tag := range key_extension {
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

	return key_extension
}

func (ozf *OpentsdbZabbixFilter) applyShortening(key_extension Tags) (shorter_key_extension Tags, err error) {
	switch ozf.conf.AdaptativeKeyShorteningStrategy {
	case "split_value":
		shorter_key_extension = applyShorteningSplit(ozf.conf.AdaptativeKeyShorteningStrategySplitValue, key_extension)
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

		opentsdb_key, ok := pack.Message.GetFieldValue("Metric")
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
		var key_extension Tags
		for _, field := range fields {
			k := field.GetName()
			if _, found := ozf.stripTags[k]; found {
				//Stripped tag, do not process
				continue
			}

			k = applyReplaceMap(k, ozf.replace)
			k = applyReplaceMap(k, ozf.replaceTagName)

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
					k = applyReplaceMap(k, ozf.replace)
					k = applyReplaceMap(k, ozf.replaceKey)
					opentsdb_key = vs
				} else {
					err = fmt.Errorf("Unexpected Metric type %+V", v)
					break
				}
			default:
				if vs, ok := v.(string); ok {
					vs = applyReplaceMap(vs, ozf.replace)
					vs = applyReplaceMap(vs, ozf.replaceTagValue)

					t := &Tag{k, vs}

					//FIXME: Less append, more correct sizing from start
					key_extension = append(key_extension, t)
				} else {
					err = fmt.Errorf("Unexpected Tag type %+V", v)
					break
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
			sort.Sort(ByKey{key_extension})
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

		zabbix_key := []string{opentsdb_key.(string)}
		suffix := key_extension.MakeKey()
		if suffix != "" {
			zabbix_key = fmt.Sprintf("%s.%s", zabbix_key, suffix)
		}

		if len(zabbix_key) > ZABBIX_KEY_LENGTH_LIMIT {
			if ozf.conf.AdaptativeKeyShortening {
				if key_extension, err = ozf.applyShortening(key_extension); err != nil {
					err = fmt.Errorf("Unable to apply AdaptativeKeyShorteningStrategy: %s", err)
					fr.LogError(err)
					pack2.Recycle()
					continue
				} else {
					zabbix_key = fmt.Sprintf("%s.%s", opentsdb_key.(string), key_extension.MakeKey())
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
		if field, err = message.NewField("Key", zabbix_key, ""); err != nil {
			err = fmt.Errorf("Unable to add Zabbix Key: %s", err)
			fr.LogError(err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		if field, err = message.NewField("Host", host, ""); err != nil {
			err = fmt.Errorf("Unable to add host: %s", err)
			fr.LogError(err)
			pack2.Recycle()
			continue
		}
		pack2.Message.AddField(field)

		if field, err = message.NewField("Value", value, ""); err != nil {
			err = fmt.Errorf("Unable to add value: %s", err)
			fr.LogError(err)
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
