package plugins

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/mathpl/active_zabbix"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

// Decoder that expects ZabbixActive string format data in the message payload,
type ZabbixActiveDecoder struct {
	runner DecoderRunner
	helper PluginHelper
	config *ZabbixActiveDecoderConfig
}

type ZabbixActiveDecoderConfig struct{}

func (d *ZabbixActiveDecoder) ConfigStruct() interface{} {
	return &ZabbixActiveDecoderConfig{}
}

func (d *ZabbixActiveDecoder) Init(config interface{}) error {
	d.config = config.(*ZabbixActiveDecoderConfig)
	return nil
}

// Implement `WantsDecoderRunner`
func (d *ZabbixActiveDecoder) SetDecoderRunner(dr DecoderRunner) {
	d.runner = dr
}

func (d *ZabbixActiveDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {
	var metric_req active_zabbix.ZabbixMetricRequestJson
	err = json.Unmarshal(pack.MsgBytes, &metric_req)
	if err != nil {
		return
	}

	packs = []*PipelinePack{}
	for _, metric := range metric_req.Data {
		if pack == nil {
			pack = d.runner.NewPack()
		}
		// Check timestamp validity.
		unixTime, err := strconv.Atoi(metric.Clock)
		if err != nil {
			err = fmt.Errorf("invalid timestamp: '%s'", metric.Clock)
			continue
		}
		pack.Message.SetTimestamp(time.Unix(int64(unixTime), 0).UnixNano())

		if err = d.addStatField(pack, "Key", metric.Key); err != nil {
			continue
		}
		if err = d.addStatField(pack, "Host", metric.Host); err != nil {
			continue
		}
		if err = d.addStatField(pack, "Value", metric.Value); err != nil {
			continue
		}

		pack.Message.SetType("zabbix")
		packs = append(packs, pack)
		pack = nil
	}

	return
}

func (d *ZabbixActiveDecoder) addStatField(pack *PipelinePack, name string,
	value interface{}) error {

	field, err := message.NewField(name, value, "")
	if err != nil {
		return fmt.Errorf("error adding field '%s': %s", name, err)
	}
	pack.Message.AddField(field)
	return nil
}

func init() {
	RegisterPlugin("ZabbixActiveDecoder", func() interface{} {
		return new(ZabbixActiveDecoder)
	})
}
