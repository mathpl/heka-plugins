package healtcheck_output

import (
	"net/http"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

type HealthcheckOutput struct {
	conf      *HealthcheckOutputConfig
	threshold time.Duration
}

type HealthcheckOutputConfig struct {
	// Address to bind
	BindAddress string `toml:"bind_address"`

	// How many seconds of inactivity before failure
	IdleThreshold int `toml:"idle_threshold"`
}

func (ohc *HealthcheckOutput) ConfigStruct() interface{} {
	return &HealthcheckOutputConfig{
		BindAddress:   "localhost:4354",
		IdleThreshold: 300,
	}
}

func (ho *HealthcheckOutput) Init(config interface{}) (err error) {
	ho.conf = config.(*HealthcheckOutputConfig)

	ho.threshold = time.Duration(ho.conf.IdleThreshold) * time.Second

	return
}

func (ho *HealthcheckOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		pack        *PipelinePack
		inChan      = or.InChan()
		lastMsgTime = time.Now()
	)

	handler := func(w http.ResponseWriter, r *http.Request) {
		if lastMsgTime.Add(ho.threshold).After(time.Now()) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		return
	}

	http.HandleFunc("/", handler)
	http.HandleFunc("/healthcheck/", handler)
	go http.ListenAndServe(ho.conf.BindAddress, nil)

	for pack = range inChan {
		lastMsgTime = time.Now()
		pack.Recycle()
	}

	return
}

func init() {
	RegisterPlugin("HealthcheckOutput", func() interface{} {
		return new(HealthcheckOutput)
	})
}
