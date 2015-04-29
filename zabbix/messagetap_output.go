package plugins

import (
	"fmt"
	"net"
	"time"

	. "github.com/mozilla-services/heka/pipeline"
)

type MessageTapOutput struct {
	conf     *MessageTapOutputConfig
	listener *net.TCPListener
	clients  []net.Conn
}

type MessageTapOutputConfig struct {
	// Address to bind
	BindAddress string `toml:"bind_address"`

	// Encoder to use
	Encoder string `toml:"encoder"`
}

func (mtc *MessageTapOutput) ConfigStruct() interface{} {
	return &MessageTapOutputConfig{
		Encoder:     "RstEncoder",
		BindAddress: "localhost:4353",
	}
}

func (mt *MessageTapOutput) Init(config interface{}) (err error) {
	mt.conf = config.(*MessageTapOutputConfig)

	addr, err := net.ResolveTCPAddr("tcp", mt.conf.BindAddress)
	if err != nil {
		return
	}

	mt.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return
	}

	mt.clients = make([]net.Conn, 0)

	return
}

func (mt *MessageTapOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok     = true
		pack   *PipelinePack
		inChan = or.InChan()
	)

	new_clients_chan := make(chan net.Conn)
	stop_chan := make(chan *bool)
	go func() {
		for {
			mt.listener.SetDeadline(time.Now().Add(time.Second))
			conn, err := mt.listener.Accept()
			select {
			case <-stop_chan:
				break
			default:
			}

			if err != nil {
				netErr, ok := err.(net.Error)
				if ok && netErr.Timeout() && netErr.Temporary() {
					continue
				}
				or.LogError(fmt.Errorf("Error accepting connection: %s", err))
			}
			mt.clients = append(mt.clients, conn)
		}
	}()

	for ok {
		select {
		case new_conn := <-new_clients_chan:
			mt.clients = append(mt.clients, new_conn)

		case pack, ok = <-inChan:
			if !ok {
				break
			}

			if msg, localErr := or.Encode(pack); localErr != nil {
				or.LogError(fmt.Errorf("Encoder failure: %s", localErr))
			} else {
				if len(mt.clients) > 0 {
					client_status := make(map[int]bool, 0)
					need_cleanup := false
					for i, c := range mt.clients {
						c.SetWriteDeadline(time.Now().Add(time.Second))
						if _, err := c.Write(msg); err != nil {
							need_cleanup = true
							client_status[i] = false
						} else {
							client_status[i] = true
						}
					}

					if need_cleanup {
						new_client_list := make([]net.Conn, 0)
						for i, c := range mt.clients {
							if client_status[i] {
								new_client_list = append(new_client_list, c)
							}
						}
						mt.clients = new_client_list
					}
				}
			}
			pack.Recycle()
		}
	}

	//Stop the tcp server
	close(stop_chan)

	return
}

func init() {
	RegisterPlugin("MessageTapOutput", func() interface{} {
		return new(MessageTapOutput)
	})
}
