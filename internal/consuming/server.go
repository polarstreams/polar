package consuming

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/jorgebay/soda/internal/utils"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	addConsumerDelay    = 5 * time.Second
	removeConsumerDelay = 30 * time.Second
)

var rebalanceDebouncer = Debounce()

// Consumer represents a consumer server
type Consumer interface {
	Initializer

	AcceptConnections() error
}

func NewConsumer(config conf.ConsumerConfig, topologyGetter discovery.TopologyGetter) Consumer {
	return &consumer{
		config:         config,
		topologyGetter: topologyGetter,
		meta:           NewConsumersMeta(topologyGetter),
	}
}

type consumer struct {
	config         conf.ConsumerConfig
	topologyGetter discovery.TopologyGetter
	meta           *ConsumersMeta
}

func (c *consumer) Init() error {
	return nil
}

func (c *consumer) AcceptConnections() error {
	server := &http2.Server{}
	port := c.config.ConsumerPort()
	address := GetServiceAddress(port, c.topologyGetter.LocalInfo(), c.config)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	startChan := make(chan bool, 1)
	go func() {
		startChan <- true
		for {
			// HTTP/2 only server (prior knowledge)
			conn, err := listener.Accept()
			if err != nil {
				log.Err(err).Msgf("Failed to accept new connections")
				break
			}

			log.Debug().Msgf("Accepted new consumer http connection on %v", conn.LocalAddr())

			router := httprouter.New()
			router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				fmt.Fprintf(w, "Consumer server listening on %d\n", port)
			})

			trackedConn := NewTrackedConnection(conn, func(trackedConn *TrackedConnection) {
				c.unRegister(trackedConn)
			})

			router.POST(conf.ConsumerRegisterUrl, toPostHandler(trackedConn, c.postRegister))
			router.POST(conf.ConsumerPollUrl, toPostHandler(trackedConn, c.postPoll))

			// server.ServeConn() will block until the connection is not readable anymore
			// start it in the background to accept further connections
			go func() {
				server.ServeConn(conn, &http2.ServeConnOpts{
					Handler: h2c.NewHandler(router, server),
				})
			}()
		}
	}()

	<-startChan

	log.Info().Msgf("Start listening to consumers for http requests on port %d", port)
	return nil
}

func (c *consumer) postRegister(
	conn *TrackedConnection,
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) error {
	// TODO: Read the consumer group, consumer id and subscribed topics

	// consumer := ConsumerInfo{}
	// consumer.id
	//connMap[conn.id] = consumer
	//consumerMap[consumer.id] = consumer
	//cache -> consumer groups

	rebalanceDebouncer(addConsumerDelay, func() {
		if c.meta.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after adding a new consumer")
		}
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("OK"))
	return nil
}

func (c *consumer) unRegister(conn *TrackedConnection) {
	// Remove connection
	// Remove consumer

	// Wait a long time to rebalance, as it's maybe rebooting/
	rebalanceDebouncer(removeConsumerDelay, func() {
		if c.meta.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after adding a new consumer")
		}
	})
}

func (c *consumer) postPoll(
	conn *TrackedConnection,
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) error {
	// get consumer by connection
	// consumer = connMap[conn.id]
	// shouldConsumerGetDataForTokens()

	return nil
}

type ConnAwareHandle func(*TrackedConnection, http.ResponseWriter, *http.Request, httprouter.Params) error

func toPostHandler(c *TrackedConnection, h ConnAwareHandle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := h(c, w, r, ps); err != nil {
			adaptHttpErr(err, w)
		}
	}
}

func adaptHttpErr(err error, w http.ResponseWriter) {
	httpErr, ok := err.(HttpError)

	if !ok {
		log.Err(err).Msg("Unexpected error when consuming")
		http.Error(w, "Internal server error", 500)
		return
	}

	w.WriteHeader(httpErr.StatusCode())
	// The message is supposed to be user friendly
	fmt.Fprintf(w, err.Error())
}
