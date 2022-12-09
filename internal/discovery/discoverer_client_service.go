package discovery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const noGenerationsStatusMessage = "Broker is unavailable to handle producer/consumer requests"

type topologyClientMessage struct {
	BaseName     string   `json:"baseName,omitempty"`    // When defined, base name to build the broker names, e.g. "polar-"
	ServiceName  string   `json:"serviceName,omitempty"` // The name of the service to build the broker names: "<baseName><ordinal>.<service>"
	Length       int      `json:"length"`                // The ring size
	BrokerNames  []string `json:"names,omitempty"`
	ProducerPort int      `json:"producerPort"`
	ConsumerPort int      `json:"consumerPort"`
}

func (d *discoverer) startClientDiscoveryServer() error {
	port := d.config.ClientDiscoveryPort()
	address := utils.GetServiceAddress(port, d.LocalInfo(), d.config)
	router := httprouter.New()
	router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		generations := d.generations.Load().(genMap)
		if len(generations) == 0 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, noGenerationsStatusMessage)
			return
		}

		fmt.Fprintf(w, "Client discovery server listening on %d\n", port)
	})

	router.GET(conf.ClientDiscoveryUrl, utils.ToHandle(d.getTopologyHandler))

	h2s := &http2.Server{}
	server := &http.Server{
		Addr:    address,
		Handler: h2c.NewHandler(router, h2s),
	}

	if err := http2.ConfigureServer(server, h2s); err != nil {
		return err
	}

	c := make(chan bool, 1)
	go func() {
		c <- true
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				log.Info().Msgf("Client discovery server stopped")
			} else {
				log.Err(err).Msgf("Client discovery stopped serving")
			}
		}
	}()

	d.clientDiscoveryServer = server

	<-c
	log.Info().Msgf("Start listening to clients for discovery on %s", address)

	return nil
}

func (d *discoverer) getTopologyHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	w.Header().Set("Content-Type", "application/json")
	t := d.Topology()

	var result *topologyClientMessage
	if names := os.Getenv(envBrokerNames); len(t.Brokers) <= 3 || names != "" {
		result = d.newResponseTopology(t)
	} else {
		result = d.newResponseTopologyUsingOrdinals(t)
	}

	utils.PanicIfErr(json.NewEncoder(w).Encode(result), "Unexpected error when serializing generation")
	return nil
}

func (d *discoverer) newResponseTopology(t *TopologyInfo) *topologyClientMessage {
	brokerNames := make([]string, len(t.Brokers))
	for i, b := range t.Brokers {
		brokerNames[i] = b.HostName
	}

	result := topologyClientMessage{
		Length:       len(t.Brokers),
		ProducerPort: d.config.ProducerPort(),
		ConsumerPort: d.config.ConsumerPort(),
		BrokerNames:  brokerNames,
	}
	return &result
}

func (d *discoverer) newResponseTopologyUsingOrdinals(t *TopologyInfo) *topologyClientMessage {
	serviceName := d.config.ServiceName()
	if serviceName != "" && d.config.PodNamespace() != "" {
		serviceName += "." + d.config.PodNamespace()
	}

	result := topologyClientMessage{
		BaseName:     d.config.BaseHostName(),
		ServiceName:  serviceName,
		Length:       len(t.Brokers),
		ProducerPort: d.config.ProducerPort(),
		ConsumerPort: d.config.ConsumerPort(),
	}
	return &result
}
