package metrics

import (
	"net/http"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	CoalescerMessagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "soda_coalescer_messages_total",
		Help: "The total number of processed messages by the coalescer (producer)",
	})

	CoalescerMessagesPerGroup = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "soda_coalescer_messages_coalesced",
		Help:    "Number of messages coalesced into compressed buffers",
		Buckets: prometheus.ExponentialBuckets(1, 4, 9), // buckets from 1 to 262144
	})

	InterbrokerReceivedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "soda_interbroker_received_coalesced_total",
		Help: "The total number of received message by the interbroker data server",
	})
)

// Serve starts the metrics endpoint
func Serve(discoverer discovery.Discoverer, config conf.Config) {
	port := config.MetricsPort()
	address := utils.GetServiceAddress(port, discoverer, config)
	c := make(chan bool, 1)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		c <- true
		err := http.ListenAndServe(address, nil)
		log.Warn().Err(err).Msg("Metrics server stopped listening")
	}()
	<-c
	log.Info().Msgf("Metrics endpoint started on port %d", port)
}
