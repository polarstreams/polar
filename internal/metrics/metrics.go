package metrics

import (
	"net/http"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	CoalescerMessagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "barco_coalescer_messages_total",
		Help: "The total number of processed messages by the coalescer (producer)",
	})

	CoalescerMessagesPerGroup = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "barco_coalescer_messages_coalesced",
		Help:    "Number of messages coalesced into compressed buffers",
		Buckets: prometheus.ExponentialBuckets(2, 2, 9), // buckets from 1 to 512
	})

	InterbrokerReceivedGroups = promauto.NewCounter(prometheus.CounterOpts{
		Name: "barco_interbroker_received_coalesced_total",
		Help: "The total number of coalesced group messages received by the interbroker data server",
	})

	ReroutedSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "barco_producing_rerouting_sent_total",
		Help: "The total number of re-routed messages sent by this broker",
	}, []string{"target"})

	ReroutedReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "barco_producing_rerouting_received_total",
		Help: "The total number of re-routed messages received by the broker",
	})

	SegmentFlushKib = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "barco_segment_flushed_kib",
		Help:    "Number of Kibibytes flushed to disk",
		Buckets: prometheus.ExponentialBuckets(2, 4, 8), // buckets from 2Kib to 32Mib
	})

	ConsumerConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "barco_consumer_connections",
		Help: "The number of open connections from consumers",
	})
)

// Serve starts the metrics endpoint
func Serve(discoverer discovery.Discoverer, config conf.Config) {
	port := config.MetricsPort()
	address := utils.GetServiceAddress(port, discoverer.LocalInfo(), config)
	log.Info().Msgf("Starting metrics endpoint on port %d", port)
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
