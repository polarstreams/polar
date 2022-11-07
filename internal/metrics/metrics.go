package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	ProducerMessagesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "barco_producer_requests_total",
		Help: "The total number of requests received by the producer server",
	})

	ProducerMessagesBodyBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "barco_producer_requests_body_bytes_total",
		Help: "The total number of bytes for all the request bodies received by the producer server",
	})

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

	InterbrokerDataMissedWrites = promauto.NewCounter(prometheus.CounterOpts{
		Name: "barco_interbroker_missed_writes_total",
		Help: "The total number of messages that couldn't be written because the timeout elapsed",
	})

	SegmentFlushBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "barco_segment_flushed_bytes",
		Help:    "The amount of bytes flushed to disk",
		Buckets: prometheus.ExponentialBuckets(2, 7, 10), // buckets from 2 to 80MiB
	})

	AllocationPoolAvailableBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "barco_producing_allocation_pool_available_bytes",
		Help: "The number of bytes available to allocate",
	})

	ActiveConsumers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "barco_consumer_active",
		Help: "The number of active consumers connected to this broker",
	})

	ConsumerOpenConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "barco_consumer_open_connections",
		Help: "The number of open connections to consumers that are being served",
	})
)

// Serve starts the metrics endpoint
func Serve(address string) {
	c := make(chan bool, 1)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		c <- true
		err := http.ListenAndServe(address, nil)
		log.Warn().Err(err).Msg("Metrics server stopped listening")
	}()
	<-c
	log.Info().Msgf("Metrics endpoint started on %s", address)
}
