package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/data/topics"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/localdb"
	"github.com/jorgebay/soda/internal/producing"
	"github.com/jorgebay/soda/internal/types"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Info().Msg("Starting Soda")

	debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	config := conf.NewConfig()
	log.Info().Msgf("Using home dir as %s", config.HomePath())
	config.CreateAllDirs()

	localDbClient := localdb.NewClient(config)
	topicHandler := topics.NewHandler(config)
	discoverer := discovery.NewDiscoverer(config)
	datalog := data.NewDatalog(config)
	gossiper := interbroker.NewGossiper(config, discoverer)
	producer := producing.NewProducer(config, topicHandler, discoverer, datalog, gossiper)

	toInit := []types.Initializer{localDbClient, topicHandler, discoverer, gossiper, producer}

	for _, item := range toInit {
		if err := item.Init(); err != nil {
			log.Fatal().Err(err).Msg("Exiting")
		}
	}

	// Initialization phase ended
	log.Info().Msg("Start accepting connections")

	if err := gossiper.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	if err := producer.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	if err := gossiper.OpenConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	startMetricsEndpoint()

	log.Info().Msg("Soda started")

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigc

	log.Info().Msg("Shutting down")
	localDbClient.Close()
}

func startMetricsEndpoint() {
	c := make(chan bool, 1)
	go func() {
		c <- true
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9902", nil)
	}()
	<-c
	log.Info().Msg("Metrics endpoint started")
}
