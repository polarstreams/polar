package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/consuming"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/data/topics"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/localdb"
	"github.com/jorgebay/soda/internal/metrics"
	"github.com/jorgebay/soda/internal/ownership"
	"github.com/jorgebay/soda/internal/producing"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Info().Msg("Starting Soda")
	rand.Seed(time.Now().UTC().UnixNano())

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
	discoverer := discovery.NewDiscoverer(config, localDbClient)
	datalog := data.NewDatalog(config)
	gossiper := interbroker.NewGossiper(config, discoverer)
	generator := ownership.NewGenerator(discoverer, gossiper, localDbClient)
	producer := producing.NewProducer(config, topicHandler, discoverer, datalog, gossiper)
	consumer := consuming.NewConsumer(config, discoverer, gossiper)

	toInit := []types.Initializer{localDbClient, topicHandler, discoverer, gossiper, generator, producer, consumer}

	for _, item := range toInit {
		if err := item.Init(); err != nil {
			log.Fatal().Err(err).Msg("Exiting")
		}
	}

	// Initialization phase ended
	log.Info().Msg("Start accepting connections from other brokers")
	if err := gossiper.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	if err := gossiper.OpenConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	gossiper.WaitForPeersUp()

	generator.StartGenerations()

	log.Info().Msg("Start accepting connections from producers")
	if err := producer.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	log.Info().Msg("Start accepting connections from consumers")
	if err := consumer.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

	metrics.Serve(discoverer, config)

	log.Info().Msg("Soda started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigChan

	log.Info().Msg("Shutting down")
	localDbClient.Close()
}
