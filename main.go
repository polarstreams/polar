package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/consuming"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/data/topics"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	"github.com/barcostreams/barco/internal/localdb"
	"github.com/barcostreams/barco/internal/metrics"
	"github.com/barcostreams/barco/internal/ownership"
	"github.com/barcostreams/barco/internal/producing"
	"github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Info().Msg("Starting Barco")
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
	gossiper := interbroker.NewGossiper(config, discoverer, localDbClient)
	generator := ownership.NewGenerator(discoverer, gossiper, localDbClient)
	producer := producing.NewProducer(config, topicHandler, discoverer, datalog, gossiper)
	consumer := consuming.NewConsumer(config, localDbClient, discoverer, gossiper)

	toInit := []types.Initializer{localDbClient, topicHandler, discoverer, gossiper, generator, producer, consumer}

	for _, item := range toInit {
		if err := item.Init(); err != nil {
			log.Fatal().Err(err).Msg("Exiting")
		}
	}

	// Basic initialization phase ended
	metrics.Serve(discoverer, config)

	log.Info().Msg("Start accepting connections from other brokers")
	if err := gossiper.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Gossiper was not able to accept connections, exiting")
	}

	gossiper.OpenConnections()

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

	log.Info().Msg("Barco started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigChan

	log.Info().Msg("Shutting down")
	localDbClient.Close()
}
