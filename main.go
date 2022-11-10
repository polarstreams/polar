package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
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
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	rand.Seed(time.Now().UTC().UnixNano())

	debug := flag.Bool("debug", false, "sets log level to debug")
	devMode := flag.Bool("dev", false, "starts a single instance in dev mode")
	logPretty := flag.Bool("pretty", false, "logs a human-friendly, colorized output")
	flag.Parse()
	if *debug || os.Getenv(conf.EnvBarcoDebug) == "true" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *logPretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	config := conf.NewConfig(*devMode)

	if !config.DevMode() {
		log.Info().Msg("Starting Barco")
	} else {
		log.Info().Msg("Starting Barco in dev mode")
	}

	log.Info().Msgf("Using architecture target %s", runtime.GOARCH)

	if conf.StartProfiling() {
		log.Info().Msgf("Profiling enabled")
		defer conf.StopProfiling()
	}

	if err := config.Init(); err != nil {
		log.Fatal().Err(err).Msg("Configuration not valid, exiting")
	}

	log.Info().Msgf("Using home dir as %s", config.HomePath())
	if err := config.CreateAllDirs(); err != nil {
		log.Fatal().Err(err).Msg("Data directories could not be created")
	}

	localDbClient := localdb.NewClient(config)
	topicHandler := topics.NewHandler(config)
	discoverer := discovery.NewDiscoverer(config, localDbClient)
	datalog := data.NewDatalog(config)
	gossiper := interbroker.NewGossiper(config, discoverer, localDbClient, datalog)
	generator := ownership.NewGenerator(config, discoverer, gossiper, localDbClient)
	producer := producing.NewProducer(config, topicHandler, discoverer, datalog, gossiper)
	consumer := consuming.NewConsumer(config, localDbClient, discoverer, datalog, gossiper)

	toInit := []types.Initializer{localDbClient, topicHandler, discoverer, gossiper, generator, producer, consumer}

	for _, item := range toInit {
		if err := item.Init(); err != nil {
			log.Fatal().Err(err).Msg("Exiting")
		}
	}

	// Basic initialization phase ended
	metrics.Serve(utils.GetServiceAddress(config.MetricsPort(), discoverer.LocalInfo(), config))

	log.Info().Msg("Start accepting connections from other brokers")
	if err := gossiper.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Gossiper was not able to accept connections, exiting")
	}

	gossiper.OpenConnections()

	gossiper.WaitForPeersUp()

	generator.StartGenerations()

	if err := producer.AcceptConnections(); err != nil {
		log.Fatal().Err(err).Msg("Exiting")
	}

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

	log.Info().Msg("Barco shutting down")

	localDbClient.MarkAsShuttingDown()
	producer.Close()
	consumer.Close()
	gossiper.SendGoobye()

	if config.ShutdownDelay() > 0 {
		log.Info().Msgf("Waiting %s before shutting down gossip", config.ShutdownDelay())
		time.Sleep(config.ShutdownDelay())
	}

	gossiper.Close()
	discoverer.Close()
	localDbClient.Close()
	log.Info().Msg("Barco shutdown completed")
}
