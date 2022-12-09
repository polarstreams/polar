package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/consuming"
	"github.com/polarstreams/polar/internal/data"
	"github.com/polarstreams/polar/internal/data/topics"
	"github.com/polarstreams/polar/internal/discovery"
	"github.com/polarstreams/polar/internal/interbroker"
	"github.com/polarstreams/polar/internal/localdb"
	"github.com/polarstreams/polar/internal/metrics"
	"github.com/polarstreams/polar/internal/ownership"
	"github.com/polarstreams/polar/internal/producing"
	"github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
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
	if *debug || os.Getenv(conf.EnvDebug) == "true" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *logPretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	config := conf.NewConfig(*devMode)

	if !config.DevMode() {
		log.Info().Msg("Starting PolarStreams")
	} else {
		log.Info().Msg("Starting PolarStreams in dev mode")
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

	log.Info().Msg("PolarStreams started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigChan

	log.Info().Msg("PolarStreams shutting down")

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
	log.Info().Msg("PolarStreams shutdown completed")
}
