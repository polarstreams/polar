package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/jorgebay/soda/internal/configuration"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/localdb"
	"github.com/jorgebay/soda/internal/producing"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting Soda")
	log.Info().Msg("Initializing local db")
	config := configuration.NewConfig()
	localDbClient := localdb.NewClient(config)
	discoverer := discovery.NewDiscoverer(config)
	producer := producing.NewProducer(config)

	if err := localDbClient.Init(); err != nil {
		log.Fatal().Err(err)
	}

	if err := discoverer.Init(); err != nil {
		log.Fatal().Err(err)
	}

	if err := producer.Init(); err != nil {
		log.Fatal().Err(err)
	}

	// Initialization phase ended
	log.Info().Msg("Start accepting connections")

	if err := producer.AcceptConnections(); err != nil {
		log.Fatal().Err(err)
	}

	log.Info().Msg("Soda started")

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<- sigc

	log.Info().Msg("Shutting down")
}
