package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data/topics"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/localdb"
	"github.com/jorgebay/soda/internal/producing"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting Soda")
	log.Info().Msg("Initializing local db")
	config := conf.NewConfig()
	localDbClient := localdb.NewClient(config)
	topicHandler := topics.NewHandler(config)
	discoverer := discovery.NewDiscoverer(config)
	producer := producing.NewProducer(config, topicHandler)

	toInit := []types.Initializer{localDbClient, topicHandler, discoverer, producer}

	for _, item := range toInit {
		if err := item.Init(); err != nil {
			log.Fatal().Err(err)
		}
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
	<-sigc

	log.Info().Msg("Shutting down")
}
