package main

import (
	"github.com/jorgebay/soda/internal/configuration"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/localdb"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting Soda")
	log.Info().Msg("Initializing local db") 
	config := configuration.Config{}
	localDbClient := localdb.NewClient()
	discoverer := discovery.NewDiscoverer(&config)

	if err := localDbClient.Init(); err != nil {
		log.Fatal().Err(err)
	}

	if err := discoverer.Init(); err != nil {
		log.Fatal().Err(err)
	}

	log.Info().Msg("Soda started")
}
