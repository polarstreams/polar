package interbroker

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

// Gossiper is responsible for communicating with other peers.
type Gossiper interface {
	types.Initializer

	// Starts accepting connections from peers.
	AcceptConnections() error

	// Starts opening connections to known peers.
	OpenConnections() error

	//TODO
	SendMessage()
}

func NewGossiper(config conf.Config) Gossiper {
	return nil
}
