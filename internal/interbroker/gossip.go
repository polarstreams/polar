package interbroker

import "github.com/jorgebay/soda/internal/configuration"

// Gossiper is responsible for communicating with other peers.
type Gossiper interface {
	Init(config *configuration.Config) error

	// Starts accepting connections from peers.
	AcceptConnections() error

	// Starts opening connections to known peers.
	OpenConnections() error

	//TODO
	SendMessage()
}

func NewGossiper() Gossiper {
	return nil
}