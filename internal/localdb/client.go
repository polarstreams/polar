package localdb

import "github.com/jorgebay/soda/internal/configuration"

// NewClient creates a new instance of Client.
func NewClient(config configuration.Config) Client {
	return &client{
		config,
	}
}

// Client represents a local db client.
type Client interface {
	Init() error
}

type client struct {
	config configuration.Config
}

func (c *client) Init() error {
	return nil
}
