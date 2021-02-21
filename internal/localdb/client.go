package localdb

import "github.com/jorgebay/soda/internal/conf"

// NewClient creates a new instance of Client.
func NewClient(config conf.Config) Client {
	return &client{
		config,
	}
}

// Client represents a local db client.
type Client interface {
	Init() error
}

type client struct {
	config conf.Config
}

func (c *client) Init() error {
	return nil
}
