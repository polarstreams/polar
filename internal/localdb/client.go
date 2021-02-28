package localdb

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

// Client represents a local db client.
type Client interface {
	types.Initializer
}

// NewClient creates a new instance of Client.
func NewClient(config conf.Config) Client {
	return &client{
		config,
	}
}

type client struct {
	config conf.Config
}

func (c *client) Init() error {
	return nil
}
