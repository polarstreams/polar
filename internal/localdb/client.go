package localdb

// NewClient creates a new instance of Client.
func NewClient() Client {
	return &client{}
}

// Client represents a local db client.
type Client interface {
	Init() error
}

type client struct {
}

func (c *client) Init() error {
	return nil
}
