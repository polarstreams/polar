package configuration

// Config represents the application configuration
type Config interface {
	ProducerPort() int32
}

func NewConfig() Config {
	return &config{}
}

type config struct {
}

func (c *config) ProducerPort() int32 {
	return 8080
}