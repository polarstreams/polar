package conf

// Config represents the application configuration
type Config interface {
	ProducerPort() int32
	ConsumerPort() int32
	AdminPort() int32
}

func NewConfig() Config {
	return &config{}
}

type config struct {
}

func (c *config) ProducerPort() int32 {
	return 8081
}

func (c *config) ConsumerPort() int32 {
	return 8082
}

func (c *config) AdminPort() int32 {
	return 8083
}
