package configuration

// Config represents the application configuration
type Config interface {
}

func NewConfig() Config {
	return &config{}
}

type config struct {
}
