package data

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

type Datalog interface {
	types.Initializer
	CreateAppender(topic string, token types.Token, genId string, config conf.DatalogConfig) (Appender, error)
}

func NewDatalog(config conf.Config) Datalog {
	return &datalog{
		config,
	}
}

type datalog struct {
	config conf.Config
}

func (d *datalog) Init() error {
	return nil
}

func (d *datalog) CreateAppender(
	topic string,
	token types.Token,
	genId string,
	config conf.DatalogConfig,
) (Appender, error) {
	return newAppender(topic, token, genId, config)
}
