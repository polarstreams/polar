package data

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

type Datalog interface {
	types.Initializer
	Appender
}

type Appender interface {
	Append(token types.Token, topic string, body []byte) error
}

func NewDatalog(config conf.Config) Datalog {
	return &datalog{
		config,
	}
}

type datalog struct {
	config conf.Config
}

func (d *datalog) Append(token types.Token, topic string, body []byte) error {
	return nil
}

func (d *datalog) Init() error {
	return nil
}
