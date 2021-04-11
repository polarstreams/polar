package data

import (
	"os"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

const directoryPermissions os.FileMode = 0755
const filePermissions os.FileMode = 0644

// Appender is responsible for creating topic segments and appending blocks to it
type Appender interface {
	Append(block []byte) error
}

type appender struct {
	basePath string
	config   conf.DatalogConfig
	segment  uint64
}

func newAppender(topic string, token types.Token, genId string, config conf.DatalogConfig) (Appender, error) {
	basePath := config.DatalogPath(topic, token, genId)

	if err := os.MkdirAll(basePath, directoryPermissions); err != nil {
		return nil, err
	}

	return &appender{
		basePath: basePath,
		config:   config,
		segment:  0,
	}, nil
}

func (a *appender) Append(block []byte) error {
	return nil
}
