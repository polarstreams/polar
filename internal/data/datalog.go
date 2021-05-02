package data

import (
	"os"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

const DirectoryPermissions os.FileMode = 0755
const FilePermissions os.FileMode = 0644

type Datalog interface {
	types.Initializer
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
