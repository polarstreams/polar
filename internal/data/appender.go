package data

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

const directoryPermissions os.FileMode = 0755
const filePermissions os.FileMode = 0644

// Appender is responsible for creating topic segments and appending blocks to it
type Appender interface {
	Append(block []byte) (segmentId uint64, err error)
}

type appender struct {
	basePath      string
	config        conf.DatalogConfig
	segmentId     uint64
	segmentFile   *os.File
	segmentLength int
}

func newAppender(topic string, token types.Token, genId string, config conf.DatalogConfig) (Appender, error) {
	basePath := config.DatalogPath(topic, token, genId)

	if err := os.MkdirAll(basePath, directoryPermissions); err != nil {
		return nil, err
	}

	return &appender{
		basePath:      basePath,
		config:        config,
		segmentId:     0,
		segmentFile:   nil,
		segmentLength: 0,
	}, nil
}

func (a *appender) Append(block []byte) (segmentId uint64, err error) {
	if err := a.createSegmentIfNeeded(len(block)); err != nil {
		return a.segmentId, err
	}

	if _, err := a.segmentFile.Write(block); err != nil {
		return a.segmentId, err
	}

	return a.segmentId, nil
}

func (a *appender) createSegmentIfNeeded(length int) error {
	createNew := a.segmentFile == nil
	// TODO: Account for group headers
	if a.segmentFile != nil && a.segmentLength+length > a.config.MaxSegmentSize() {
		// TODO: Close previous one

		createNew = true
	}

	if !createNew {
		// Continue using the same file
		return nil
	}

	if createNew {
		name := fmt.Sprintf("%20d.dlog", a.segmentId)
		a.segmentId++

		//TODO: Check Direct IO and create file manually
		//O_DIRECT                         = 0x4000

		f, err := os.OpenFile(filepath.Join(a.basePath, name), conf.WriteFlags, filePermissions)
		if err != nil {
			return err
		}
		a.segmentFile = f
	}

	return nil
}
