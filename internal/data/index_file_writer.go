package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

// Represents a writer for index files
type indexFileWriter struct {
	items    chan indexFileItem
	basePath string
	config   conf.DatalogConfig
	closed   chan bool
}

func newIndexFileWriter(basePath string, config conf.DatalogConfig) *indexFileWriter {
	w := &indexFileWriter{
		items:    make(chan indexFileItem, 1), // Try not to block when sending
		config:   config,
		basePath: basePath,
		closed:   make(chan bool, 1),
	}
	go w.writeLoop()
	return w
}

// writeLoop writes the index files
func (w *indexFileWriter) writeLoop() {
	var segmentId *uint64
	var file *os.File
	lastStoredFileOffset := int64(0)
	buffer := utils.NewBufferCap(16)
	writeThreshold := int64(w.config.IndexFilePeriodBytes())
	for item := range w.items {
		if item.toClose {
			// File closing
			if file != nil {
				if err := file.Close(); err != nil {
					log.Err(err).Msgf("Index file closed with error on path %s", w.basePath)
				} else {
					log.Debug().Msgf("Index file closed on path %s", w.basePath)
				}
				file = nil
				segmentId = nil
				lastStoredFileOffset = 0
			}
			continue
		}

		if segmentId == nil {
			name := fmt.Sprintf("%020d.%s", item.segmentId, conf.IndexFileExtension)
			f, err := os.OpenFile(filepath.Join(w.basePath, name), conf.IndexFileWriteFlags, FilePermissions)
			if err != nil {
				log.Err(err).Msgf("Index file %s could not be created on path %s", w.basePath, name)
				continue
			} else {
				log.Debug().Msgf("Index file created on path %s", w.basePath)
			}
			file = f
			id := item.segmentId
			segmentId = &id
		}

		if item.fileOffset-lastStoredFileOffset >= writeThreshold {
			buffer.Reset()
			binary.Write(buffer, conf.Endianness, item.offset)
			binary.Write(buffer, conf.Endianness, item.fileOffset)
			binary.Write(buffer, conf.Endianness, crc32.ChecksumIEEE(buffer.Bytes()))
			if _, err := file.Write(buffer.Bytes()); err != nil {
				log.Err(err).Msgf("There was an error writing to the index file on path %s", w.basePath)
			} else {
				log.Debug().Msgf("Written to %d index file on path %s", *segmentId, w.basePath)
			}
			lastStoredFileOffset = item.fileOffset
		}
	}

	w.closed <- true
}

// When conditions apply, it adds a line to the index file mapping file offset with message offset
// in the background.
func (w *indexFileWriter) append(segmentId uint64, offset uint64, fileOffset int64) {
	w.items <- indexFileItem{
		segmentId:  segmentId,
		offset:     offset,
		fileOffset: fileOffset,
	}
}

// Closes the current file in the background
func (w *indexFileWriter) closeFile(segmentId uint64) {
	w.items <- indexFileItem{
		segmentId: segmentId,
		toClose:   true,
	}
}
