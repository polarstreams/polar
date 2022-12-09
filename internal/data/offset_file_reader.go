package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
)

func readProducerOffset(topicId *TopicDataId, config conf.DatalogConfig) (int64, error) {
	basePath := config.DatalogPath(topicId)
	file, err := os.OpenFile(filepath.Join(basePath, conf.ProducerOffsetFileName), conf.ProducerOffsetFileReadFlags, 0)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	buf := make([]byte, offsetFileSize)
	if _, err = file.Read(buf); err != nil {
		return 0, err
	}
	buffer := bytes.NewBuffer(buf)

	var storedOffset int64
	var storedChecksum uint32

	// Calculate the expected checksum
	expectedChecksum := crc32.ChecksumIEEE(buffer.Bytes()[:8])

	if err := binary.Read(buffer, conf.Endianness, &storedOffset); err != nil {
		return 0, err
	}
	if err := binary.Read(buffer, conf.Endianness, &storedChecksum); err != nil {
		return 0, err
	}

	if expectedChecksum != storedChecksum {
		return 0, fmt.Errorf("Checksum does not match for producer file offset at %s", basePath)
	}

	return storedOffset, nil
}
