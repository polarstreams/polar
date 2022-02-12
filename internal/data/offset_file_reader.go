package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
)

func ReadProducerOffset(topicId *TopicDataId, config conf.DatalogConfig) (uint64, error) {
	basePath := config.DatalogPath(topicId)
	file, err := os.OpenFile(filepath.Join(basePath, conf.ProducerOffsetFileName), conf.ProducerOffsetFileReadFlags, 0)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	buf := make([]byte, alignmentSize)
	if _, err = file.Read(buf); err != nil {
		return 0, err
	}
	buffer := bytes.NewBuffer(buf)

	var storedOffset uint64
	var storedChecksum uint32

	// Calculate the expected checksum
	expectedChecksum := crc32.ChecksumIEEE(buffer.Bytes()[:8])

	binary.Read(buffer, conf.Endianness, &storedOffset)
	binary.Read(buffer, conf.Endianness, &storedChecksum)

	if expectedChecksum != storedChecksum {
		return 0, fmt.Errorf("Checksum does not match for producer file offset at %s", basePath)
	}

	return storedOffset, nil
}
