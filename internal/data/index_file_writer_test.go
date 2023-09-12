package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/test/conf/mocks"
	"github.com/polarstreams/polar/internal/utils"
)

var _ = Describe("indexFileWriter", func() {
	It("should write only when its above threshold", func() {
		config := new(mocks.Config)
		config.On("IndexFilePeriodBytes").Return(200)

		dir, err := os.MkdirTemp("", "test_index")
		Expect(err).NotTo(HaveOccurred())

		w := &indexFileWriter{
			items:        make(chan indexFileItem),
			basePath:     dir,
			config:       config,
			offsetWriter: newOffsetFileWriter(),
		}

		go w.writeLoop()

		segmentId := int64(123)
		w.append(segmentId, 0, 100, 0)
		assertStored(dir, segmentId, []indexOffset{}) // Nothing stored
		w.append(segmentId, 150, 200, 0)

		expected := []indexOffset{{Offset: 150, FileOffset: 200}}

		assertStored(dir, segmentId, expected)

		w.append(segmentId, 180, 250, 0)
		w.append(segmentId, 190, 350, 0)
		w.append(segmentId, 191, 420, 0)

		expected = append(expected, indexOffset{Offset: 191, FileOffset: 420})
		assertStored(dir, segmentId, expected)

		close(w.items)
	})

	It("should close the file and continue with the next", func() {
		config := new(mocks.Config)
		config.On("IndexFilePeriodBytes").Return(200)

		dir, err := os.MkdirTemp("", "test_index_close")
		Expect(err).NotTo(HaveOccurred())

		w := &indexFileWriter{
			items:        make(chan indexFileItem),
			basePath:     dir,
			config:       config,
			offsetWriter: newOffsetFileWriter(),
		}

		go w.writeLoop()

		segmentId := int64(123)
		w.append(segmentId, 0, 100, 0)
		assertStored(dir, segmentId, []indexOffset{}) // Nothing stored
		w.closeFile(segmentId, 0)

		// Noop
		segmentId += 1
		w.closeFile(segmentId, 0)

		segmentId += 1
		w.append(segmentId, 352, 1001, 0)
		w.closeFile(segmentId, 0)

		assertStored(dir, segmentId, []indexOffset{{Offset: 352, FileOffset: 1001}})

		close(w.items)
	})

	It("should write the producer offset file", func() {
		config := new(mocks.Config)
		config.On("IndexFilePeriodBytes").Return(200)

		dir, err := os.MkdirTemp("", "test_index_and_offset")
		Expect(err).NotTo(HaveOccurred())

		w := &indexFileWriter{
			items:        make(chan indexFileItem),
			basePath:     dir,
			config:       config,
			offsetWriter: newOffsetFileWriter(),
		}

		go w.writeLoop()

		segmentId := int64(123)
		w.append(segmentId, 0, 100, 50)
		assertProducerOffsetStored(dir, 50)
		w.append(segmentId, 150, 200, 180)
		assertProducerOffsetStored(dir, 180)
		w.closeFile(segmentId, 190)
		assertProducerOffsetStored(dir, 190)

		close(w.items)
	})
})

func assertStored(basePath string, segmentId int64, values []indexOffset) {
	expectedFileLength := utils.BinarySize(indexOffset{}) * len(values) // 8 + 8 + 4
	var blob []byte
	maxWaits := 2500 // ~ 5 seconds
	if len(values) == 0 {
		maxWaits = 10
	}
	// Wait for the data to be stored in the file
	for i := 0; i < maxWaits; i++ {
		time.Sleep(20 * time.Millisecond)
		name := fmt.Sprintf("%020d.index", segmentId)
		b, err := os.ReadFile(filepath.Join(basePath, name))
		if err == nil && len(b) == expectedFileLength {
			blob = b
			break
		}
	}

	Expect(blob).To(HaveLen(expectedFileLength))

	if expectedFileLength == 0 {
		return
	}
	buffer := bytes.NewBuffer(blob)

	for _, value := range values {
		storedIndex := indexOffset{}
		// Calculate the expected checksum
		buf := buffer.Bytes()[:16]
		value.Checksum = crc32.ChecksumIEEE(buf)

		binary.Read(buffer, conf.Endianness, &storedIndex)
		Expect(storedIndex).To(Equal(value))
	}
}

func assertProducerOffsetStored(basePath string, expected uint64) {
	var blob []byte
	var storedOffset uint64
	maxWaits := 2500 // ~ 5 seconds

	// Wait for the data to be stored in the file
	for i := 0; i < maxWaits; i++ {
		time.Sleep(20 * time.Millisecond)
		b, err := os.ReadFile(filepath.Join(basePath, conf.ProducerOffsetFileName))
		if err == nil && len(b) == offsetFileSize {
			buffer := bytes.NewBuffer(b)
			binary.Read(buffer, conf.Endianness, &storedOffset)
			if storedOffset == expected {
				blob = b
				break
			}
		}
	}

	Expect(blob).To(HaveLen(offsetFileSize))
	buffer := bytes.NewBuffer(blob)

	var storedChecksum uint32
	// Calculate the expected checksum
	buf := buffer.Bytes()[:8]
	expectedChecksum := crc32.ChecksumIEEE(buf)

	binary.Read(buffer, conf.Endianness, &storedOffset)
	binary.Read(buffer, conf.Endianness, &storedChecksum)
	Expect(storedOffset).To(Equal(expected))
	Expect(storedChecksum).To(Equal(expectedChecksum))
}
