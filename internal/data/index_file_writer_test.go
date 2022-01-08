package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/test/conf/mocks"
	"github.com/jorgebay/soda/internal/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("indexFileWriter", func() {
	It("should write only when its above threshold", func() {
		config := new(mocks.Config)
		config.On("IndexFilePeriodBytes").Return(200)

		dir, err := ioutil.TempDir("", "test_index")
		Expect(err).NotTo(HaveOccurred())

		w := &indexFileWriter{
			items:    make(chan indexFileItem),
			basePath: dir,
			config:   config,
		}

		go w.writeLoop()

		segmentId := uint64(123)
		w.append(segmentId, 0, 100)
		assertStored(dir, segmentId, []indexOffset{}) // Nothing stored
		w.append(segmentId, 150, 200)

		expected := []indexOffset{{Offset: 150, FileOffset: 200}}

		assertStored(dir, segmentId, expected)

		w.append(segmentId, 180, 250)
		w.append(segmentId, 190, 350)
		w.append(segmentId, 191, 420)

		expected = append(expected, indexOffset{Offset: 191, FileOffset: 420})
		assertStored(dir, segmentId, expected)

		close(w.items)
	})

	It("should close the file and continue with the next", func() {
		config := new(mocks.Config)
		config.On("IndexFilePeriodBytes").Return(200)

		dir, err := ioutil.TempDir("", "test_index_close")
		Expect(err).NotTo(HaveOccurred())

		w := &indexFileWriter{
			items:    make(chan indexFileItem),
			basePath: dir,
			config:   config,
		}

		go w.writeLoop()

		segmentId := uint64(123)
		w.append(segmentId, 0, 100)
		assertStored(dir, segmentId, []indexOffset{}) // Nothing stored
		w.closeFile(segmentId)

		// Noop
		segmentId += 1
		w.closeFile(segmentId)

		segmentId += 1
		w.append(segmentId, 352, 1001)
		w.closeFile(segmentId)

		assertStored(dir, segmentId, []indexOffset{{Offset: 352, FileOffset: 1001}})

		close(w.items)
	})
})

func assertStored(basePath string, segmentId uint64, values []indexOffset) {
	expectedFileLength := utils.BinarySize(indexOffset{}) * len(values) // 8 + 8 + 4
	var blob []byte
	maxWaits := 100
	if len(values) == 0 {
		maxWaits = 10
	}
	// Wait for the data to be stored in the file
	for i := 0; i < maxWaits; i++ {
		time.Sleep(20)
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
