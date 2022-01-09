package data

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/test/conf/mocks"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SegmentReader", func() {
	Describe("read()", func() {
		XIt("should support empty reads", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(1 * conf.Mib)
			s := &SegmentReader{
				config:    config,
				Items:     make(chan ReadItem, 16),
				pollDelay: 1 * time.Millisecond,
			}

			file, err := os.CreateTemp("", "segment_file*.dlog")
			Expect(err).NotTo(HaveOccurred())
			s.segmentFile = file

			go s.read()

			item := newTestReadItem()
			pollChunkAndAssert(s, item, 0)
		})

		XIt("should support partial chunks", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(1 * conf.Mib)
			s := &SegmentReader{
				config:    config,
				Items:     make(chan ReadItem, 16),
				pollDelay: 1 * time.Millisecond,
			}

			file, err := os.CreateTemp("", "segment_file*.dlog")
			Expect(err).NotTo(HaveOccurred())

			s.segmentFile, err = os.Open(file.Name())
			Expect(err).NotTo(HaveOccurred())
			chunkBuffer := createTestChunk(50, 10, 100)
			file.Write(chunkBuffer[:2])

			go s.read()

			item := newTestReadItem()
			// Initially is empty
			pollChunkAndAssert(s, item, 0)

			// Write the rest of the header: still empty result
			file.Write(chunkBuffer[2:chunkHeaderSize])
			pollChunkAndAssert(s, item, 0)

			// Write the rest of the body: data is returned
			file.Write(chunkBuffer[chunkHeaderSize:])
			pollChunkAndAssert(s, item, 50)

			close(s.Items)
		})

		It("should continue reading the next file", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(2048)

			dir, err := os.MkdirTemp("", "read_next_file*")
			Expect(err).NotTo(HaveOccurred())
			firstFile, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			secondFile, err := os.Create(filepath.Join(dir, "00020.dlog"))

			// Write to the files
			buffer := createTestChunk(512-chunkHeaderSize, 0, 20)
			_, err = firstFile.Write(buffer)
			Expect(err).NotTo(HaveOccurred())

			buffer = createTestChunk(512*2-chunkHeaderSize, 100, 30)
			_, err = secondFile.Write(buffer)

			firstFile.Sync()
			secondFile.Sync()

			firstFile.Close()
			secondFile.Close()

			s := &SegmentReader{
				config:    config,
				Items:     make(chan ReadItem, 16),
				basePath:  dir,
				pollDelay: 1 * time.Millisecond,
			}
			go s.read()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunkAndAssert(s, item, 512-chunkHeaderSize)
			// First empty poll
			pollChunkAndAssert(s, item, 0)
			// Second empty poll swaps the file
			pollChunkAndAssert(s, item, 0)
			pollChunkAndAssert(s, item, 512*2-chunkHeaderSize)
		})

		XIt("should read alignment", func() {

		})
	})
})

type testReadItem struct {
	chunkResult chan SegmentChunk
	errorResult chan error
}

func newTestReadItem() *testReadItem {
	return &testReadItem{
		chunkResult: make(chan SegmentChunk),
		errorResult: make(chan error),
	}
}

func (r *testReadItem) SetResult(err error, chunk SegmentChunk) {
	r.chunkResult <- chunk
	r.errorResult <- err
}

func createTestChunk(bodyLength int, start int, recordLength int) []byte {
	header := chunkHeader{
		BodyLength:   uint32(bodyLength),
		Start:        uint64(start),
		RecordLength: uint32(recordLength),
		Crc:          123,
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, conf.Endianness, &header)

	body := make([]byte, bodyLength)
	for i := 0; i < bodyLength; i++ {
		body[i] = byte(i)
	}
	buffer.Write(body)

	return buffer.Bytes()
}

func pollChunkAndAssert(s *SegmentReader, item *testReadItem, bodyLength int) {
	s.Items <- item
	chunk := <-item.chunkResult
	err := <-item.errorResult

	Expect(err).NotTo(HaveOccurred())
	Expect(chunk).ToNot(BeNil())
	Expect(chunk.DataBlock()).To(HaveLen(bodyLength))

	if bodyLength > 0 {
		expectedBody := make([]byte, bodyLength)
		for i := 0; i < bodyLength; i++ {
			expectedBody[i] = byte(i)
		}

		Expect(chunk.DataBlock()).To(Equal(expectedBody))
	}
}
