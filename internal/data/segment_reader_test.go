package data

import (
	"bytes"
	"encoding/binary"
	"os"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/test/conf/mocks"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SegmentReader", func() {
	Describe("read()", func() {
		It("should support empty reads", func() {
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

		It("should support partial chunks", func() {
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
			chunkBuffer := createTestChunk(50)
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

func createTestChunk(bodyLength int) []byte {
	header := chunkHeader{
		BodyLength:   uint32(bodyLength),
		Start:        10,
		RecordLength: 100,
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
