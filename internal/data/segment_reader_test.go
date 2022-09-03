package data

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/test/conf/mocks"
	tMocks "github.com/barcostreams/barco/internal/test/types/mocks"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("SegmentReader", func() {
	Describe("read()", func() {
		It("should support empty reads", func() {
			s := newTestReader()
			file, err := os.CreateTemp("", "segment_file*.dlog")
			Expect(err).NotTo(HaveOccurred())
			s.segmentFile = file

			go s.read()

			item := newTestReadItem()
			pollChunkOnce(s, item, 0)
		})

		It("should support partial chunks", func() {
			s := newTestReader()
			file, err := os.CreateTemp("", "segment_file*.dlog")
			Expect(err).NotTo(HaveOccurred())

			s.segmentFile, err = os.Open(file.Name())
			Expect(err).NotTo(HaveOccurred())
			chunkBuffer := createTestChunk(50, 0, 100)
			file.Write(chunkBuffer[:2])

			go s.read()

			item := newTestReadItem()
			// Initially is empty
			pollChunkOnce(s, item, 0)

			// Write the rest of the header: still empty result
			file.Write(chunkBuffer[2:chunkHeaderSize])
			pollChunkOnce(s, item, 0)

			// Write the rest of the body: data is returned
			file.Write(chunkBuffer[chunkHeaderSize:])
			pollChunk(s, item, 50)

			close(s.Items)
		})

		It("should continue reading the next file", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(2048)
			config.On("AutoCommitInterval").Return(1 * time.Second)

			dir, err := os.MkdirTemp("", "read_next_file*")
			Expect(err).NotTo(HaveOccurred())
			firstFile, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			secondFile, err := os.Create(filepath.Join(dir, "00020.dlog"))

			// Write to the files
			_, err = firstFile.Write(createTestChunk(512-chunkHeaderSize, 0, 20))
			Expect(err).NotTo(HaveOccurred())

			// buffer = createTestChunk(512-chunkHeaderSize, 50, 40)
			_, err = secondFile.Write(createTestChunk(512*2-chunkHeaderSize, 20, 30))
			Expect(err).NotTo(HaveOccurred())
			_, err = secondFile.Write(createTestChunk(512-chunkHeaderSize, 50, 40))
			Expect(err).NotTo(HaveOccurred())

			firstFile.Sync()
			secondFile.Sync()

			s := newTestReader()
			s.config = config
			s.basePath = dir

			go s.read()
			defer firstFile.Close()
			defer secondFile.Close()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 512-chunkHeaderSize)
			// First empty poll
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 0)
			// Second empty poll swaps the file
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 512*2-chunkHeaderSize)
			pollChunkOnce(s, item, 512-chunkHeaderSize)
		})

		It("should read alignment", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(1024)
			config.On("AutoCommitInterval").Return(1 * time.Second)

			dir, err := os.MkdirTemp("", "read_alignment*")
			Expect(err).NotTo(HaveOccurred())
			file, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			defer file.Close()

			// Write a chunk, followed by an alignment buffer
			_, err = file.Write(createTestChunk(510-chunkHeaderSize, 0, 20))
			Expect(err).NotTo(HaveOccurred())
			_, err = file.Write([]byte{0x80, 0x80})
			Expect(err).NotTo(HaveOccurred())
			_, err = file.Write(createTestChunk(512*2-3-chunkHeaderSize, 20, 15))
			Expect(err).NotTo(HaveOccurred())
			_, err = file.Write([]byte{0x80, 0x80, 0x80})

			file.Sync()

			s := newTestReader()
			s.config = config
			s.basePath = dir

			go s.read()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 510-chunkHeaderSize)
			// First empty poll
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 512*2-3-chunkHeaderSize)
			pollChunkOnce(s, item, 0)
		})

		It("should poll until there's new data", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(1024)
			config.On("AutoCommitInterval").Return(1 * time.Second)

			dir, err := os.MkdirTemp("", "poll_new_data*")
			Expect(err).NotTo(HaveOccurred())
			file, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			defer file.Close()

			// Write a chunk, followed by an alignment buffer
			_, err = file.Write(createTestChunk(510-chunkHeaderSize, 0, 20))
			Expect(err).NotTo(HaveOccurred())
			_, err = file.Write([]byte{0x80, 0x80})
			Expect(err).NotTo(HaveOccurred())

			file.Sync()

			s := newTestReader()
			s.config = config
			s.basePath = dir

			go s.read()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 510-chunkHeaderSize)
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 0)
			pollChunkOnce(s, item, 0)

			// New data
			_, err = file.Write(createTestChunk(512-chunkHeaderSize, 20, 30))
			Expect(err).NotTo(HaveOccurred())
			file.Sync()

			pollChunkOnce(s, item, 512-chunkHeaderSize)
		})

		It("should skip files with invalid names", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(2048)
			config.On("AutoCommitInterval").Return(1 * time.Second)

			dir, err := os.MkdirTemp("", "poll_gap_empty_file_*")
			Expect(err).NotTo(HaveOccurred())
			file1, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			file2, err := os.Create(filepath.Join(dir, "invalid.dlog"))
			Expect(err).NotTo(HaveOccurred())
			file3, err := os.Create(filepath.Join(dir, "00050.dlog"))
			Expect(err).NotTo(HaveOccurred())
			defer file1.Close()
			defer file2.Close()
			defer file3.Close()

			// Write a chunk, followed by an alignment buffer
			file1.Write(createAlignedChunk(100, 0, 50))
			file3.Write(createAlignedChunk(200, 50, 50))
			file1.Sync()
			file3.Sync()

			s := newTestReader()
			s.config = config
			s.basePath = dir

			go s.read()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunk(s, item, 100)
			pollChunk(s, item, 200)
		})

		It("should recognize gaps from chunks", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(2048)
			config.On("AutoCommitInterval").Return(1 * time.Second)

			dir, err := os.MkdirTemp("", "poll_stream_*")
			Expect(err).NotTo(HaveOccurred())
			file, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			defer file.Close()

			// Write a chunk, followed by an alignment buffer
			_, err = file.Write(createAlignedChunk(1200, 0, 20))
			Expect(err).NotTo(HaveOccurred())
			file.Write(createAlignedChunk(200, 50, 50))
			file.Write(createAlignedChunk(300, 100, 120))

			file.Sync()

			rr := &rrFake{
				// fake chunks starting at 20 and finishing at 50
				streamBuf: append(createTestChunk(100, 20, 15), createTestChunk(150, 35, 15)...),
			}

			s := newTestReader()
			s.replicationReader = rr
			s.config = config
			s.basePath = dir

			go s.read()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunk(s, item, 1200)
			pollChunk(s, item, 100)
			pollChunk(s, item, 150)
			Expect(atomic.LoadInt64(&rr.streamCalled)).To(BeNumerically(">", int64(0)))

			pollChunk(s, item, 200)
			pollChunk(s, item, 300)
		})

		It("should recognize gaps from empty files", func() {
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(2048)
			config.On("AutoCommitInterval").Return(1 * time.Second)

			dir, err := os.MkdirTemp("", "poll_gap_empty_file_*")
			Expect(err).NotTo(HaveOccurred())
			file1, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			emptyFile2, err := os.Create(filepath.Join(dir, "00020.dlog"))
			Expect(err).NotTo(HaveOccurred())
			file3, err := os.Create(filepath.Join(dir, "00050.dlog"))
			Expect(err).NotTo(HaveOccurred())
			defer file1.Close()
			defer emptyFile2.Close()
			defer file3.Close()

			// Write a chunk, followed by an alignment buffer
			_, err = file1.Write(createAlignedChunk(100, 0, 20))
			Expect(err).NotTo(HaveOccurred())
			file3.Write(createAlignedChunk(300, 50, 50))

			file1.Sync()

			rr := &rrFake{
				// fake chunk starting at 20 and finishing at 50
				streamBuf: createTestChunk(200, 20, 30),
			}

			s := newTestReader()
			s.replicationReader = rr
			s.config = config
			s.basePath = dir

			go s.read()
			defer close(s.Items)

			item := newTestReadItem()
			pollChunk(s, item, 100)
			pollChunk(s, item, 200)
			pollChunk(s, item, 300)
			Expect(atomic.LoadInt64(&rr.streamCalled)).To(BeNumerically(">", int64(0)))
		})

		It("should reset when the origin changes", func() {
			dir, err := os.MkdirTemp("", "reset_origin_*")
			Expect(err).NotTo(HaveOccurred())
			file1, err := os.Create(filepath.Join(dir, "00000.dlog"))
			Expect(err).NotTo(HaveOccurred())
			file2, err := os.Create(filepath.Join(dir, "00050.dlog"))
			Expect(err).NotTo(HaveOccurred())
			defer file1.Close()
			defer file2.Close()

			// Write a chunk, followed by an alignment buffer
			_, err = file1.Write(createAlignedChunk(100, 0, 20))
			Expect(err).NotTo(HaveOccurred())
			_, err = file1.Write(createAlignedChunk(200, 20, 30))
			Expect(err).NotTo(HaveOccurred())
			_, err = file2.Write(createAlignedChunk(300, 50, 50))
			Expect(err).NotTo(HaveOccurred())

			file1.Sync()
			file2.Sync()

			mockedOffset := Offset{
				Offset:  20,
				Version: 3,
			}
			setCalls := int64(0)
			config := new(mocks.Config)
			config.On("ReadAheadSize").Return(1 * conf.MiB)
			config.On("AutoCommitInterval").Return(1 * time.Nanosecond) // Commit always
			offsetState := new(tMocks.OffsetState)
			offsetState.
				On("Set", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Run(func(_ mock.Arguments) {
					atomic.AddInt64(&setCalls, 1)
				})

			offsetState.On("Get", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mockedOffset)

			s := &SegmentReader{
				config:      config,
				Items:       make(chan ReadItem, 16),
				offsetState: offsetState,
				headerBuf:   make([]byte, chunkHeaderSize),
				Topic: TopicDataId{
					Name:       "abc",
					Token:      0,
					RangeIndex: 0,
					Version:    3,
				},
				isLeader: true,
				basePath: dir,
			}

			go s.read()
			defer close(s.Items)

			item1 := newTestReadItemWithOrigin(uuid.New())
			pollChunk(s, item1, 100)
			pollChunk(s, item1, 200)
			pollChunk(s, item1, 300)

			// New origin, it should go back to use the committed offset
			item2 := newTestReadItemWithOrigin(uuid.New())
			pollChunk(s, item2, 200)
			pollChunk(s, item2, 300)
			offsetState.AssertNumberOfCalls(GinkgoT(), "Get", 1)
		})
	})

	Describe("pollFile()", func() {
		It("should align when remainingIndex is not zero", func() {
			const bodyLength = 700
			s := newTestReader()
			file, err := os.CreateTemp("", "segment_file_poll_file*.dlog")
			Expect(err).NotTo(HaveOccurred())
			chunk := createAlignedChunk(bodyLength, 0, 20)
			Expect(file.Write(chunk)).NotTo(BeZero())
			Expect(file.Sync()).NotTo(HaveOccurred())
			file.Close()

			s.segmentFile, err = os.OpenFile(file.Name(), conf.SegmentFileReadFlags, 0)
			defer s.segmentFile.Close()

			buf := makeAlignedBuffer(alignmentSize * 8)
			const remainingIndex = 5
			// Fill with data until remainingIndex
			for i := 0; i < remainingIndex; i++ {
				buf[i] = 0xf0
			}
			result, err := s.pollFile(buf, remainingIndex)
			Expect(err).NotTo(HaveOccurred())
			Expect(result[0]).To(Equal(byte(0xf0)))
			Expect(result[:remainingIndex]).To(Equal(buf[:remainingIndex]))
			Expect(result[remainingIndex:]).To(Equal(chunk))
		})
	})
})

type testReadItem struct {
	chunkResult chan SegmentChunk
	errorResult chan error
	origin      uuid.UUID
}

func newTestReadItem() *testReadItem {
	return &testReadItem{
		chunkResult: make(chan SegmentChunk),
		errorResult: make(chan error),
	}
}

func newTestReadItemWithOrigin(origin uuid.UUID) *testReadItem {
	item := newTestReadItem()
	item.origin = origin
	return item
}

func (r *testReadItem) SetResult(err error, chunk SegmentChunk) {
	r.chunkResult <- chunk
	r.errorResult <- err
}

func (r *testReadItem) Origin() uuid.UUID {
	return r.origin
}

func (r *testReadItem) CommitOnly() bool {
	return false
}

type rrFake struct {
	streamBuf    []byte
	streamCalled int64
}

func (f *rrFake) MergeFileStructure() (bool, error) {
	return true, nil
}

func (f *rrFake) StreamFile(segmentId int64, topic *TopicDataId, startOffset int64, maxRecords int, buf []byte) (int, error) {
	atomic.AddInt64(&f.streamCalled, 1)
	return copy(buf, f.streamBuf), nil
}

func createTestChunk(bodyLength, start, recordLength int) []byte {
	header := chunkHeader{
		Flags:        0,
		BodyLength:   uint32(bodyLength),
		Start:        int64(start),
		RecordLength: uint32(recordLength),
		Crc:          0,
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, conf.Endianness, header.Flags)
	binary.Write(buffer, conf.Endianness, header.BodyLength)
	binary.Write(buffer, conf.Endianness, header.Start)
	binary.Write(buffer, conf.Endianness, header.RecordLength)
	header.Crc = crc32.ChecksumIEEE(buffer.Bytes())
	binary.Write(buffer, conf.Endianness, header.Crc)

	body := make([]byte, bodyLength)
	for i := 0; i < bodyLength; i++ {
		body[i] = byte(i)
	}
	buffer.Write(body)

	return buffer.Bytes()
}

func createAlignedChunk(bodyLength, start, recordLength int) []byte {
	chunk := createTestChunk(bodyLength, start, recordLength)
	rem := len(chunk) % alignmentSize

	if rem == 0 {
		return chunk
	}

	totalSize := len(chunk) + alignmentSize - rem
	result := makeAlignedBuffer(totalSize)
	n := copy(result, chunk)

	for i := n; i < totalSize; i++ {
		result[i] = alignmentFlag
	}
	return result
}

// Polls multiple times, skiping empty chunks
func pollChunk(s *SegmentReader, item *testReadItem, bodyLength int) {
	Expect(bodyLength).To(BeNumerically(">", 0), "bodyLength must be greater than 0")
	var chunk SegmentChunk
	for i := 0; i < 10; i++ {
		s.Items <- item
		chunk = <-item.chunkResult
		err := <-item.errorResult

		Expect(err).NotTo(HaveOccurred())
		Expect(chunk).ToNot(BeNil())
		if len(chunk.DataBlock()) > 0 {
			break
		}
	}

	Expect(chunk.DataBlock()).To(HaveLen(bodyLength))
	expectedBody := make([]byte, bodyLength)
	for i := 0; i < bodyLength; i++ {
		expectedBody[i] = byte(i)
	}

	Expect(chunk.DataBlock()).To(Equal(expectedBody))
}

func pollChunkOnce(s *SegmentReader, item *testReadItem, bodyLength int) {
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

func newTestReader() *SegmentReader {
	config := new(mocks.Config)
	config.On("ReadAheadSize").Return(1 * conf.MiB)
	config.On("AutoCommitInterval").Return(1 * time.Second)
	offsetState := new(tMocks.OffsetState)
	offsetState.On("Set",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	return &SegmentReader{
		config:      config,
		Items:       make(chan ReadItem, 16),
		offsetState: offsetState,
		headerBuf:   make([]byte, chunkHeaderSize),
		isLeader:    true,
	}
}
