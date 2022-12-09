package data

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/test/conf/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("datalog", func() {
	Describe("alignBuffer()", func() {
		It("should have the address aligned", func() {
			buf := make([]byte, 10000)
			for i := 0; i < alignmentSize+2; i++ {
				result, offset := alignBuffer(buf[i:])
				Expect(addressAlignment(result)).To(Equal(0))
				Expect(offset).To(BeNumerically("<", alignmentSize))
			}
		})

		It("should have the length aligned", func() {
			buf := makeAlignedBuffer(alignmentSize * 4)
			result, offset := alignBuffer(buf[5:])
			Expect(len(result)).To(Equal(alignmentSize * 3))

			result, offset = alignBuffer(buf)
			Expect(len(buf)).To(Equal(alignmentSize * 4))
			Expect(offset).To(BeZero())

			result, _ = alignBuffer(buf[alignmentSize:])
			Expect(len(result)).To(Equal(alignmentSize * 3))

			result, _ = alignBuffer(buf[alignmentSize+1:])
			Expect(len(result)).To(Equal(alignmentSize * 2))
		})
	})

	Describe("readChunksUntil()", func() {
		It("should return a slice with chunk", func() {
			const bodySize = 400
			chunk := createAlignedChunk(bodySize, 100, 50)
			obtainedBuf, complete, err := readChunksUntil(chunk, 100, 20)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(obtainedBuf).To(Equal(chunk[:chunkHeaderSize+bodySize]))
		})

		It("should return a slice with multiple chunks", func() {
			const bodySize1 = 480
			const bodySize2 = 200
			chunk1 := createAlignedChunk(bodySize1, 100, 50)
			chunks := append(chunk1, createTestChunk(bodySize2, 150, 50)...)
			obtainedBuf, complete, err := readChunksUntil(chunks, 100, 300)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(obtainedBuf).To(Equal(chunks[:len(chunk1)+chunkHeaderSize+bodySize2]))
		})

		It("should return a slice with the chunk matching the start-end range", func() {
			const bodySize1 = 480
			const bodySize2 = 200
			chunk1 := createAlignedChunk(bodySize1, 100, 50)
			chunk2 := createTestChunk(bodySize2, 150, 50)
			chunks := append(chunk1, chunk2...)
			obtainedBuf, complete, err := readChunksUntil(chunks, 150, 300)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(obtainedBuf).To(Equal(chunk2))
		})

		It("should return not completed when header is not contained", func() {
			chunk2 := createTestChunk(200, 100, 50)
			chunks := append(createTestChunk(200, 0, 100), chunk2[:chunkHeaderSize-2]...)
			obtainedBuf, complete, err := readChunksUntil(chunks, 100, 300)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeFalse())
			Expect(obtainedBuf).To(Equal(chunk2[:chunkHeaderSize-2]))
		})

		It("should return not completed when chunk body is not contained", func() {
			chunk2 := createTestChunk(200, 100, 50)
			chunks := append(createTestChunk(200, 0, 100), chunk2[:chunkHeaderSize+10]...)
			obtainedBuf, complete, err := readChunksUntil(chunks, 100, 300)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeFalse())
			Expect(obtainedBuf).To(Equal(chunk2[:chunkHeaderSize+10]))
		})
	})

	Describe("ReadFileFrom()", func() {
		It("should return a single chunks when contained", func() {
			const segmentId = 0
			chunks := [][]byte{
				createAlignedChunk(400, 0, 100),   // 0 -> 100
				createAlignedChunk(400, 100, 100), // 100 -> 200
				createAlignedChunk(400, 200, 100), // 200 -> 300
			}
			config := createSegmentFileAndConfig(segmentId, chunks)
			d := &datalog{config: config}

			const maxChunkSize = 4096
			buf := makeAlignedBuffer(maxChunkSize)

			// Request 100->120 range
			obtained, err := d.ReadFileFrom(buf, conf.MiB, segmentId, 100, 20, nil)
			Expect(err).NotTo(HaveOccurred())

			expectedChunk := chunks[1] // 100->200
			Expect(obtained).To(
				Equal(expectedChunk[:chunkHeaderSize+400]),
				"It should return the chunk from 100 to 200 without alignment")
		})

		It("should return the chunks until max records when it fit into buffer", func() {
			const segmentId = 0
			chunks := [][]byte{
				createAlignedChunk(400, 0, 100),   // 0 -> 100
				createAlignedChunk(400, 100, 100), // 100 -> 200
				createAlignedChunk(400, 200, 100), // 200 -> 300
				createAlignedChunk(400, 300, 100), // 300 -> 400
			}
			config := createSegmentFileAndConfig(segmentId, chunks)
			d := &datalog{config: config}

			const maxChunkSize = 4096
			buf := makeAlignedBuffer(maxChunkSize)

			// Request 100->400 range
			obtained, err := d.ReadFileFrom(buf, conf.MiB, segmentId, 100, 300, nil)
			Expect(err).NotTo(HaveOccurred())

			expectedChunks := chunks[1]                                                 // 100->200
			expectedChunks = append(expectedChunks, chunks[2]...)                       // 200->300
			expectedChunks = append(expectedChunks, chunks[3][:chunkHeaderSize+400]...) // 300->400 without alignment
			Expect(obtained).To(Equal(expectedChunks))
		})

		It("should return the chunks that fit into buffer", func() {
			const segmentId = 0
			chunks := [][]byte{
				createAlignedChunk(3000, 0, 100),   // 0 -> 100
				createAlignedChunk(400, 100, 100),  // 100 -> 200
				createAlignedChunk(3000, 200, 100), // 200 -> 300
			}
			config := createSegmentFileAndConfig(segmentId, chunks)
			d := &datalog{config: config}

			const maxChunkSize = 4096
			buf := makeAlignedBuffer(maxChunkSize)

			// Request 100->400 range
			obtained, err := d.ReadFileFrom(buf, conf.MiB, segmentId, 100, 300, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(obtained).To(
				Equal(chunks[1][:chunkHeaderSize+400]),
				"Only 100->200 chunk fitted into buffer")
		})

		It("should issue multiple reads when not fitting into memory after skipping", func() {
			const segmentId = 0
			const maxChunkSize = alignmentSize * 8
			const largeChunkLength = maxChunkSize - 1000
			chunks := [][]byte{
				createAlignedChunk(largeChunkLength, 0, 100),   // 0 -> 100
				createAlignedChunk(400, 100, 100),              // 100 -> 200
				createAlignedChunk(largeChunkLength, 200, 100), // 200 -> 300
			}
			config := createSegmentFileAndConfig(segmentId, chunks)
			d := &datalog{config: config}

			buf := makeAlignedBuffer(maxChunkSize)

			// Request 200->400 range
			obtained, err := d.ReadFileFrom(buf, conf.MiB, segmentId, 200, 300, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(obtained).To(
				Equal(chunks[2][:chunkHeaderSize+largeChunkLength]),
				"the last chunk after multiple reads")
		})
	})
})

func createSegmentFileAndConfig(segmentId int64, chunks [][]byte) *mocks.Config {
	dir, err := ioutil.TempDir("", "read_file_from")
	Expect(err).NotTo(HaveOccurred())

	config := new(mocks.Config)
	config.On("DatalogPath", mock.Anything).Return(dir)

	fileName := filepath.Join(dir, conf.SegmentFileName(segmentId))
	file, err := os.OpenFile(fileName, conf.SegmentFileWriteFlags, FilePermissions)
	Expect(err).NotTo(HaveOccurred())
	defer file.Close()

	totalLength := 0
	for _, chunk := range chunks {
		totalLength += len(chunk)
	}

	buf := makeAlignedBuffer(totalLength)
	index := 0
	for _, chunk := range chunks {
		index += copy(buf[index:], chunk)
	}

	Expect(file.Write(buf)).NotTo(BeZero())
	Expect(file.Sync()).NotTo(HaveOccurred())

	return config
}
