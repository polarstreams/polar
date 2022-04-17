package data

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("datalog", func() {
	Describe("alignBuffer()", func() {
		It("return the buffer aligned", func() {
			buf := make([]byte, alignmentSize*4)
			for i := 0; i < len(buf); i++ {
				buf[i] = byte(i)
			}

			Expect(alignBuffer(buf[:alignmentSize+3])).To(Equal(buf[:alignmentSize]))
			Expect(alignBuffer(buf[:alignmentSize*2-1])).To(Equal(buf[:alignmentSize]))
			Expect(alignBuffer(buf[:alignmentSize*2+1])).To(Equal(buf[:alignmentSize*2]))
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

		// TODO: Implement
		XIt("should return not completed when header is not contained")

		// TODO: Implement
		XIt("should return not completed when chunk body is not contained")
	})
})
