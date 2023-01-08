package utils

import (
	"encoding/binary"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("multiBufferReader()", func() {
	buffers := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		buf := make([]byte, 4)
		buffers = append(buffers, buf)
		for j := 0; j < 4; j++ {
			buf[j] = byte(i*4 + j)
		}
	}

	It("should get the buffer of length 1", func() {
		r := &multiBufferReader{
			buffers: buffers,
		}

		for i := 0; i < 40; i++ {
			buf, err := r.getBuffer(1)
			Expect(err).NotTo(HaveOccurred())
			Expect(buf[0]).To(Equal(byte(i)))
		}
	})

	It("should get buffers of uneven length", func() {
		r := &multiBufferReader{
			buffers: buffers,
		}

		for i := 0; i < 12; i++ {
			buf, err := r.getBuffer(3)
			Expect(err).NotTo(HaveOccurred())
			startIndex := byte(i * 3)
			Expect(buf).To(Equal([]byte{startIndex, startIndex + 1, startIndex + 2}))
		}
	})

	It("should get buffers of even length", func() {
		r := &multiBufferReader{
			buffers: buffers,
		}

		for i := 0; i < 10; i++ {
			buf, err := r.getBuffer(4)
			Expect(err).NotTo(HaveOccurred())
			startIndex := byte(i * 4)
			Expect(buf).To(Equal([]byte{startIndex, startIndex + 1, startIndex + 2, startIndex + 3}))
		}
	})

	It("should read uint32", func() {
		for initialOffset := 0; initialOffset < 4; initialOffset++ {
			testReadUint32(buffers, initialOffset)
		}
	})

	It("should read uint64", func() {
		for initialOffset := 0; initialOffset < 8; initialOffset++ {
			testReadUint64(buffers, initialOffset)
		}
	})
})

func testReadUint32(buffers [][]byte, initialOffset int) {
	r := &multiBufferReader{
		buffers: buffers,
	}

	_, err := r.getBuffer(initialOffset)
	Expect(err).NotTo(HaveOccurred())

	totalReads := 10
	if initialOffset > 0 {
		totalReads--
	}

	for i := 0; i < totalReads; i++ {
		value, err := r.ReadUint32()
		Expect(err).NotTo(HaveOccurred())
		startIndex := byte(initialOffset) + byte(i*4)
		buf := []byte{startIndex, startIndex + 1, startIndex + 2, startIndex + 3}
		Expect(value).To(Equal(binary.BigEndian.Uint32(buf)))
	}
}

func testReadUint64(buffers [][]byte, initialOffset int) {
	r := &multiBufferReader{
		buffers: buffers,
	}

	_, err := r.getBuffer(initialOffset)
	Expect(err).NotTo(HaveOccurred())

	totalReads := 5
	if initialOffset > 0 {
		totalReads--
	}

	for i := 0; i < totalReads; i++ {
		value, err := r.ReadUint64()
		Expect(err).NotTo(HaveOccurred())
		startIndex := byte(initialOffset) + byte(i*8)
		buf := []byte{startIndex, startIndex + 1, startIndex + 2, startIndex + 3, startIndex + 4, startIndex + 5, startIndex + 6, startIndex + 7}
		Expect(value).To(Equal(binary.BigEndian.Uint64(buf)))
	}
}
