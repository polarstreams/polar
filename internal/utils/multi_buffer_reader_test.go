package utils

import (
	"encoding/binary"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("multiBufferReader()", func() {
	const totalLength = 40
	buffers := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		buf := make([]byte, 4)
		buffers = append(buffers, buf)
		for j := 0; j < 4; j++ {
			buf[j] = byte(i*4 + j)
		}
	}

	Describe("getBuffer()", func() {
		It("should get the buffer of length 1", func() {
			r := &multiBufferReader{
				buffers: buffers,
			}

			for i := 0; i < totalLength; i++ {
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

			// From that point on, it's EOF
			for i := 0; i < 5; i++ {
				buf, err := r.getBuffer(4)
				Expect(err).To(Equal(io.EOF))
				Expect(buf).To(Equal([]byte{}))
			}
		})
	})

	Describe("Read()", func() {
		It("should read to a buffer of length 1", func() {
			r := &multiBufferReader{
				buffers: buffers,
			}

			for i := 0; i < totalLength; i++ {
				buf := make([]byte, 1)
				n, err := r.Read(buf)
				Expect(err).NotTo(HaveOccurred())
				Expect(buf[0]).To(Equal(byte(i)))
				Expect(n).To(Equal(1))
			}

			for i := 0; i < 5; i++ {
				n, err := r.Read(make([]byte, 1))
				Expect(n).To(BeZero())
				Expect(err).To(Equal(io.EOF))
			}
		})

		It("should get buffers of uneven length", func() {
			r := &multiBufferReader{
				buffers: buffers,
			}

			startIndex := 0
			for i := 0; i < 12; i++ {
				buf := make([]byte, 3)
				n, err := r.Read(buf)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(BeNumerically("<=", len(buf)))
				expected := make([]byte, n)
				for i := 0; i < n; i++ {
					expected[i] = byte(startIndex + i)
				}
				Expect(buf[:n]).To(Equal(expected))
				startIndex += n
			}
		})

		It("should get buffers of even length", func() {
			r := &multiBufferReader{
				buffers: buffers,
			}

			for i := 0; i < 10; i++ {
				buf := make([]byte, 4)
				n, err := r.Read(buf)
				Expect(err).NotTo(HaveOccurred())
				startIndex := byte(i * 4)
				Expect(n).To(Equal(4))
				Expect(buf).To(Equal([]byte{startIndex, startIndex + 1, startIndex + 2, startIndex + 3}))
			}

			// From that point on, it's EOF
			for i := 0; i < 5; i++ {
				n, err := r.Read(make([]byte, 1))
				Expect(n).To(BeZero())
				Expect(err).To(Equal(io.EOF))
			}
		})
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

	Describe("Bytes()", func() {
		Context("with bufferIndex 0", func() {
			It("should return the unread portion", func() {
				r := &multiBufferReader{
					buffers: buffers,
					index:   3,
				}
				result, length := r.Bytes()
				Expect(result).To(HaveLen(len(buffers)))
				Expect(length).To(Equal(totalLength - 3))
				Expect(result[0]).To(Equal([]byte{3}))
				Expect(result[1:]).To(Equal(buffers[1:]))
			})
		})

		Context("with bufferIndex 1", func() {
			It("should return the unread portion", func() {
				r := &multiBufferReader{
					buffers: buffers,
				}
				_, _ = r.getBuffer(4)
				result, length := r.Bytes()
				Expect(length).To(Equal(totalLength - 4))
				Expect(result).To(HaveLen(len(buffers) - 1))
				Expect(result).To(Equal(buffers[1:]))
			})
		})

		Context("with bufferIndex last", func() {
			It("should return the unread portion", func() {
				r := &multiBufferReader{
					buffers: buffers,
				}
				_, _ = r.getBuffer(39)
				result, length := r.Bytes()
				Expect(length).To(Equal(totalLength - 39))
				Expect(result).To(Equal([][]byte{{39}}))
			})

			It("should return an empty buffer", func() {
				r := &multiBufferReader{
					buffers: buffers,
				}
				_, _ = r.getBuffer(40)
				result, length := r.Bytes()
				Expect(length).To(Equal(0))
				Expect(result).To(Equal([][]byte{}))
			})
		})
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
