package data

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SegmentWriter", func() {
	Describe("alignBuffer()", func() {
		const alignment = 512
		It("should complete the remaining length", func() {
			s := &SegmentWriter{
				buffer: new(bytes.Buffer),
			}

			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(0))

			s.buffer.Write([]byte{0, 0, 0})
			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(alignment))
			Expect(s.buffer.Bytes()[:3]).To(Equal([]byte{0, 0, 0}))

			// Followed by 0xff
			for i := 3; i < alignment; i++ {
				Expect(s.buffer.Bytes()[i]).To(Equal(byte(0xff)))
			}

			// It's aligned, no effect
			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(alignment))

			s.buffer.Write([]byte{1})
			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(alignment * 2))
			Expect(s.buffer.Bytes()[alignment]).To(Equal(byte(1)))

			// Filled with 0xff
			for i := 1; i < alignment; i++ {
				Expect(s.buffer.Bytes()[alignment+i]).To(Equal(byte(0xff)))
			}
		})
	})
})
