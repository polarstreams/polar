package utils

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils", func() {
	Describe("Jitter", func() {
		It("should be greater than 95%% and below 105%% of the value", func() {
			allEqual := true
			d := 10000 * time.Millisecond
			lastValue := Jitter(d)
			for i := 0; i < 15; i++ {
				v := Jitter(d)
				if v != lastValue {
					allEqual = false
				}
				Expect(v).To(BeNumerically(">", 0))
				Expect(v).To(BeNumerically("~", 9500*time.Millisecond, 10500*time.Millisecond))
			}
			Expect(allEqual).To(BeFalse())
		})
	})

	Describe("ValidRingLength()", func() {
		It("should return the last ring that can contain it", func() {
			values := [][]int{
				{1, 1},
				{2, 3},
				{4, 3},
				{6, 6},
				{7, 6},
				{11, 6},
				{12, 12},
			}

			for _, v := range values {
				Expect(ValidRingLength(v[0])).To(Equal(v[1]), "Doesn't match for %v", v)
			}
		})
	})

	Describe("ReadIntoBuffers()", func() {
		It("should read into the first buffer", func() {
			buffers := [][]byte{
				make([]byte, 8),
				make([]byte, 8),
			}

			value := "something"
			err := ReadIntoBuffers(strings.NewReader(value), buffers, 4)
			Expect(err).NotTo(HaveOccurred())
			Expect(buffers[0][:4]).To(Equal([]byte("some")))
			Expect(buffers[0][4:]).To(Equal(make([]byte, 4)))
			Expect(buffers[1]).To(Equal(make([]byte, 8)))
		})

		It("should read into the second buffer", func() {
			buffers := [][]byte{
				make([]byte, 8),
				make([]byte, 8),
			}

			value := "something"
			err := ReadIntoBuffers(strings.NewReader(value), buffers, 9)
			Expect(err).NotTo(HaveOccurred())
			Expect(buffers[0]).To(Equal([]byte("somethin")))
			Expect(buffers[1][:1]).To(Equal([]byte("g")))
			Expect(buffers[1][1:]).To(Equal(make([]byte, 7)))
		})

		It("should read into the exact length", func() {
			buffers := [][]byte{
				make([]byte, 8),
				make([]byte, 8),
			}

			value := "hello something!"
			err := ReadIntoBuffers(strings.NewReader(value), buffers, 16)
			Expect(err).NotTo(HaveOccurred())
			Expect(buffers[0]).To(Equal([]byte("hello so")))
			Expect(buffers[1]).To(Equal([]byte("mething!")))
		})
	})
})
