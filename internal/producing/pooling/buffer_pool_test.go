package pooling

import (
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pooling Suite")
}

var _ = Describe("bufferPool", func() {
	It("should get a single buffer", func() {
		pool := NewBufferPool(16 * baseSize)
		values := pool.Get(baseSize * 5)
		Expect(len(values)).To(Equal(5))
		Expect(values[0]).To(HaveLen(baseSize))

		Expect(len(pool.Get(baseSize*2 + 5))).To(Equal(3))
	})

	It("should get in parallel", func() {
		const expectedSize = baseSize * 11
		pool := NewBufferPool(16 * baseSize)
		c := make(chan [][]byte, 100)
		go func() {
			values := pool.Get(baseSize * 5)
			c <- values
		}()
		go func() {
			values := pool.Get(baseSize * 3)
			c <- values
		}()
		go func() {
			values := pool.Get(baseSize*2 + 10)
			c <- values
		}()

		obtainedSize := 0
		for i := 0; i < 3; i++ {
			buffers := <-c
			for _, b := range buffers {
				obtainedSize += len(b)

				// Make sure we are not getting the same address space
				Expect(b).NotTo(ContainElement(byte(1)))
				fillBytes(b, 1)
			}
		}

		Expect(obtainedSize).To(Equal(expectedSize))
	})

	It("should block when no more space is available", func() {
		var freeInvoked atomic.Int32
		pool := NewBufferPool(baseSize * 16)
		value1 := pool.Get(baseSize * 10)
		value2 := pool.Get(baseSize * 4)
		go func() {
			time.Sleep(10 * time.Millisecond)
			freeInvoked.Add(1)
			pool.Free(value1)
		}()

		go func() {
			time.Sleep(10 * time.Millisecond)
			freeInvoked.Add(1)
			pool.Free(value2)
		}()

		// This call should block
		result := pool.Get(baseSize * 15)
		Expect(result).To(HaveLen(15))
		Expect(freeInvoked.Load()).To(Equal(int32(2)))
	})
})

func fillBytes(b []byte, value byte) {
	for i := 0; i < len(b); i++ {
		b[i] = value
	}
}
