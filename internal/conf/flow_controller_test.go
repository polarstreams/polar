package conf

import (
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("flowControl", func() {
	Describe("allocate()", func() {
		It("should decrease the remaining length", func() {
			f := newFlowControl(100)
			f.Allocate(10)
			Expect(f.remaining).To(Equal(90))
			f.Allocate(20)
			Expect(f.remaining).To(Equal(70))
			f.Free(20)
			f.Allocate(5)
			Expect(f.remaining).To(Equal(85))
		})

		It("should block until there's enough space", func() {
			f := newFlowControl(100)
			f.Allocate(40)
			f.Allocate(35)
			Expect(f.remaining).To(Equal(25))
			var wasFreedInvoked int32 = 0

			go func() {
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt32(&wasFreedInvoked, 1)
				f.Free(40)
			}()

			go func() {
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt32(&wasFreedInvoked, 1)
				f.Free(35)
			}()

			// This call should block
			f.Allocate(90)
			Expect(atomic.LoadInt32(&wasFreedInvoked)).To(Equal(int32(2)))
			Expect(f.remaining).To(Equal(10))
		})
	})
})
