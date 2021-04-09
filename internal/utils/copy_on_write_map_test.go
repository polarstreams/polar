package utils_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jorgebay/soda/internal/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Map Suite")
}

var _ = Describe("CopyOnWriteMap()", func() {
	It("support concurrent use", func() {
		var wg sync.WaitGroup
		m := utils.NewCopyOnWriteMap()
		value1 := "value 1"
		value2 := "value 2"
		var key1Counter int32
		var key2Counter int32

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				v, loaded := m.LoadOrStore("a", func() interface{} { return value1 })
				Expect(v).To(Equal(value1))

				if !loaded {
					atomic.AddInt32(&key1Counter, 1)
				}

				v, loaded = m.LoadOrStore("b", func() interface{} { return value2 })
				Expect(v).To(Equal(value2))

				if !loaded {
					atomic.AddInt32(&key2Counter, 1)
				}

				wg.Done()
			}()
		}

		wg.Wait()

		Expect(atomic.LoadInt32(&key1Counter)).To(Equal(int32(1)))
		Expect(atomic.LoadInt32(&key2Counter)).To(Equal(int32(1)))
	})
})
