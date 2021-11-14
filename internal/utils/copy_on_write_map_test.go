package utils

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Utils Suite")
}

var _ = Describe("CopyOnWriteMap()", func() {
	It("should support concurrent use", func() {
		var wg sync.WaitGroup
		m := NewCopyOnWriteMap()
		value1 := "value 1"
		value2 := "value 2"
		var key1Counter int32
		var key2Counter int32

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				v, loaded, err := m.LoadOrStore("a", func() (interface{}, error) { return value1, nil })
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(Equal(value1))

				if !loaded {
					atomic.AddInt32(&key1Counter, 1)
				}

				v, loaded, err = m.LoadOrStore("b", func() (interface{}, error) { return value2, nil })
				Expect(err).NotTo(HaveOccurred())
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

	It("should allow creation errors", func() {
		var wg sync.WaitGroup
		m := NewCopyOnWriteMap()
		value2 := "value 2"
		var key2Counter int32

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				v, loaded, err := m.LoadOrStore("a", func() (interface{}, error) { return nil, errors.New("test error") })
				Expect(err).To(HaveOccurred())

				v, loaded, err = m.LoadOrStore("b", func() (interface{}, error) { return value2, nil })
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(Equal(value2))

				if !loaded {
					atomic.AddInt32(&key2Counter, 1)
				}

				wg.Done()
			}()
		}

		wg.Wait()

		Expect(atomic.LoadInt32(&key2Counter)).To(Equal(int32(1)))

		// Successful store
		v, loaded, err := m.LoadOrStore("a", func() (interface{}, error) { return "a value", nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("a value"))
		Expect(loaded).To(Equal(false))
	})
})
