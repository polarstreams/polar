package utils

import (
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const timerPrecision = 20 * time.Millisecond

var _ = Describe("Debounce()", func() {
	It("should fire once when debouncing threshold is 0", func() {
		delay := 20 * time.Millisecond
		debouncer := Debounce(delay, 0)
		var value int32 = 0

		elapsed := func() {
			atomic.AddInt32(&value, 1)
		}

		for i := 0; i < 10; i++ {
			debouncer(elapsed)
		}

		time.Sleep(delay + timerPrecision)
		Expect(atomic.LoadInt32(&value)).To(BeEquivalentTo(1))
	})

	It("should fire once when it's within debouncing threshold", func() {
		delay := 100 * time.Millisecond
		debouncer := Debounce(delay, 0.5)
		var value int32 = 0

		elapsed := func() {
			atomic.AddInt32(&value, 1)
		}

		for i := 0; i < 3; i++ {
			debouncer(elapsed)
			time.Sleep(timerPrecision)
		}

		time.Sleep(delay + timerPrecision)
		Expect(atomic.LoadInt32(&value)).To(BeEquivalentTo(1))
	})

	It("should fire multiple times", func() {
		delay := 20 * time.Millisecond
		debouncer := Debounce(delay, 0)
		var value int32 = 0

		elapsed := func() {
			atomic.AddInt32(&value, 1)
		}

		debouncer(elapsed)
		time.Sleep(delay + timerPrecision)
		debouncer(elapsed)
		time.Sleep(delay + timerPrecision)

		Expect(atomic.LoadInt32(&value)).To(BeEquivalentTo(2))
	})

	It("should fire multiple times when it's outside the debouncing threshold", func() {
		delay := 40 * time.Millisecond
		debouncer := Debounce(delay, 0.1)
		var value int32 = 0

		elapsed := func() {
			atomic.AddInt32(&value, 1)
		}

		debouncer(elapsed)
		// Sleep a short amount of time, but more than 10% of 40ms has passed
		time.Sleep(timerPrecision)
		debouncer(elapsed)
		time.Sleep(delay + timerPrecision)

		Expect(atomic.LoadInt32(&value)).To(BeEquivalentTo(2))
	})
})
