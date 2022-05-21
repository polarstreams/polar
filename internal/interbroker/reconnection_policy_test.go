package interbroker

import (
	"math"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("reconnectionPolicy", func() {
	It("should return an exponential delay with a plateau", func() {
		p := newReconnectionPolicy()
		for i := 0; i < 8; i++ {
			expected := time.Duration(math.Pow(2, float64(i+1))) * baseReconnectionDelayMs * time.Millisecond
			Expect(p.next()).To(Equal(expected), "For value %d", i)
		}
		for i := 0; i < 60; i++ {
			expected := time.Duration(reconnectionPlateauMs) * time.Millisecond
			Expect(p.next()).To(Equal(expected), "For plateau value %d", i)
		}
		for i := 9; i < 11; i++ {
			expected := time.Duration(math.Pow(2, float64(i))) * baseReconnectionDelayMs * time.Millisecond
			Expect(p.next()).To(Equal(expected), "For after plateau value %d", i)
		}

		for i := 0; i < 50; i++ {
			expected := time.Duration(maxReconnectionDelayMs) * time.Millisecond
			Expect(p.next()).To(Equal(expected), "For max value %d", i)
		}
	})
})
