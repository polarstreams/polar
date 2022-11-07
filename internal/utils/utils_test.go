package utils

import (
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
})
