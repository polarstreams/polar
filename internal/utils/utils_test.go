package utils

import (
	"fmt"
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
				fmt.Println("--v", v)
				if v != lastValue {
					allEqual = false
				}
				Expect(v).To(BeNumerically(">", 0))
				Expect(v).To(BeNumerically("~", 9500*time.Millisecond, 10500*time.Millisecond))
			}
			Expect(allEqual).To(BeFalse())
		})
	})
})
