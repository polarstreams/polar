package types

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ordinalsPlacementOrder()", func() {
	It("should return a valid ring for 3", func() {
		Expect(ordinalsPlacementOrder(3)).To(Equal([]uint32{0, 1, 2}))
	})

	It("should return a valid ring for 6", func() {
		Expect(ordinalsPlacementOrder(6)).To(Equal([]uint32{0, 3, 1, 4, 2, 5}))
	})

	It("should return a valid ring for 12", func() {
		Expect(ordinalsPlacementOrder(12)).To(Equal([]uint32{0, 6, 3, 7, 1, 8, 4, 9, 2, 10, 5, 11}))
	})

	It("should return a valid ring for 24", func() {
		Expect(ordinalsPlacementOrder(24)).To(Equal([]uint32{0, 12, 6, 13, 3, 14, 7, 15, 1, 16, 8, 17, 4, 18, 9, 19, 2, 20, 10, 21, 5, 22, 11, 23}))
	})

	It("should return a valid ring for 48", func() {
		Expect(ordinalsPlacementOrder(48)).To(Equal([]uint32{0, 24, 12, 25, 6, 26, 13, 27, 3, 28, 14, 29, 7, 30, 15, 31, 1, 32, 16, 33, 8, 34, 17, 35, 4, 36, 18, 37, 9, 38, 19, 39, 2, 40, 20, 41, 10, 42, 21, 43, 5, 44, 22, 45, 11, 46, 23, 47}))
	})
})
