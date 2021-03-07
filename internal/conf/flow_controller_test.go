package conf

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFlowControl(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Flow Controller Suite")
}

var _ = Describe("flowControl", func() {
	Describe("allocate()", func() {
		It("should decrease the remaining length", func() {
			f := newFlowControl(100)
			f.allocate(10)
			Expect(f.remaining).To(Equal(90))
			f.allocate(20)
			Expect(f.remaining).To(Equal(70))
			f.free(20)
			f.allocate(5)
			Expect(f.remaining).To(Equal(85))
		})

		XIt("should block until there's enough space")
	})
})
