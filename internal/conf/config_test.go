package conf

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFlowControl(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Controller Suite")
}

var _ = Describe("parseHostName()", func() {
	It("should return default when provided is empty", func ()  {
		base, ordinal := parseHostName("")
		Expect(base).To(Equal("soda-"))
		Expect(ordinal).To(Equal(0))
	})

	It("should parse the value according to StatefulSet name convention", func ()  {
		base, ordinal := parseHostName("abc-2")
		Expect(base).To(Equal("abc-"))
		Expect(ordinal).To(Equal(2))

		// Left most
		base, ordinal = parseHostName("abc-12.def-34")
		Expect(base).To(Equal("abc-"))
		Expect(ordinal).To(Equal(12))

		base, ordinal = parseHostName("instance-1.soda")
		Expect(base).To(Equal("instance-"))
		Expect(ordinal).To(Equal(1))
	})
})