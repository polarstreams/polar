package types

import (
	"math"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTokens(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Types Suite")
}

var _ = Describe("getTokenAtIndex()", func() {
	It("should start from min int64", func() {
		Expect(GetTokenAtIndex(6, 0)).To(Equal(Token(math.MinInt64)))
	})

	It("should cover all the ring and not have major differences", func() {
		for factor := 0; factor < 12; factor++ {
			nTokens := int(3 * math.Pow(2, float64(factor)))
			previous := Token(startToken)
			diff := -1 * (startToken - GetTokenAtIndex(nTokens, 1))

			// All pieces are the same
			for i := 1; i < nTokens; i++ {
				t := GetTokenAtIndex(nTokens, i)
				Expect(t - previous).To(Equal(diff))
				previous = t
			}

			lastToken := previous
			lastDiff := math.MaxInt64 - lastToken + 1
			percentage := math.Abs(100 - float64(lastDiff)/float64(diff)*100)

			// Except the last token that is a little higher
			// The difference for the last range slice should be lower than 1%
			Expect(percentage).To(BeNumerically("<", 1))

		}

	})

	It("should not move existing tokens", func() {
		nTokens := 3
		for i := 0; i < nTokens; i++ {
			t := GetTokenAtIndex(nTokens, i)
			for j := 0; j < 10; j++ {
				ringBase := int(math.Pow(2, float64(j)))
				ringSize := 3 * ringBase
				index := i * ringBase
				Expect(GetTokenAtIndex(ringSize, index)).To(Equal(t))
			}
		}
	})
})
