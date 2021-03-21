package types

import (
	"math"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTokens(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Token Controller Suite")
}

var _ = Describe("getPrimaryToken()", func() {
	It("should get the lower token", func() {
		tokenRange := []Token{math.MinInt64, -10, 1000000}
		values := map[Token]int{
			-20:           0,
			math.MinInt64: 0,
			123:           1,
			0:             1,
			-10:           1,
			1000000:       2,
			1000002:       2,
			math.MaxInt64: 2,
		}

		for key, value := range values {
			Expect(getPrimaryToken(key, tokenRange)).To(Equal(tokenRange[value]))
		}
	})
})

var _ = Describe("getTokenAtIndex()", func() {
	It("should start from min int64", func() {
		Expect(getTokenAtIndex(6, 0)).To(Equal(Token(math.MinInt64)))
	})

	It("should cover all the ring", func() {
		nTokens := 6;
		rangeLength := int64(math.MaxUint64 / uint64(nTokens))
		for i := 1; i < nTokens; i++ {
			Expect(getTokenAtIndex(nTokens, i)).To(Equal(Token(math.MinInt64 + (rangeLength*int64(i)))))
		}
	})
})
