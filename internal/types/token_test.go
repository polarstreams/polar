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

var _ = Describe("Token", func() {
	Describe("GetTokenAtIndex()", func() {
		It("should start from min int64", func() {
			Expect(GetTokenAtIndex(6, 0)).To(Equal(Token(math.MinInt64)))
		})

		It("should wrap around", func() {
			Expect(GetTokenAtIndex(6, 6)).To(Equal(Token(math.MinInt64)))
			Expect(GetTokenAtIndex(6, 7)).To(Equal(GetTokenAtIndex(6, 1)))
		})

		It("should cover all the ring and not have major differences", func() {
			for factor := 0; factor < 12; factor++ {
				nTokens := int(3 * math.Pow(2, float64(factor)))
				previous := Token(StartToken)
				diff := -1 * (StartToken - GetTokenAtIndex(nTokens, 1))

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

	Describe("GetPrimaryTokenIndex()", func() {
		It("Should calculate the RangeIndex", func() {
			brokerIndex, rangeIndex := GetPrimaryTokenIndex(StartToken, 6, 8)
			Expect(brokerIndex).To(Equal(BrokerIndex(0)))
			Expect(rangeIndex).To(Equal(RangeIndex(0)))

			brokerIndex, rangeIndex = GetPrimaryTokenIndex(Token(math.MaxInt64), 6, 8)
			Expect(brokerIndex).To(Equal(BrokerIndex(5)))
			Expect(rangeIndex).To(Equal(RangeIndex(0)))

			brokerIndex, rangeIndex = GetPrimaryTokenIndex(Token(math.MaxInt64)-10000, 6, 8)
			Expect(brokerIndex).To(Equal(BrokerIndex(5)))
			Expect(rangeIndex).To(Equal(RangeIndex(7)))

			brokerIndex, rangeIndex = GetPrimaryTokenIndex(Token(math.MaxInt64)-999999999999999999, 6, 8)
			Expect(brokerIndex).To(Equal(BrokerIndex(5)))
			Expect(rangeIndex).To(Equal(RangeIndex(5)))

			brokerIndex, rangeIndex = GetPrimaryTokenIndex(StartToken+Token(chunkSizeUnit*getRingFactor(6)/2), 6, 8)
			Expect(brokerIndex).To(Equal(BrokerIndex(0)))
			Expect(rangeIndex).To(Equal(RangeIndex(4)))
		})
	})

	Describe("HashToken()", func() {
		It("Should return the expected values", func() {
			// Taken from Cassandra token() function
			values := []testTokenHash{
				{"abcd", -5153323217664422577},
				{"wxyz", 6541399000449243469},
				{"mmmm", 1406774400723249678},
				{"Hashing a value", 3002999691413861203},
				{"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.", 3329387957440342626},
			}

			for _, item := range values {
				Expect(HashToken(item.text)).To(Equal(Token(item.token)))
			}
		})
	})
})

type testTokenHash struct {
	text  string
	token int64
}
