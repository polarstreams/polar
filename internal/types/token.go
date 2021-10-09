package types

import (
	"fmt"
	"math"
	"sort"
)

const startToken = math.MinInt64

type Token int64

const maxRingSize = 12288 // 3*math.Pow(2, 12)
const chunkSizeUnit = math.MaxUint64 / maxRingSize

func (t Token) String() string {
	return fmt.Sprintf("%d", t)
}

// getPrimaryToken gets the start of the token range for a given token
func getPrimaryToken(token Token, tokenRange []Token) Token {
	return tokenRange[GetPrimaryTokenIndex(token, tokenRange)]
}

func GetToken(key string) Token {
	return Token(Murmur3H1([]byte(key)))
}

// GetPrimaryTokenIndex returns the index of the start token in a given range
func GetPrimaryTokenIndex(token Token, tokenRange []Token) int {
	i := sort.Search(len(tokenRange), func(i int) bool {
		return tokenRange[i] > token
	})

	return i - 1
}

func GetTokenAtIndex(length int, index int) Token {
	return startToken + Token(chunkSizeUnit*getRingFactor(length)*int64(index))
}

func getRingFactor(ringSize int) int64 {
	return int64(maxRingSize / ringSize)
}
