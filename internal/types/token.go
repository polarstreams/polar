package types

import (
	"math"
	"sort"
)

type Token int64

const startToken = math.MinInt64

// getPrimaryToken gets the start of the token range for a given token
func getPrimaryToken(token Token, tokenRange []Token) Token {
	i := sort.Search(len(tokenRange), func(i int) bool {
		return tokenRange[i] > token
	})

	return tokenRange[i-1]
}

func getTokenAtIndex(length int, index int) Token {
	size := int64(math.MaxUint64 / uint64(length))
	return Token(startToken + size*int64(index))
}
