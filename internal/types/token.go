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

// Gets a token based on a murmur3 hash
func HashToken(key string) Token {
	return Token(Murmur3H1([]byte(key)))
}

// GetPrimaryTokenIndex returns the broker index of the start token in a given range
func GetPrimaryTokenIndex(token Token, tokenRangeLength int) BrokerIndex {
	i := sort.Search(tokenRangeLength, func(i int) bool {
		return GetTokenAtIndex(tokenRangeLength, i) > token
	})

	return BrokerIndex(i - 1)
}

func GetTokenAtIndex(length int, index int) Token {
	return startToken + Token(chunkSizeUnit*getRingFactor(length)*int64(index))
}

func getRingFactor(ringSize int) int64 {
	return int64(maxRingSize / ringSize)
}
