package types

import (
	"fmt"
	"math"
	"sort"
)

const startToken Token = math.MinInt64

// Represents a partition token
type Token int64

// Represents an index in the token range
type RangeIndex uint8

// Represents slices of the token range between two tokens
type TokenRanges struct {
	Token   Token
	Indices []RangeIndex
}

const maxRingSize = 12288 // 3*math.Pow(2, 12)
const chunkSizeUnit = math.MaxUint64 / maxRingSize

func (t Token) String() string {
	return fmt.Sprintf("%d", t)
}

func (t RangeIndex) String() string {
	return fmt.Sprintf("%d", t)
}

// Gets a token based on a murmur3 hash
func HashToken(key string) Token {
	return Token(Murmur3H1([]byte(key)))
}

// GetPrimaryTokenIndex returns the broker index of the start token in a given range
func GetPrimaryTokenIndex(token Token, tokenRangeLength int, ranges int) (BrokerIndex, RangeIndex) {
	i := sort.Search(tokenRangeLength, func(i int) bool {
		return GetTokenAtIndex(tokenRangeLength, i) > token
	})

	index := i - 1
	rangeSize := chunkSizeUnit * getRingFactor(tokenRangeLength) / int64(ranges)
	tokenDiff := absInt64(token - GetTokenAtIndex(tokenRangeLength, index))
	rangeIndex := RangeIndex(tokenDiff / rangeSize)

	if int(rangeIndex) >= ranges {
		// The last range is larger than chunkSize*factor
		rangeIndex = 0
	}
	return BrokerIndex(index), rangeIndex
}

func GetTokenAtIndex(length int, index int) Token {
	// Wrap around
	index = index % length
	return startToken + Token(chunkSizeUnit*getRingFactor(length)*int64(index))
}

func getRingFactor(ringSize int) int64 {
	return int64(maxRingSize / ringSize)
}

func absInt64(num Token) int64 {
	if num < 0 {
		return -int64(num)
	}
	return int64(num)
}
