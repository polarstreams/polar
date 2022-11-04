package types

import (
	"fmt"
	"math"
	"sort"

	"github.com/rs/zerolog/log"
)

const StartToken Token = math.MinInt64

// Represents a partition token
type Token int64

// Represents an index in the token range
type RangeIndex uint8

// Represents slices of the token range between two tokens
type TokenRanges struct {
	Token       Token
	ClusterSize int
	Indices     []RangeIndex
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
func GetPrimaryTokenIndex(token Token, clusterSize int, rangesPerToken int) (BrokerIndex, RangeIndex) {
	i := sort.Search(clusterSize, func(i int) bool {
		return GetTokenAtIndex(clusterSize, i) > token
	})

	index := i - 1
	// Expressing the range size as a factor ONLY works if the rangesPerToken never change
	// during the lifetime of a topic
	rangeSize := chunkSizeUnit * getRingFactor(clusterSize) / int64(rangesPerToken)
	tokenDiff := absInt64(token - GetTokenAtIndex(clusterSize, index))
	rangeIndex := RangeIndex(tokenDiff / rangeSize)

	if int(rangeIndex) >= rangesPerToken {
		// The last range is larger than chunkSize*factor
		rangeIndex = 0
	}
	return BrokerIndex(index), rangeIndex
}

func GetTokenAtIndex(length int, index int) Token {
	// Wrap around
	index = index % length
	return StartToken + Token(chunkSizeUnit*getRingFactor(length)*int64(index))
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

// Gets the start and end of a range based on the token, range index and cluster size.
// Note that for the last range, the end will be `MaxInt64`.
func RangeByTokenAndClusterSize(token Token, index RangeIndex, rangesPerToken int, clusterSize int) (Token, Token) {
	// Expressing the range size as a factor ONLY works if the rangesPerToken never change
	// during the lifetime of a topic
	rangeSize := Token(chunkSizeUnit * getRingFactor(clusterSize) / int64(rangesPerToken))
	start := token + rangeSize*Token(index)
	end := Token(0)
	if index < RangeIndex(rangesPerToken)-1 {
		end = token + rangeSize*Token(index+1)
	} else {
		// The end should be the next token that will cover any mod difference
		i := sort.Search(clusterSize, func(i int) bool {
			return GetTokenAtIndex(clusterSize, i) >= token
		})

		if i == clusterSize {
			log.Panic().Msgf("Invalid token %d when mapping to ranges", token)
		}
		end = GetTokenAtIndex(clusterSize, i+1)
		if end == StartToken {
			end = Token(math.MaxInt64)
		}
	}
	return start, end
}
