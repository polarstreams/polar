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

// Represents a point-in-time set of ranges
type GenerationRanges struct {
	Generation *Generation
	Indices    []RangeIndex
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
	rangeSize := Token(chunkSizeUnit * (getRingFactor(clusterSize) / int64(rangesPerToken)))

	if clusterSize == 1 {
		// Calculations overflow with a single-broker cluster
		return rangeByTokenAndClusterSizeDevMode(index, rangesPerToken, clusterSize)
	}

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

func rangeByTokenAndClusterSizeDevMode(index RangeIndex, rangesPerToken int, clusterSize int) (Token, Token) {
	// Dev mode clusters can not be graduated to a multi broker cluster, so we are safe
	if rangesPerToken == 1 {
		return StartToken, Token(math.MaxInt64)
	}
	rangeSizeForSize2 := float64(math.MaxInt64)
	rangeSize := rangeSizeForSize2 * (2 / float64(rangesPerToken))
	start := StartToken + Token(rangeSize)*Token(index)
	end := Token(math.MaxInt64)
	if index < RangeIndex(rangesPerToken)-1 {
		end = StartToken + Token(rangeSize)*Token(index+1)
	}
	return start, end
}

func ProjectRangeByClusterSize(
	token Token,
	index RangeIndex,
	rangesPerToken int,
	clusterSize int,
	newClusterSize int,
) []*TokenRanges {
	start, end := RangeByTokenAndClusterSize(token, index, rangesPerToken, clusterSize)
	result := make([]*TokenRanges, 0)
	var item *TokenRanges

	for i := 0; i < newClusterSize; i++ {
		newToken := GetTokenAtIndex(newClusterSize, i)
		for newIndex := RangeIndex(0); newIndex < RangeIndex(rangesPerToken); newIndex++ {
			newStart, newEnd := RangeByTokenAndClusterSize(newToken, newIndex, rangesPerToken, newClusterSize)
			if Intersects(start, end, newStart, newEnd) {
				if item == nil || item.Token != newToken {
					item = &TokenRanges{
						Token:       newToken,
						ClusterSize: newClusterSize,
						Indices:     []RangeIndex{newIndex},
					}
					result = append(result, item)
				} else {
					item.Indices = append(item.Indices, newIndex)
				}
			}
		}
	}
	return result
}

func Intersects(startA, endA, startB, endB Token) bool {
	min := endA
	max := startB
	if startA >= startB {
		min = endB
		max = startA
	}

	if min == Token(math.MaxInt64) {
		// Special case for the last token
		return min >= max
	}

	return min > max
}
