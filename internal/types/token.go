package types

import "sort"

type Token int64

// getPrimaryToken gets the start of the token range for a given token
func getPrimaryToken(token Token, tokenRange []Token) Token {
	i := sort.Search(len(tokenRange), func(i int) bool {
		return tokenRange[i] > token
	})

	return tokenRange[i-1]
}
