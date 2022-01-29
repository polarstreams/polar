package consuming

import . "github.com/jorgebay/soda/internal/types"

type ConsumerGroupQueries interface {
	// Locally stores the offset of a given group.
	SetOffset(group string, token Token, offset Offset)

	// Load local offsets from the persistent volume
	LoadLocalOffsets(group string, token Token)

	// When serving data as follower, it queries the following broker
	// Whether there's more data to serve. It returns true when
	// the peer finds more data.
	//
	// An error should cause the server to ignore the poll
	IsMaxOffset(group string, token Token, offset Offset) (bool, error)
}
