package consuming

import . "github.com/jorgebay/soda/internal/types"

// TODO: Rename to something like ConsumerGroupQueries and split some of them into "state"
type ConsumerGroupState interface {
	// Determines whether the consumer group can be served with token data.
	// It navigates through the generation tree, looking for parents.
	//
	// Only called one per consumer group reader.
	CanConsumeToken(group string, token Token, version GenVersion) bool

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
