package data

import . "github.com/jorgebay/soda/internal/types"

type LocalWriteItem interface {
	SegmentChunk
	Replication() ReplicationInfo
	SetResult(error)
}

type ReplicationDataItem interface {
	SegmentChunk
	SegmentId() int64
	SetResult(error)
}

// Represents a queued message to read from a segment.
// When the read is completed, `SetResult()` is invoked.
type ReadItem interface {
	ShouldClose() bool

	SetResult(error, SegmentChunk)
}
