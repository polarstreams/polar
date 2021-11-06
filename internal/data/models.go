package data

import (
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
)

var emptyBuffer = make([]byte, 0)

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

type chunkHeader struct {
	// Strict ordering, exported fields
	BodyLength   uint32
	Start        uint64 // The start offset
	RecordLength uint32 // The amount of messages contained in the chunk
	Crc          int32
}

var chunkHeaderSize = utils.BinarySize(chunkHeader{})

// Represents a queued message to read from a segment.
// When the read is completed, `SetResult()` is invoked.
type ReadItem interface {
	SetResult(error, SegmentChunk)
}

type ReadSegmentChunk struct {
	buffer []byte
	start  uint64
	length uint32
}

func NewEmptyChunk(start uint64) SegmentChunk {
	return &ReadSegmentChunk{
		buffer: emptyBuffer,
		start:  start,
		length: 0,
	}
}

func (s *ReadSegmentChunk) DataBlock() []byte {
	return s.buffer
}

func (s *ReadSegmentChunk) StartOffset() uint64 {
	return s.start
}

func (s *ReadSegmentChunk) RecordLength() uint32 {
	return s.length
}
