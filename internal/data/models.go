package data

import (
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
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
	Flags        byte
	BodyLength   uint32 // The amount of bytes of the body
	Start        int64  // The offset of the first message
	RecordLength uint32 // The amount of messages contained in the chunk
	Crc          int32
}

var chunkHeaderSize = utils.BinarySize(chunkHeader{})

// Represents a queued message to read from a segment.
// When the read is completed, `SetResult()` is invoked.
type ReadItem interface {
	SetResult(error, SegmentChunk)
}

// Represents a queued message to write to the index file.
type indexFileItem struct {
	segmentId  int64
	offset     int64 // The message offset
	fileOffset int64
	toClose    bool
	tailOffset int64
}

type ReadSegmentChunk struct {
	buffer []byte
	start  int64  // The offset of the first message
	length uint32 // The amount of messages in the chunk
}

func NewEmptyChunk(start int64) SegmentChunk {
	return &ReadSegmentChunk{
		buffer: emptyBuffer,
		start:  start,
		length: 0,
	}
}

func (s *ReadSegmentChunk) DataBlock() []byte {
	return s.buffer
}

func (s *ReadSegmentChunk) StartOffset() int64 {
	return s.start
}

func (s *ReadSegmentChunk) RecordLength() uint32 {
	return s.length
}
