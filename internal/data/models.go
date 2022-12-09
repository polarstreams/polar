package data

import (
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
)

// See: https://lwn.net/Articles/12032/
const alignmentSize = 512

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

type ReplicationReader interface {
	MergeFileStructure() (bool, error) // Merge the index files content and file structures

	// Reads at least a chunk from a replica and returns the amount of bytes written in the buffer
	StreamFile(
		segmentId int64,
		topic *TopicDataId,
		startOffset int64,
		maxRecords int,
		buf []byte) (int, error)
}

type chunkHeader struct {
	// Strict ordering, exported fields
	Flags        byte
	BodyLength   uint32 // The amount of bytes of the body
	Start        int64  // The offset of the first message
	RecordLength uint32 // The amount of messages contained in the chunk
	Crc          uint32
}

var chunkHeaderSize = utils.BinarySize(chunkHeader{})

// Represents a queued message to read from a segment.
// When the read is completed, `SetResult()` is invoked.
type ReadItem interface {
	Origin() string   // An identifier of the source of the poll used to determine whether the reader should use the last stored offset and not auto commit
	CommitOnly() bool // Determines whether it should only commit and not read as part of this request
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
	Buffer []byte
	Start  int64  // The offset of the first message
	Length uint32 // The amount of messages in the chunk
}

func NewEmptyChunk(start int64) SegmentChunk {
	return &ReadSegmentChunk{
		Buffer: emptyBuffer,
		Start:  start,
		Length: 0,
	}
}

func (s *ReadSegmentChunk) DataBlock() []byte {
	return s.Buffer
}

func (s *ReadSegmentChunk) StartOffset() int64 {
	return s.Start
}

func (s *ReadSegmentChunk) RecordLength() uint32 {
	return s.Length
}

type writerType string

const (
	replicaWriter writerType = "replica"
	leaderWriter  writerType = "leader"
)
