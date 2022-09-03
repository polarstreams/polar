package consuming

import (
	"encoding/binary"
	"io"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/google/uuid"
)

// Represents a single consumer instance
type ConsumerInfo struct {
	Id     string   `json:"id"`    // A unique id within the consumer group
	Group  string   `json:"group"` // A group unique id
	Topics []string `json:"topics"`

	// Only used internally
	assignedTokens []Token
}

type ReplicationReaderFactory interface {
	GetOrCreate(topic *TopicDataId, topology *TopologyInfo, topicGen *Generation, offsetState OffsetState) data.ReplicationReader
}

type segmentReadItem struct {
	chunkResult chan SegmentChunk
	errorResult chan error
	origin      uuid.UUID
	commitOnly  bool
}

func newSegmentReadItem(origin uuid.UUID, commitOnly bool) *segmentReadItem {
	return &segmentReadItem{
		chunkResult: make(chan SegmentChunk, 1),
		errorResult: make(chan error, 1),
		origin:      origin,
		commitOnly:  commitOnly,
	}
}

func (r *segmentReadItem) SetResult(err error, chunk SegmentChunk) {
	r.chunkResult <- chunk
	r.errorResult <- err
}

func (r *segmentReadItem) Origin() uuid.UUID {
	return r.origin
}

func (r *segmentReadItem) CommitOnly() bool {
	return r.commitOnly
}

func (r *segmentReadItem) result() (err error, chunk SegmentChunk) {
	return <-r.errorResult, <-r.chunkResult
}

// Represents a single response item from a poll request
type consumerResponseItem struct {
	chunk SegmentChunk
	topic TopicDataId
}

func (i *consumerResponseItem) Marshal(w io.Writer) error {
	// Can be extracted into "MarshalTopic"
	if err := binary.Write(w, conf.Endianness, i.topic.Token); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, i.topic.RangeIndex); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, i.topic.Version); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, uint8(len(i.topic.Name))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(i.topic.Name)); err != nil {
		return err
	}
	payload := i.chunk.DataBlock()
	if err := binary.Write(w, conf.Endianness, int32(len(payload))); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// Presents a map key for readers by token range
type readerKey struct {
	token      Token
	rangeIndex RangeIndex
}
