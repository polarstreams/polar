package data

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/metrics"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const flushResolution = 200 * time.Millisecond
const flushInterval = 5 * time.Second
const segmentMaxBufferSize = 32 * conf.Mib
const alignmentSize = 512

var alignmentBuffer = createAlignmentBuffer()

// SegmentWriter contains the logic to write a segment on disk and replicate it.
type SegmentWriter struct {
	Items         chan DataItem
	segmentId     int64
	buffer        *bytes.Buffer
	lastFlush     time.Time
	config        conf.DatalogConfig
	segmentFile   *os.File
	segmentLength int
	basePath      string
	topic         types.TopicDataId
	replicator    types.Replicator
}

func NewSegmentWriter(
	topic types.TopicDataId,
	gossiper types.Replicator,
	config conf.DatalogConfig,
	segmentId int64,
) (*SegmentWriter, error) {
	basePath := config.DatalogPath(topic.Name, topic.Token, fmt.Sprint(topic.GenId))

	s := &SegmentWriter{
		// Limit's to 1 outstanding write (the current one)
		// The next group can be generated while the previous is being flushed and sent
		Items:       make(chan DataItem, 0),
		buffer:      bytes.NewBuffer(make([]byte, 0, segmentMaxBufferSize)),
		config:      config,
		segmentFile: nil,
		basePath:    basePath,
		topic:       topic,
		replicator:  gossiper,
		segmentId:   segmentId,
	}

	if err := os.MkdirAll(basePath, DirectoryPermissions); err != nil {
		return nil, err
	}

	if segmentId == 0 {
		log.Debug().Msgf("Creating segment writer for token %d and topic '%s' as leader", topic.Token, topic.Name)
		s.segmentId = time.Now().UnixNano()
		go s.appendAsLeader()
	} else {
		log.Debug().Msgf("Creating segment writer for token %d and topic '%s' as replica", topic.Token, topic.Name)
		go s.appendAsReplica()
	}

	go s.flushTimer()
	return s, nil
}

type DataItem interface {
	DataBlock() []byte
	Replication() *types.ReplicationInfo
	SegmentId() int64
	SetResult(error)
}

// appendAsLeader appends to the local file and sends to replicas
func (s *SegmentWriter) appendAsLeader() {
	for item := range s.Items {
		if s.maybeFlush() {
			s.maybeCloseSegment()
		}

		if item == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}

		s.writeToBuffer(item.DataBlock())

		// Response channel should be buffered in case the response is discarded
		response := make(chan error, 1)

		// Start sending in the background while flushing is occurring
		go s.send(item.DataBlock(), *item.Replication(), s.segmentId, response)

		// Check whether to flush before blocking again in the for loop
		if s.maybeFlush() {
			// Check whether the file has to be closed
			s.maybeCloseSegment()
		}

		item.SetResult(<-response)
	}
}

// appendAsReplica appends to local file as replica
func (s *SegmentWriter) appendAsReplica() {
	for item := range s.Items {
		s.maybeFlush()
		if item == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}

		if s.segmentId != item.SegmentId() {
			if s.buffer.Len() > 0 {
				s.flush("closing")
			}
			s.closeFile(item.SegmentId())
		}

		s.writeToBuffer(item.DataBlock())

		// Check whether to flush before blocking again in the for loop
		s.maybeFlush()

		item.SetResult(nil)
	}
}

// maybeFlush will write to the file when the next group doesn't fit in memory
// or time has passed since last flush
func (s *SegmentWriter) maybeFlush() bool {
	if s.buffer.Len() == 0 {
		// No data to flush yet
		return false
	}

	canBufferNextGroup := s.buffer.Len()+s.config.MaxGroupSize() < segmentMaxBufferSize
	if canBufferNextGroup && time.Now().Sub(s.lastFlush) < flushInterval {
		// Time has not passed and there's enough capacity
		// in the buffer for the next group
		return false
	}

	if s.segmentFile == nil {
		// Create new file
		name := fmt.Sprintf("%20d.dlog", s.segmentId)
		f, err := os.OpenFile(filepath.Join(s.basePath, name), conf.WriteFlags, FilePermissions)
		if err != nil {
			// Can't create segment
			log.Err(err).Msgf("Failed to create segment file at %s", s.basePath)
			panic(err)
		}
		s.segmentFile = f
	}

	reason := "timer"
	if !canBufferNextGroup {
		reason = "buffer size"
	}

	s.flush(reason)

	return true
}

func (s *SegmentWriter) flush(reason string) {
	s.alignBuffer()

	log.Debug().
		Str("reason", reason).
		Msgf("Flushing segment file %d for topic '%s' and token %d", s.segmentId, s.topic.Name, s.topic.Token)

	length := s.buffer.Len()

	// Sync copy the buffer to the file
	if _, err := s.segmentFile.Write(s.buffer.Bytes()); err != nil {
		// Data loss, we should panic
		log.Err(err).Msgf("Failed to write to segment file %d at %s", s.segmentId, s.basePath)
		panic(err)
	}

	s.segmentLength += length
	s.buffer.Reset()
	s.lastFlush = time.Now()
	metrics.SegmentFlushKib.Observe(float64(length) / 1024)
}

// maybeCloseSegment determines whether the segment file should be closed.
func (s *SegmentWriter) maybeCloseSegment() {
	if s.segmentLength+segmentMaxBufferSize > s.config.MaxSegmentSize() {
		s.closeFile(time.Now().UnixNano())
	}
}

func (s *SegmentWriter) closeFile(newSegmentId int64) {
	previousSegmentId := s.segmentId
	previousFile := s.segmentFile
	log.Debug().
		Msgf("Closing segment file %d for topic '%s' and token %d", previousSegmentId, s.topic.Name, s.topic.Token)

	// Close the file in the background
	go func() {
		log.
			Err(previousFile.Close()).
			Msgf("Segment file %d closed for topic '%s' and token %d",
				previousSegmentId, s.topic.Name, s.topic.Token)
	}()

	s.segmentFile = nil
	s.segmentId = newSegmentId
	s.segmentLength = 0
}

func (s *SegmentWriter) writeToBuffer(data []byte) {
	if s.lastFlush.IsZero() || s.buffer.Len() == 0 {
		// When the buffer was previously empty, we should reset the flush check logic
		s.lastFlush = time.Now()
	}
	s.buffer.Write(data)
}

func (c *SegmentWriter) flushTimer() {
	for {
		time.Sleep(flushResolution)
		// Send a nil data item as an indication of a flush message
		c.Items <- nil
	}
}

func (s *SegmentWriter) alignBuffer() {
	rem := s.buffer.Len() % alignmentSize
	if rem == 0 {
		return
	}

	toAlign := alignmentSize - rem

	s.buffer.Write(alignmentBuffer[0:toAlign])
}

func (s *SegmentWriter) send(
	block []byte,
	replicationInfo types.ReplicationInfo,
	segmentId int64,
	response chan error,
) {
	err := s.replicator.SendToFollowers(replicationInfo, s.topic, segmentId, block)
	response <- err
}

func createAlignmentBuffer() []byte {
	b := make([]byte, alignmentSize-1)
	for i := range b {
		b[i] = 0xff
	}
	return b
}
