package data

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const flushResolution = 200 * time.Millisecond
const flushInterval = 2 * time.Second
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
		s.segmentId = time.Now().UnixNano()
	}

	go s.flushTimer()
	go s.appendAndSend()
	return s, nil
}

type DataItem interface {
	DataBlock() []byte
	Replication() *types.ReplicationInfo
	SetResult(error)
}

func (s *SegmentWriter) appendAndSend() {
	for item := range s.Items {
		s.maybeFlushSegment()
		if item == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}

		s.writeToSegmentBuffer(item.DataBlock())

		// Response channel should be buffered in case the response is discarded
		response := make(chan error, 1)

		if replication := item.Replication(); replication != nil {
			// Start sending in the background while flushing is occurring
			go s.send(item.DataBlock(), *replication, response)
		} else {
			response <- nil
		}

		// Check whether to flush before blocking again in the for loop
		s.maybeFlushSegment()

		item.SetResult(<-response)
	}
}

// maybeFlushSegment will write to the file when the next group doesn't fit in memory
// or time has passed since last flush
func (s *SegmentWriter) maybeFlushSegment() {
	if s.buffer.Len() == 0 {
		// No data to flush yet
		return
	}

	canBufferNextGroup := s.buffer.Len()+s.config.MaxGroupSize() < segmentMaxBufferSize
	if canBufferNextGroup && time.Now().Sub(s.lastFlush) < flushInterval {
		// Time has not passed and there's enough capacity
		// in the buffer for the next group
		return
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

	s.alignBuffer()

	log.Debug().Msgf("Flushing segment file %d for topic '%s' and token %d", s.segmentId, s.topic.Name, s.topic.Token)

	// Sync copy the buffer to the file
	s.segmentFile.Write(s.buffer.Bytes())
	if _, err := s.buffer.WriteTo(s.segmentFile); err != nil {
		// Data loss, we should panic
		log.Err(err).Msgf("Failed to write to segment file %d at %s", s.segmentId, s.basePath)
		panic(err)
	}

	s.segmentLength += s.buffer.Len()

	if s.segmentLength+segmentMaxBufferSize > s.config.MaxSegmentSize() {
		// Close the file
		log.Err(s.segmentFile.Close()).Msgf("Segment file %d closed", s.segmentId)
		s.segmentFile = nil
	}

	s.buffer.Reset()
	s.lastFlush = time.Now()

	return
}

func (s *SegmentWriter) writeToSegmentBuffer(data []byte) {
	if s.lastFlush.IsZero() {
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

func (s *SegmentWriter) send(block []byte, replicationInfo types.ReplicationInfo, response chan error) {
	err := s.replicator.SendToFollowers(replicationInfo, s.topic, s.segmentId, block)
	response <- err
}

func createAlignmentBuffer() []byte {
	b := make([]byte, alignmentSize-1)
	for i := range b {
		b[i] = 0xff
	}
	return b
}
