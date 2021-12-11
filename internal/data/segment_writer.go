package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/metrics"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

const flushResolution = 200 * time.Millisecond
const flushInterval = 5 * time.Second
const alignmentSize = 512

var alignmentBuffer = createAlignmentBuffer()

// SegmentWriter contains the logic to write segments on disk and replicate them.
type SegmentWriter struct {
	Items          chan SegmentChunk
	segmentId      uint64
	buffer         *bytes.Buffer
	lastFlush      time.Time
	bufferedOffset uint64 // Stores the offset of the first message buffered since it was buffered
	config         conf.DatalogConfig
	segmentFile    *os.File
	indexFile      *indexFileWriter
	segmentLength  uint64
	basePath       string
	topic          TopicDataId
	replicator     Replicator
}

func NewSegmentWriter(
	topic TopicDataId,
	gossiper Replicator,
	config conf.DatalogConfig,
	segmentId *uint64,
) (*SegmentWriter, error) {
	basePath := config.DatalogPath(topic.Name, topic.Token, fmt.Sprint(topic.GenId))

	if err := os.MkdirAll(basePath, DirectoryPermissions); err != nil {
		return nil, err
	}

	s := &SegmentWriter{
		// Limit's to 1 outstanding write (the current one)
		// The next group can be generated while the previous is being flushed and sent
		Items:       make(chan SegmentChunk, 0),
		buffer:      utils.NewBufferCap(config.SegmentBufferSize()),
		config:      config,
		segmentFile: nil,
		indexFile:   newIndexFileWriter(basePath, config),
		basePath:    basePath,
		topic:       topic,
		replicator:  gossiper,
	}

	if segmentId == nil {
		log.Debug().Msgf("Creating segment writer for token %d and topic '%s' as leader", topic.Token, topic.Name)
		// Start with a file at offset 0
		s.createFile(0)
		go s.writeLoopAsLeader()
	} else {
		log.Debug().Msgf("Creating segment writer for token %d and topic '%s' as replica", topic.Token, topic.Name)
		s.createFile(*segmentId)
		go s.writeLoopAsReplica()
	}

	go s.flushTimer()
	return s, nil
}

// writeLoopAsLeader appends to the local file and sends to replicas
func (s *SegmentWriter) writeLoopAsLeader() {

	for dataItem := range s.Items {
		if s.maybeFlush() {
			s.maybeCloseSegment()
		}

		if dataItem == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}

		item, ok := dataItem.(LocalWriteItem)
		if !ok {
			log.Panic().Msgf("Invalid type for writing as a leader: %v", dataItem)
		}

		s.writeToBuffer(item)

		// Response channel should be buffered in case the response is discarded
		response := make(chan error, 1)

		// Start sending in the background while flushing is occurring
		go s.send(item, s.segmentId, response)

		// Check whether to flush before blocking again in the for loop
		if s.maybeFlush() {
			// Check whether the file has to be closed
			s.maybeCloseSegment()
		}

		item.SetResult(<-response)
	}
}

// writeLoopAsReplica appends to local file as replica
func (s *SegmentWriter) writeLoopAsReplica() {

	for dataItem := range s.Items {
		s.maybeFlush()

		if dataItem == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}

		item, ok := dataItem.(ReplicationDataItem)
		if !ok {
			log.Panic().Msgf("Invalid type for writing as a replica: %v", dataItem)
		}

		if s.segmentId != item.SegmentId() {
			if s.buffer.Len() > 0 {
				s.flush("closing")
			}
			s.closeFile()
		}

		s.writeToBuffer(item)

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

	canBufferNextGroup := s.buffer.Len()+s.config.MaxGroupSize() < s.config.SegmentBufferSize()
	if canBufferNextGroup && time.Now().Sub(s.lastFlush) < flushInterval {
		// Time has not passed and there's enough capacity
		// in the buffer for the next group
		return false
	}

	reason := "timer"
	if !canBufferNextGroup {
		reason = "buffer size"
	}

	s.flush(reason)

	return true
}

func (s *SegmentWriter) createFile(segmentId uint64) {
	s.segmentId = segmentId
	name := fmt.Sprintf("%020d.dlog", s.segmentId)
	log.Debug().Msgf("Creating segment file %s on %s", name, s.basePath)

	f, err := os.OpenFile(filepath.Join(s.basePath, name), conf.WriteFlags, FilePermissions)
	if err != nil {
		// Can't create segment
		log.Err(err).Msgf("Failed to create segment file at %s", s.basePath)
		panic(err)
	}
	s.segmentFile = f
}

func (s *SegmentWriter) flush(reason string) {
	s.alignBuffer()
	length := uint64(s.buffer.Len())

	if s.segmentFile == nil {
		s.createFile(s.bufferedOffset)
	}

	log.Debug().
		Str("reason", reason).
		Msgf("Flushing segment file %d on %s", s.segmentId, s.basePath)

	// Sync copy the buffer to the file
	if _, err := s.segmentFile.Write(s.buffer.Bytes()); err != nil {
		// Data loss, we should panic
		log.Err(err).Msgf("Failed to write to segment file %d at %s", s.segmentId, s.basePath)
		panic(err)
	}

	s.indexFile.append(s.segmentId, s.bufferedOffset, s.segmentLength)
	s.segmentLength += length
	s.buffer.Reset()
	s.lastFlush = time.Now()
	metrics.SegmentFlushKib.Observe(float64(length) / 1024)
}

// maybeCloseSegment determines whether the segment file should be closed.
func (s *SegmentWriter) maybeCloseSegment() {
	if s.segmentLength+uint64(s.config.SegmentBufferSize()) > uint64(s.config.MaxSegmentSize()) {
		s.closeFile()
	}
}

func (s *SegmentWriter) closeFile() {
	previousSegmentId := s.segmentId
	previousFile := s.segmentFile
	log.Debug().Msgf("Closing segment file %d on %s", previousSegmentId, s.basePath)

	// Close the file in the background
	go func() {
		log.Err(previousFile.Close()).Msgf("Segment file %d closed on %s", previousSegmentId, s.basePath)
	}()

	s.indexFile.closeFile(previousSegmentId)
	s.segmentFile = nil
	s.segmentId = math.MaxUint64
	s.segmentLength = 0
}

func (s *SegmentWriter) writeToBuffer(item SegmentChunk) {
	if s.lastFlush.IsZero() || s.buffer.Len() == 0 {
		// When the buffer was previously empty, we should reset the flush check logic
		s.lastFlush = time.Now()
		s.bufferedOffset = item.StartOffset()
	}
	headStartIndex := s.buffer.Len()
	compressedBody := item.DataBlock()

	// Write head
	binary.Write(s.buffer, conf.Endianness, uint32(len(compressedBody)))
	binary.Write(s.buffer, conf.Endianness, item.StartOffset())
	binary.Write(s.buffer, conf.Endianness, item.RecordLength())

	// Calculate head checksum and write it
	head := s.buffer.Bytes()[headStartIndex:]
	binary.Write(s.buffer, conf.Endianness, crc32.ChecksumIEEE(head))

	// Write body
	s.buffer.Write(compressedBody)
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
	item LocalWriteItem,
	segmentId uint64,
	response chan error,
) {
	err := s.replicator.SendToFollowers(
		item.Replication(),
		s.topic,
		segmentId,
		item)
	response <- err
}

func createAlignmentBuffer() []byte {
	b := make([]byte, alignmentSize-1)
	for i := range b {
		b[i] = 0xff
	}
	return b
}
