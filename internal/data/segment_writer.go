package data

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/metrics"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog/log"
)

const flushResolution = 200 * time.Millisecond
const alignmentFlag = byte(1 << 7)

var alignmentBuffer = createAlignmentBuffer()

// SegmentWriter contains the logic to write segments on disk and replicate them.
//
// There should be an instance per topic+token+generation. When the generation changes for
// a token, the channel should be closed.
type SegmentWriter struct {
	Items          chan SegmentChunk
	Topic          TopicDataId
	segmentId      int64
	buffer         *bytes.Buffer // Must be backed by an aligned buffer
	lastFlush      time.Time
	bufferedOffset int64 // Stores the offset of the first message buffered since it was buffered
	tailOffset     int64 // Value of the last written message
	config         conf.DatalogConfig
	segmentFile    *os.File
	indexFile      *indexFileWriter
	segmentLength  int64
	basePath       string
	replicator     Replicator
}

func NewSegmentWriter(
	topic TopicDataId,
	gossiper Replicator,
	config conf.DatalogConfig,
	segmentId *int64,
) (*SegmentWriter, error) {
	basePath := config.DatalogPath(&topic)

	if err := os.MkdirAll(basePath, DirectoryPermissions); err != nil {
		return nil, err
	}

	s := &SegmentWriter{
		// Limit's to 1 outstanding write (the current one)
		// The next group can be generated while the previous is being flushed and sent
		Items:       make(chan SegmentChunk, 0),
		Topic:       topic,
		buffer:      createAlignedByteBuffer(config.SegmentBufferSize()), // Use an aligned buffer for writing
		config:      config,
		segmentFile: nil,
		indexFile:   newIndexFileWriter(basePath, config),
		basePath:    basePath,
		replicator:  gossiper,
	}

	if segmentId == nil {
		log.Info().Msgf("Creating segment writer as leader for %s", &topic)
		// Start with a file at offset 0
		s.createFile(0)
		go s.writeLoopAsLeader()
	} else {
		log.Info().Msgf("Creating segment writer as replica for %s", &topic)
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

	s.close()
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

	s.close()
}

func (s *SegmentWriter) close() {
	if s.buffer.Len() > 0 {
		s.flush("closing writer")
	}
	s.closeFile()
}

// maybeFlush will write to the file when the next group doesn't fit in memory
// or time has passed since last flush
func (s *SegmentWriter) maybeFlush() bool {
	if s.buffer.Len() == 0 {
		// No data to flush yet
		return false
	}

	canBufferNextGroup := s.buffer.Len()+s.config.MaxGroupSize() < s.config.SegmentBufferSize()
	if canBufferNextGroup && time.Now().Sub(s.lastFlush) < s.config.SegmentFlushInterval() {
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

func (s *SegmentWriter) createFile(segmentId int64) {
	s.segmentId = segmentId
	name := conf.SegmentFileName(s.segmentId)
	log.Debug().Msgf("Creating segment file %s on %s", name, s.basePath)

	f, err := os.OpenFile(filepath.Join(s.basePath, name), conf.SegmentFileWriteFlags, FilePermissions)
	if err != nil {
		// Can't create segment
		log.Err(err).Msgf("Failed to create segment file at %s", s.basePath)
		panic(err)
	}
	s.segmentFile = f
}

func (s *SegmentWriter) flush(reason string) {
	s.writeAlignmentBytes()
	length := int64(s.buffer.Len())

	if s.segmentFile == nil {
		s.createFile(s.bufferedOffset)
	}

	buf := s.buffer.Bytes()
	log.Debug().
		Str("reason", reason).
		Int64("offset", s.tailOffset).
		Msgf("Writing %d bytes to segment file %s/%s", len(buf), s.basePath, conf.SegmentFileName(s.segmentId))

	// Sync copy the buffer to the file
	if _, err := s.segmentFile.Write(buf); err != nil {
		// Data loss, we should panic
		log.Err(err).Msgf("Failed to write to segment file %d at %s", s.segmentId, s.basePath)
		panic(err)
	}

	// Store the index file and producer offset
	s.indexFile.append(s.segmentId, s.bufferedOffset, s.segmentLength, s.tailOffset)
	s.segmentLength += length
	s.buffer.Reset()
	s.lastFlush = time.Now()
	metrics.SegmentFlushKib.Observe(float64(length) / 1024)
}

// maybeCloseSegment determines whether the segment file should be closed.
func (s *SegmentWriter) maybeCloseSegment() {
	if s.segmentLength+int64(s.config.SegmentBufferSize()) > int64(s.config.MaxSegmentSize()) {
		s.closeFile()
	}
}

func (s *SegmentWriter) closeFile() {
	previousSegmentId := s.segmentId
	previousFile := s.segmentFile
	log.Debug().Msgf("Closing segment file %d on %s", previousSegmentId, s.basePath)

	// Close the segment file in the background
	go func() {
		err := previousFile.Close()
		log.Err(err).Msgf("Segment file %d closed on %s", previousSegmentId, s.basePath)
	}()

	// Close the index file
	s.indexFile.closeFile(previousSegmentId, s.tailOffset)

	s.segmentFile = nil
	s.segmentId = math.MaxInt64
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
	const flags = byte(0) // Only valid flag is alignment 0x80 (10000000)

	recordLength := item.RecordLength()
	if recordLength > 0 {
		s.tailOffset = item.StartOffset() + int64(recordLength) - 1
	}

	// Write head
	binary.Write(s.buffer, conf.Endianness, flags)
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
	defer func() {
		// Channel might be closed in the future, move on
		recover()
	}()

	for {
		time.Sleep(flushResolution)
		// Send a nil data item as an indication of a flush message
		c.Items <- nil
	}
}

// Adds the alignment bytes to the buffer
func (s *SegmentWriter) writeAlignmentBytes() {
	rem := s.buffer.Len() % alignmentSize
	if rem == 0 {
		return
	}

	toAlign := alignmentSize - rem
	s.buffer.Write(alignmentBuffer[0:toAlign])
}

func (s *SegmentWriter) send(
	item LocalWriteItem,
	segmentId int64,
	response chan error,
) {
	err := s.replicator.SendToFollowers(
		item.Replication(),
		s.Topic,
		segmentId,
		item)
	response <- err
}

func createAlignmentBuffer() []byte {
	b := make([]byte, alignmentSize-1)
	for i := range b {
		b[i] = alignmentFlag
	}
	return b
}
