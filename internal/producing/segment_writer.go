package producing

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const flushResolution = 200 * time.Millisecond
const flushInterval = 2 * time.Second
const segmentMaxBufferSize = 32 * conf.Mib
const alignmentSize = 512

var alignmentBuffer = createAlignmentBuffer()

type segmentWriter struct {
	items         chan *dataItem
	segmentId     int64
	buffer        *bytes.Buffer
	lastFlush     time.Time
	config        conf.ProducerConfig
	segmentFile   *os.File
	segmentLength int
	basePath      string
}

func newSegmentWriter(
	topic string,
	token types.Token,
	genId string,
	config conf.ProducerConfig,
) (*segmentWriter, error) {
	basePath := config.DatalogPath(topic, token, genId)

	s := &segmentWriter{
		// Limit's to 1 outstanding write (the current one)
		// The next group can be generated while the previous is being flushed and sent
		items:       make(chan *dataItem, 0),
		buffer:      bytes.NewBuffer(make([]byte, 0, segmentMaxBufferSize)),
		config:      config,
		segmentFile: nil,
		basePath:    basePath,
	}

	if err := os.MkdirAll(basePath, data.DirectoryPermissions); err != nil {
		return nil, err
	}

	go s.flushTimer()
	go s.appendAndSend()
	return s, nil
}

type dataItem struct {
	data  []byte
	group []record
}

func (s *segmentWriter) appendAndSend() {
	for item := range s.items {
		s.maybeFlushSegment()
		if item == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}
		s.writeToSegmentBuffer(item.data)
		// Response channel should be buffered in case the response is discarded
		response := make(chan error, 1)

		// Start sending in the background while flushing is occurring
		go s.send(s.segmentId, item.data, item.group, response)

		s.maybeFlushSegment()
		sendResponse(item.group, <-response)
	}
}

// maybeFlushSegment will write to the file when the next group doesn't fit in memory
// or time has passed since last flush
func (s *segmentWriter) maybeFlushSegment() {
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
		f, err := os.OpenFile(filepath.Join(s.basePath, name), conf.WriteFlags, data.FilePermissions)
		if err != nil {
			// Can't create segment
			log.Err(err).Msgf("Failed to create segment file at %s", s.basePath)
			panic(err)
		}
		s.segmentFile = f
	}

	s.alignBuffer()
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

func (s *segmentWriter) writeToSegmentBuffer(data []byte) {
	if s.lastFlush.IsZero() {
		s.lastFlush = time.Now()
	}
	if s.segmentId == 0 {
		s.segmentId = time.Now().UnixNano()
	}
	s.buffer.Write(data)
}

func (c *segmentWriter) flushTimer() {
	for {
		time.Sleep(flushResolution)
		// Send a nil data item as an indication of a flush message
		c.items <- nil
	}
}

func (s *segmentWriter) alignBuffer() {
	rem := s.buffer.Len() % alignmentSize
	if rem == 0 {
		return
	}

	toAlign := alignmentSize - rem

	s.buffer.Write(alignmentBuffer[0:toAlign])
}

func (c *segmentWriter) send(segmentId int64, block []byte, group []record, response chan error) {

	//TODO: Implement

	// if err := p.gossiper.SendToFollowers(replicationInfo, topic, body); err != nil {
	// 	return err
	// }
	response <- nil
}

func createAlignmentBuffer() []byte {
	b := make([]byte, alignmentSize-1)
	for i := range b {
		b[i] = 0xff
	}
	return b
}

func sendResponse(group []record, err error) {
	for _, r := range group {
		r.response <- err
	}
}
