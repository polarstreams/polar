package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

type SegmentReader struct {
	Items       chan ReadItem
	basePath    string
	config      conf.DatalogConfig
	topic       TopicDataId
	offset      uint64
	fileName    string
	pollDelay   time.Duration
	segmentFile *os.File
}

const pollTimes = 10
const defaultPollDelay = 500 * time.Millisecond

// Returns a log file reader.
//
// The segment reader instance is valid for a single generation, closed when the generation ends.
//
// It aggressively reads ahead and maintains local cache, so there should there
// should be a different reader per consumer group.
func NewSegmentReader(
	topic TopicDataId,
	config conf.DatalogConfig,
) *SegmentReader {
	basePath := config.DatalogPath(topic.Name, topic.Token, fmt.Sprint(topic.GenId))
	s := &SegmentReader{
		config:    config,
		basePath:  basePath,
		Items:     make(chan ReadItem, 16),
		topic:     topic,
		pollDelay: defaultPollDelay,
	}

	go s.startReading()

	return s
}

func (s *SegmentReader) startReading() {
	// Determine file start position

	//TODO: read as leader or replica per generation

	log.Info().Msgf("Start reading for topic: %s", s.topic.String())

	// Read in loop
	s.read()
}

func (s *SegmentReader) read() {
	buf := make([]byte, s.config.ReadAheadSize())
	var closeError error = nil
	header := chunkHeader{}
	remainingReader := bytes.NewReader(emptyBuffer)

	for item := range s.Items {
		writeIndex := 0
		if remainingReader.Len() > 0 {
			chunk := readChunk(remainingReader)
			if chunk != nil {
				item.SetResult(nil, chunk)
				continue
			}

			// There isn't enough data remaining for a chunk.
			// Drain the remaining reader and copy the bytes to the beginning of the buffer
			writeIndex = remainingReader.Len()
			remainingReader.Read(buf)
		}

		n, err := s.pollFile(buf[writeIndex:])

		if err != nil {
			closeError = err
			item.SetResult(err, nil)
			break
		}

		if n == 0 {
			// TODO:
			// check if there's a newer file ->
			// check if there's a newer generation ->
			item.SetResult(nil, NewEmptyChunk(s.offset))
			continue
		}

		reader := bytes.NewReader(buf[:n+writeIndex])
		chunk := readChunk(reader)
		remainingReader = reader

		if chunk != nil {
			item.SetResult(nil, chunk)
		} else {
			item.SetResult(nil, NewEmptyChunk(s.offset))
		}

		// TODO: Support discontinuous blocks for replicas
		s.offset += uint64(header.BodyLength)
	}

	s.close(closeError)
}

// Returns the number of bytes read since index
func (s *SegmentReader) pollFile(buffer []byte) (int, error) {
	for i := 0; i < pollTimes; i++ {
		n, err := s.segmentFile.Read(buffer)

		if n != 0 {
			return n, nil
		}

		if err == io.EOF || err == nil {
			// We don't have the necessary data yet
			time.Sleep(s.pollDelay)
			continue
		}

		// There was an error related to either permission change / file not found
		// or file descriptor closed by the OS
		message := fmt.Sprintf("Unexpected error reading file %s in %s", s.fileName, s.basePath)
		log.Err(err).Msg(message)
		return 0, fmt.Errorf(message)
	}

	return 0, nil
}

// closes the current file and saves the current state
func (s *SegmentReader) close(err error) {
	log.Info().Msgf("Closing segment reader for topic: %s", s.topic.String())
	if err != nil {
		for item := range s.Items {
			// TODO: Make sure to close channel after receiving an error
			item.SetResult(err, nil)
		}
	}
}

func readChunk(reader *bytes.Reader) SegmentChunk {
	if reader.Len() < chunkHeaderSize {
		return nil
	}

	header := chunkHeader{}
	err := binary.Read(reader, conf.Endianness, &header)
	utils.PanicIfErr(err, "Unexpected EOF when reading chunk header")

	// TODO: Check head CRC

	if reader.Len() < int(header.BodyLength) {
		// Rewind to the header position
		reader.Seek(-int64(chunkHeaderSize), io.SeekCurrent)
		return nil
	}

	// TODO: read buffer pooling
	readBuffer := make([]byte, int(header.BodyLength))
	_, _ = reader.Read(readBuffer)

	chunk := &ReadSegmentChunk{
		buffer: readBuffer,
		start:  header.Start,
		length: header.BodyLength,
	}
	return chunk
}
