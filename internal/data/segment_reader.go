package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

type SegmentReader struct {
	Items             chan ReadItem
	basePath          string
	config            conf.DatalogConfig
	group             string
	Topic             TopicDataId
	SourceVersion     GenVersion // The version in which this reader was created, a consumer might be on Gen=v3 but the current is v4. In this case, source would be v4 and topic.Version = v3
	offsetState       OffsetState
	maxProducedOffset *uint64
	messageOffset     uint64
	fileName          string
	nextFileName      string
	pollDelay         time.Duration
	segmentFile       *os.File
	lastSeek          time.Time
}

const pollTimes = 10
const defaultPollDelay = 500 * time.Millisecond
const minSeekIntervals = 2 * time.Second

// Returns a log file reader.
//
// The segment reader instance is valid for a single generation, closed when the generation ends or the broker is no
// longer the leader.
//
// It aggressively reads ahead and maintains local cache, so there should there
// should be a different reader instance per consumer group.
func NewSegmentReader(
	group string,
	topic TopicDataId,
	sourceVersion GenVersion,
	offsetState OffsetState,
	maxProducedOffset *uint64,
	config conf.DatalogConfig,
) (*SegmentReader, error) {
	// From the same base folder, the SegmentReader will continue reading through the files in order
	basePath := config.DatalogPath(&topic)
	s := &SegmentReader{
		config:            config,
		basePath:          basePath,
		Items:             make(chan ReadItem, 16),
		Topic:             topic,
		group:             group,
		SourceVersion:     sourceVersion,
		offsetState:       offsetState,
		maxProducedOffset: maxProducedOffset,
		pollDelay:         defaultPollDelay,
	}

	if err := s.initRead(); err != nil {
		return nil, err
	}

	go s.startReading()

	return s, nil
}

func (s *SegmentReader) startReading() {
	// Determine file start position

	// TODO: differentiate between reading from the latest generation or the previous one
	// TODO: read as leader or replica per generation

	log.Info().Msgf("Start reading for %s", &s.Topic)

	// Read in loop
	s.read()
}

func (s *SegmentReader) read() {
	buf := make([]byte, s.config.ReadAheadSize())
	var closeError error = nil
	remainingReader := bytes.NewReader(emptyBuffer)
	lastCommit := time.Time{}

	for item := range s.Items {
		s.storeOffset(lastCommit)

		writeIndex := 0
		if s.segmentFile == nil {
			// Segment file might be nil when there was no data at the beginning
			err := s.initRead()
			if err != nil {
				// There was an error opening the file
				item.SetResult(err, nil)
				continue
			}
			if s.segmentFile == nil {
				// There's still no file to read
				item.SetResult(nil, NewEmptyChunk(s.messageOffset))
				continue
			}
		}

		if remainingReader.Len() > 0 {
			// Consume all the read-ahead data
			chunk := readChunk(remainingReader)
			if chunk != nil {
				item.SetResult(nil, chunk)
				continue
			}

			// There isn't enough data remaining for a chunk.
			if remainingReader.Len() > 0 {
				writeIndex = remainingReader.Len()
				// Drain the remaining reader and copy the bytes to the beginning of the buffer
				remainingReader.Read(buf)
			}
		}

		n, err := s.pollFile(buf[writeIndex:])

		if err != nil {
			// There's no point in trying to continue reading
			closeError = err
			item.SetResult(err, nil)
			break
		}

		if n == 0 {
			// There's no new data in this file
			item.SetResult(nil, NewEmptyChunk(s.messageOffset))

			if s.nextFileName == "" {
				s.nextFileName = s.checkNextFile()
			} else {
				// We've polled the previous file after discovering a new one
				// We can safely switch the active file
				s.swapSegmentFile()
			}
			continue
		}

		reader := bytes.NewReader(buf[:n+writeIndex])
		chunk := readChunk(reader)
		remainingReader = reader

		if chunk != nil {
			item.SetResult(nil, chunk)
			s.messageOffset += chunk.StartOffset() + uint64(chunk.RecordLength())
		} else {
			item.SetResult(nil, NewEmptyChunk(s.messageOffset))
		}

		// TODO: Support discontinuous blocks for replicas
	}

	s.close(closeError)
}

func (s *SegmentReader) storeOffset(lastCommit time.Time) {
	commit := false
	if time.Since(lastCommit) >= s.config.AutoCommitInterval() {
		lastCommit = time.Now()
		commit = true
	}
	value := Offset{
		Offset:  s.messageOffset,
		Version: s.Topic.GenId,
		Source:  s.SourceVersion,
	}
	log.Debug().Bool("commit", commit).Str("group", s.group).Msgf("Setting offset for %s", &s.Topic)
	s.offsetState.Set(s.group, s.Topic.Name, s.Topic.Token, s.Topic.RangeIndex, value, OffsetCommitAll)
}

// Tries open the initial file and seek the correct position, returning an error when there's an
// I/O-related error
func (s *SegmentReader) initRead() error {
	foundFileName, fileOffset, err := s.fullSeek()
	if err != nil {
		return err
	}

	if foundFileName == "" {
		// No file found on folder
		return nil
	}
	s.segmentFile, err = os.OpenFile(filepath.Join(s.basePath, foundFileName), conf.SegmentFileReadFlags, 0)
	if err != nil {
		log.Err(err).Msgf("File %s in %s could not be opened by reader", foundFileName, s.basePath)
		return err
	}
	s.fileName = foundFileName

	if fileOffset > 0 {
		log.Info().Msgf("Seeking position %d for reading in file %s", fileOffset, foundFileName)
		// The file offset is expected to be aligned by the writer
		_, err = s.segmentFile.Seek(fileOffset, io.SeekStart)
		log.Warn().Err(err).Msgf("Segment file could not be seeked")
	} else {
		log.Info().Msgf("Started reading file %s from position 0", foundFileName)
	}

	return nil
}

// Iterates through all the dlog files in the basePath and looks for the closer (lower bound)
// file for the reader.offset value.
//
// It returns the file name, file offset and the error (when base path not found) with no side effect.
func (s *SegmentReader) fullSeek() (string, int64, error) {
	pattern := fmt.Sprintf("%s/*.%s", s.basePath, conf.SegmentFileExtension)
	log.Info().Msgf("Looking for files matching the pattern %s", pattern)
	entries, err := filepath.Glob(pattern)
	if err != nil {
		log.Err(err).Msgf("There was an error listing files in %s while seeking", s.basePath)
		return "", 0, err
	}

	if len(entries) == 0 {
		log.Info().Msgf("Reader could not find any files in %s", s.basePath)
		return "", 0, nil
	}

	sort.Strings(entries)

	dlogFilePrefix := ""
	for _, entry := range entries {
		filePrefix := strings.Split(filepath.Base(entry), ".")[0]
		startOffset, err := strconv.ParseUint(filePrefix, 10, 64)
		if err != nil {
			continue
		}
		if startOffset > s.messageOffset {
			break
		}
		dlogFilePrefix = filePrefix
	}

	fileOffset := tryReadIndexFile(s.basePath, dlogFilePrefix, s.messageOffset)

	return fmt.Sprintf("%s.%s", dlogFilePrefix, conf.SegmentFileExtension), fileOffset, nil
}

// Returns the name of the file after the current one or an empty string
func (s *SegmentReader) checkNextFile() string {
	entries, err := filepath.Glob(fmt.Sprintf("%s/*.%s", s.basePath, conf.SegmentFileExtension))
	if err != nil {
		log.Err(err).Msgf("There was an error listing files in %s checking for next file", s.basePath)
		return ""
	}

	sort.Strings(entries)
	nextIndex := -1

	for i, entry := range entries {
		if filepath.Base(entry) == s.fileName {
			nextIndex = i + 1
			break
		}
	}

	if nextIndex > 0 && len(entries) > nextIndex {
		return filepath.Base(entries[nextIndex])
	}

	return ""
}

// Closes the previous segment file and opens the new one, setting is as the current one.
func (s *SegmentReader) swapSegmentFile() {
	newFile, err := os.OpenFile(filepath.Join(s.basePath, s.nextFileName), conf.SegmentFileReadFlags, 0)
	if err != nil {
		log.Err(err).Msgf("Next file could not be opened")
		return
	}
	s.fileName = s.nextFileName
	s.nextFileName = ""

	if err := s.segmentFile.Close(); err != nil {
		log.Warn().Err(err).Msgf("There was an error when closing file in %s", s.basePath)
	}
	s.segmentFile = newFile
}

// Returns the number of bytes read since index
func (s *SegmentReader) pollFile(buffer []byte) (int, error) {
	// buffer might not be a multiple of alignmentSize
	bytesToAlign := len(buffer) % alignmentSize
	if bytesToAlign > 0 {
		// Crop the last bytes to make to compatible with DIRECT I/O
		buffer = buffer[:len(buffer)-bytesToAlign]
	}

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
	log.Info().Msgf("Closing segment reader for topic: %s", s.Topic.String())
	if err != nil {
		for item := range s.Items {
			// TODO: Make sure to close channel after receiving an error
			item.SetResult(err, nil)
		}
	}
}

func readChunk(reader *bytes.Reader) SegmentChunk {
	header := chunkHeader{}
	// Peek the next chunk flags for alignment
	for {
		flag, err := reader.ReadByte()
		if err == io.EOF {
			return nil
		}
		utils.PanicIfErr(err, "Unexpected error when reading chunk header")
		if flag != alignmentFlag {
			// It's a valid header
			_ = reader.UnreadByte()
			break
		}
	}

	if reader.Len() < chunkHeaderSize {
		return nil
	}

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
