package data

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog/log"
)

type SegmentReader struct {
	Items             chan ReadItem
	basePath          string
	headerBuf         []byte
	config            conf.DatalogConfig
	group             string
	isLeader          bool // Determines whether the current broker was the leader of the generation we are reading from
	replicationReader ReplicationReader
	Topic             TopicDataId
	SourceVersion     GenId // The version in which this reader was created, a consumer might be on Gen=v3 but the current is v4. In this case, source would be v4 and topic.Version = v3
	offsetState       OffsetState
	MaxProducedOffset *int64 // When set, it determines the last offset produced for this topicId for an old generation
	messageOffset     int64
	fileName          string
	nextFileName      string
	segmentFile       *os.File
	lastSeek          time.Time
}

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
	isLeader bool,
	replicationReader ReplicationReader,
	topic TopicDataId,
	sourceVersion GenId,
	initialOffset int64,
	offsetState OffsetState,
	maxProducedOffset *int64,
	config conf.DatalogConfig,
) (*SegmentReader, error) {
	// From the same base folder, the SegmentReader will continue reading through the files in order
	basePath := config.DatalogPath(&topic)
	s := &SegmentReader{
		config:            config,
		basePath:          basePath,
		Items:             make(chan ReadItem, 16),
		headerBuf:         make([]byte, chunkHeaderSize),
		Topic:             topic,
		group:             group,
		isLeader:          isLeader,
		replicationReader: replicationReader,
		SourceVersion:     sourceVersion,
		messageOffset:     initialOffset,
		offsetState:       offsetState,
		MaxProducedOffset: maxProducedOffset,
	}

	if err := s.initRead(true); err != nil {
		return nil, err
	}

	go s.startReading()

	return s, nil
}

func (s *SegmentReader) startReading() {
	log.Info().Msgf("Start reading for %s", &s.Topic)

	if !s.isLeader {
		// Start early to initialize in the background
		s.initRead(false)

		s.readAsFollower()
	} else {
		s.read()
	}
}

func (s *SegmentReader) read() {
	buf := make([]byte, s.config.ReadAheadSize())
	var closeError error = nil
	reader := bytes.NewReader(emptyBuffer) // The reader that gets assigned the buffer slice from disk
	lastCommit := &time.Time{}

	for item := range s.Items {
		s.storeOffset(lastCommit)

		writeIndex := 0
		if s.segmentFile == nil {
			// Segment file might be nil when there was no data at the beginning
			err := s.initRead(false)
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

		if reader.Len() > 0 {
			// Consume all the read-ahead data
			if s.serveChunk(reader, item) {
				continue
			}

			// There isn't enough data remaining for a chunk.
			if reader.Len() > 0 {
				writeIndex = reader.Len()
				// Drain the remaining reader and copy the bytes to the beginning of the buffer
				reader.Read(buf)
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

			// Check whether we have finished
			if s.nextFileName == "" {
				s.nextFileName = s.checkNextFile()
			} else {
				// We've polled the previous file after discovering a new one
				// We can safely switch the active file
				s.swapSegmentFile()
			}
			continue
		}

		totalWritten := writeIndex + n
		reader = bytes.NewReader(buf[:totalWritten])

		if !s.serveChunk(reader, item) {
			item.SetResult(nil, NewEmptyChunk(s.messageOffset))
		}
	}

	s.close(closeError)
}

func (s *SegmentReader) readAsFollower() {
	// TODO: Implement
	s.read()
}

func (s *SegmentReader) storeOffset(lastCommit *time.Time) {
	commitType := OffsetCommitLocal
	if time.Since(*lastCommit) >= s.config.AutoCommitInterval() {
		*lastCommit = time.Now()
		commitType = OffsetCommitAll
	} else if s.MaxProducedOffset != nil && s.messageOffset >= *s.MaxProducedOffset {
		log.Debug().Str("group", s.group).Msgf("Consumed all messages of a previous generation %s", &s.Topic)
		commitType = OffsetCommitAll
	}

	value := Offset{
		Offset:  s.messageOffset,
		Version: s.Topic.Version,
		Source:  s.SourceVersion,
	}

	if commitType == OffsetCommitAll {
		log.Debug().Str("group", s.group).Msgf("Setting offset for %s on all replicas", &s.Topic)
	}

	s.offsetState.Set(s.group, s.Topic.Name, s.Topic.Token, s.Topic.RangeIndex, value, commitType)
}

// Tries open the initial file and seek the correct position, returning an error when there's an
// I/O-related error
func (s *SegmentReader) initRead(foreground bool) error {
	foundFileName, fileOffset, err := s.fullSeek(foreground)
	if err != nil {
		return err
	}

	if foundFileName == "" {
		// No file found on folder, will attempt later
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
func (s *SegmentReader) fullSeek(foreground bool) (string, int64, error) {
	pattern := fmt.Sprintf("%s/*.%s", s.basePath, conf.SegmentFileExtension)
	if !s.isLeader {
		if foreground {
			// Avoid blocking when creating a reader
			return "", 0, nil
		}

		if done, err := s.setStructureAsFollower(); err != nil {
			return "", 0, err
		} else if !done {
			return "", 0, nil
		}
	}

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
		startOffset, err := strconv.ParseInt(filePrefix, 10, 64)
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

func (s *SegmentReader) setStructureAsFollower() (bool, error) {
	// The path might not exist
	if err := os.MkdirAll(s.basePath, DirectoryPermissions); err != nil {
		return false, err
	}

	return s.replicationReader.MergeFileStructure()
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

// Returns the number of bytes read since index, only returning an error when there's something wrong
// with the file descriptor (not on EOF)
func (s *SegmentReader) pollFile(buffer []byte) (int, error) {
	// buffer might not be a multiple of alignmentSize
	bytesToAlign := len(buffer) % alignmentSize
	if bytesToAlign > 0 {
		// Crop the last bytes to make to compatible with DIRECT I/O
		buffer = buffer[:len(buffer)-bytesToAlign]
	}

	n, err := s.segmentFile.Read(buffer)

	if n != 0 {
		return n, nil
	}

	if err == io.EOF {
		return 0, nil
	}

	// There was an error related to either permission change / file not found
	// or file descriptor closed by the OS
	message := fmt.Sprintf("Unexpected error reading file %s in %s", s.fileName, s.basePath)
	log.Err(err).Msg(message)
	return 0, fmt.Errorf(message)
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

func (s *SegmentReader) serveChunk(reader *bytes.Reader, item ReadItem) bool {
	var chunk SegmentChunk = nil
	for {
		chunk = s.readChunk(reader)
		if chunk == nil {
			return false
		}

		if chunk.StartOffset() >= s.messageOffset {
			// Skip chunks served in another session (failover / restarts / ...)
			break
		}
	}

	item.SetResult(nil, chunk)
	s.messageOffset = chunk.StartOffset() + int64(chunk.RecordLength())
	return true
}

// Returns a non-nil chunk when there was a full chunk in the reader bytes.
// There may be remaining
func (s *SegmentReader) readChunk(reader *bytes.Reader) SegmentChunk {
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

	header, err := s.readHeader(reader)
	// TODO: Support moving forward
	utils.PanicIfErr(err, "CRC validation failed")

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
		length: header.RecordLength,
	}
	return chunk
}

func (s *SegmentReader) readHeader(reader *bytes.Reader) (*chunkHeader, error) {
	header := &chunkHeader{}
	_, err := reader.Read(s.headerBuf)
	utils.PanicIfErr(err, "Unexpected EOF when reading chunk header")

	// The checksum is in the last position of the header
	expectedChecksum := crc32.ChecksumIEEE(s.headerBuf[:chunkHeaderSize-4])

	err = binary.Read(bytes.NewReader(s.headerBuf), conf.Endianness, header)
	utils.PanicIfErr(err, "Unexpected EOF when reading chunk header from new reader")

	if expectedChecksum != header.Crc {
		return nil, fmt.Errorf("Checksum mismatch")
	}

	if header.Start < 0 {
		return nil, fmt.Errorf("Invalid length")
	}

	return header, nil
}
