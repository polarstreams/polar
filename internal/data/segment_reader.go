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
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type SegmentReader struct {
	Items                 chan ReadItem
	basePath              string
	headerBuf             []byte
	config                conf.DatalogConfig
	group                 string
	isLeader              bool // Determines whether the current broker was the leader of the generation we are reading from
	replicationReader     ReplicationReader
	Topic                 TopicDataId
	SourceVersion         GenId // The version in which this reader was created, a consumer might be on Gen=v3 but the current is v4. In this case, source would be v4 and topic.Version = v3
	offsetState           OffsetState
	MaxProducedOffset     *int64 // When set, it determines the last offset produced for this topicId for an old generation, inclusive
	messageOffset         int64  // The expected next message offset, e.g. "0" when no message was read; "10" when 0-9 were read
	fileName              string
	segmentFile           *os.File
	lastChunkFilePosition int64 // The file offset where the last chunk starts
	filePosition          int64
	skipFromFile          int64 // The number of bytes to skip after reading from file (to seek with alignment)
	readingFromReplica    bool
	lastFullSeek          int64
}

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
	}

	s.read()
}

func (s *SegmentReader) read() {
	buf := make([]byte, s.config.ReadAheadSize()) // Reusable buffer across calls
	reader := bytes.NewReader(emptyBuffer)        // The reader that gets assigned the buffer slice from disk
	var closeError error
	lastCommit := &time.Time{}
	nextFileName := ""
	offsetGap := int64(-1) // The last message offset (inclusive) of the missing messages range: [s.messageOffset, offsetGap]
	var lastOrigin *uuid.UUID

	for item := range s.Items {
		shouldResetLastCommitted := lastOrigin != nil && *lastOrigin != item.Origin()
		if shouldResetLastCommitted {
			if item.CommitOnly() {
				// Wants to commit but it wasn't the last reader
				item.SetResult(fmt.Errorf("Manual commit was ignored"), nil)
				continue
			}
			if s.resetOffsetToLastCommitted() {
				reader.Reset(emptyBuffer)
			}
		} else {
			s.storeOffset(lastCommit, item.CommitOnly())
			if item.CommitOnly() {
				item.SetResult(nil, NewEmptyChunk(s.messageOffset))
				continue
			}
		}

		origin := item.Origin()
		lastOrigin = &origin
		remainderIndex := 0
		var chunk SegmentChunk

		if reader.Len() > 0 {
			// Consume all the read-ahead data
			var gap int64
			chunk, gap = s.consumeReadAhead(reader, buf, &remainderIndex)

			if gap >= 0 {
				offsetGap = gap // There's a gap in the current buffer, override
			}
		}

		if chunk != nil {
			item.SetResult(nil, chunk)
			continue
		}

		item.SetResult(nil, NewEmptyChunk(s.messageOffset))

		if s.handleFileGap(&offsetGap, reader, buf) {
			continue
		}

		if s.segmentFile == nil {
			// Segment file might be nil when there was no data initially
			s.initRead(false)
			if s.segmentFile == nil {
				continue
			}
		}

		readBuffer, err := s.pollFile(buf, remainderIndex)

		if err != nil {
			// TODO: Determine what to do in case of error
			closeError = err
			log.Err(err).Msgf("Error while reading file %s/%s", s.basePath, s.fileName)
		}

		if len(readBuffer)-remainderIndex <= 0 {
			// There's no new data in this file, check whether we have finished
			if nextFileName == "" {
				nextFileName, offsetGap = s.checkNextFile()
			} else {
				// We've polled the previous file after discovering a new one
				// We can safely switch the active file
				s.swapSegmentFile(&nextFileName)
			}
			continue
		}

		reader.Reset(readBuffer)
	}

	s.close(closeError)
}

func (s *SegmentReader) consumeReadAhead(reader *bytes.Reader, buf []byte, remainderIndex *int) (SegmentChunk, int64) {
	offsetGap := int64(-1)
	chunk := s.readChunk(reader, &offsetGap)

	if chunk == nil && reader.Len() > 0 {
		// There isn't enough data remaining for a chunk.
		*remainderIndex = reader.Len()
		// Drain the remaining reader and copy the bytes to the beginning of the buffer
		reader.Read(buf)
	}

	return chunk, offsetGap
}

// Returns true when there was a file gap and it attempted read from a replica
func (s *SegmentReader) handleFileGap(offsetGap *int64, reader *bytes.Reader, buf []byte) bool {
	gap := *offsetGap
	if gap >= 0 {
		if s.messageOffset <= gap {
			log.Debug().Msgf("Handling file gap in %s/%s with the range [%d, %d]", s.basePath, s.fileName, s.messageOffset, gap)
			s.readingFromReplica = true

			segmentId := conf.SegmentIdFromName(s.fileName)
			maxRecords := int(s.messageOffset-gap) + 1

			if s.replicationReader == nil {
				log.Panic().
					Msgf("No replication reader found for %s for file gap in %s/%s", &s.Topic, s.basePath, s.fileName)
			}

			// Read into buffer from peer
			n, err := s.replicationReader.StreamFile(segmentId, &s.Topic, s.messageOffset, maxRecords, buf)
			if err != nil {
				log.Err(err).Msgf("File %s/%s could not be read from replicas", s.basePath, s.fileName)
			}

			log.Debug().Msgf("Obtained %d bytes from peer for file gap %s/%s", n, s.basePath, s.fileName)
			reader.Reset(buf[:n])

			return true
		}

		*offsetGap = -1
		s.readingFromReplica = false

		// Reset file position: aligned seek
		s.skipFromFile = s.lastChunkFilePosition % alignmentSize
		fileOffset := s.lastChunkFilePosition - s.skipFromFile // Align position

		log.Info().Msgf("Seeking position %d of file %s/%s after gap", fileOffset, s.basePath, s.fileName)
		if _, err := s.segmentFile.Seek(fileOffset, io.SeekStart); err != nil {
			log.Err(err).Msgf("Could not seek position in %s in %s", s.fileName, s.basePath)
		}
	}

	return false
}

func (s *SegmentReader) storeOffset(lastCommit *time.Time, manual bool) {
	commitType := OffsetCommitLocal
	value := Offset{
		Offset:  s.messageOffset,
		Version: s.Topic.Version,
		Source:  s.SourceVersion,
	}

	if time.Since(*lastCommit) >= s.config.AutoCommitInterval() || manual {
		*lastCommit = time.Now()
		commitType = OffsetCommitAll
	} else if s.MaxProducedOffset != nil && s.messageOffset >= *s.MaxProducedOffset {
		log.Debug().Str("group", s.group).Msgf("Consumed all messages of a previous generation %s", &s.Topic)
		commitType = OffsetCommitAll
		value.Offset = OffsetCompleted
	}

	if commitType == OffsetCommitAll {
		log.Debug().Str("group", s.group).Msgf("Setting offset for %s on all replicas: %s", &s.Topic, &value)
	}

	s.offsetState.Set(s.group, s.Topic.Name, s.Topic.Token, s.Topic.RangeIndex, value, commitType)
}

// Rewinds to the last known committed offset
func (s *SegmentReader) resetOffsetToLastCommitted() bool {
	if s.segmentFile == nil {
		// It was not initialized yet, it's OK
		return false
	}

	offset := s.offsetState.Get(s.group, s.Topic.Name, s.Topic.Token, s.Topic.RangeIndex)
	if offset == nil {
		offset = &Offset{
			Offset:  0,
			Version: s.Topic.Version,
			Source:  s.SourceVersion,
		}
	}

	if offset.Version != s.Topic.Version {
		log.Error().Msgf("Unexpected offset version for group %s and topic %s", s.group, &s.Topic)
		offset = &Offset{
			Offset:  0,
			Version: s.Topic.Version,
			Source:  s.SourceVersion,
		}
	}

	s.segmentFile = nil
	s.messageOffset = offset.Offset
	return true
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
		log.Info().Msgf("Started reading file %s/%s from position 0", s.basePath, foundFileName)
	}

	return nil
}

func (s *SegmentReader) open(fileName string) error {
	file, err := os.OpenFile(filepath.Join(s.basePath, fileName), conf.SegmentFileReadFlags, 0)
	if err != nil {
		log.Err(err).Msgf("File %s in %s could not be opened by reader", fileName, s.basePath)
		return err
	}
	s.segmentFile = file
	s.fileName = fileName
	s.lastChunkFilePosition = 0
	s.filePosition = 0
	s.skipFromFile = 0
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

	// Avoid spamming logs
	shouldLog := time.Since(time.UnixMilli(atomic.LoadInt64(&s.lastFullSeek))) > 1*time.Minute
	if shouldLog {
		log.Info().Msgf("Looking for files matching the pattern %s", pattern)
		atomic.StoreInt64(&s.lastFullSeek, time.Now().UnixMilli())
	}

	entries, err := filepath.Glob(pattern)
	if err != nil {
		log.Err(err).Msgf("There was an error listing files in %s while seeking", s.basePath)
		return "", 0, err
	}

	if len(entries) == 0 {
		if shouldLog {
			log.Info().Msgf("Reader could not find any files in %s", s.basePath)
		}
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

// Returns the name of the file after the current one or an empty string,
// along with offset representing the gap (last message offset inclusive) missing in the local file system
func (s *SegmentReader) checkNextFile() (string, int64) {
	entries, err := filepath.Glob(fmt.Sprintf("%s/*.%s", s.basePath, conf.SegmentFileExtension))
	if err != nil {
		log.Err(err).Msgf("There was an error listing files in %s checking for next file", s.basePath)
		return "", -1
	}

	sort.Strings(entries)
	foundCurrent := false
	nextFileName := ""
	offsetGap := int64(-1)

	for _, entry := range entries {
		fileName := filepath.Base(entry)
		if foundCurrent {
			// Check file pattern
			nameWithoutExt := fileName[:len(fileName)-len(conf.SegmentFileExtension)-1]
			segmentId, err := strconv.ParseInt(nameWithoutExt, 10, 64)
			if err != nil {
				// The filename is invalid, skip it
				continue
			}
			if segmentId > s.messageOffset {
				offsetGap = segmentId - 1
			}
			nextFileName = fileName
			break
		}

		if fileName == s.fileName {
			foundCurrent = true
		}
	}

	if offsetGap >= 0 {
		return "", offsetGap
	}

	if nextFileName != "" {
		return nextFileName, -1
	}

	if s.MaxProducedOffset != nil && s.messageOffset <= *s.MaxProducedOffset {
		// There's an expected file that was not found
		return "", *s.MaxProducedOffset
	}

	return "", -1
}

// Closes the previous segment file and opens the new one, setting is as the current one.
func (s *SegmentReader) swapSegmentFile(nextFileName *string) {
	previousFile := s.segmentFile
	err := s.open(*nextFileName)
	*nextFileName = ""

	if err != nil {
		log.Err(err).Msgf("Next file could not be opened")
		return
	}

	if err := previousFile.Close(); err != nil {
		log.Warn().Err(err).Msgf("There was an error when closing file in %s", s.basePath)
	}
}

// Returns the number of bytes read since index, only returning an error when there's something wrong
// with the file descriptor (not on EOF).
//
// Direct I/O alignment requirement makes logic harder to follow
func (s *SegmentReader) pollFile(buf []byte, remainderIndex int) ([]byte, error) {
	fileBuffer, alignOffset := alignBuffer(buf[remainderIndex:])
	n, err := s.segmentFile.Read(fileBuffer)

	// Ignore EOF error
	if err != nil && err != io.EOF {
		// There was an error related likely either permission change / file not found
		// or file descriptor closed by the OS
		message := fmt.Sprintf("Unexpected error reading file %s in %s", s.fileName, s.basePath)
		log.Err(err).Msg(message)
		return nil, fmt.Errorf(message)
	}

	if remainderIndex > 0 {
		// Move the initial bytes to the position before aligned offset
		copy(buf[alignOffset:], buf[0:remainderIndex])
	}

	// Create a slice from alignOffset+remainderIndex-remainderIndex
	result := buf[alignOffset : remainderIndex+alignOffset+n]

	if s.skipFromFile > 0 {
		// There was a previous seek that needed to be aligned
		skip := s.skipFromFile
		s.skipFromFile = 0
		if skip <= int64(n) {
			return result[skip:], nil
		}
	}
	return result, nil
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

// Reads the following chunks until finding the one with expected start offset
func (s *SegmentReader) readChunk(reader *bytes.Reader, offsetGap *int64) SegmentChunk {
	var chunk SegmentChunk = nil

	// Skip chunks served in another session (failover / restarts / ...)
	for {
		var n int
		initialFilePosition := s.filePosition
		n, chunk = s.readSingleChunk(reader)

		if !s.readingFromReplica {
			s.filePosition += int64(n)
		}
		if chunk == nil {
			return nil
		}

		// Store the last known position of a valid chunk
		if !s.readingFromReplica {
			s.lastChunkFilePosition = initialFilePosition
		}

		if chunk.StartOffset() > s.messageOffset {
			// There's a gap in the file, set offsetGap to the last message offset missing
			*offsetGap = chunk.StartOffset() - 1
			return nil
		}

		if chunk.StartOffset() == s.messageOffset {
			break
		}
	}

	s.messageOffset = chunk.StartOffset() + int64(chunk.RecordLength())
	return chunk
}

// Returns a non-nil chunk when there was a full chunk in the reader bytes.
// There may be remaining
func (s *SegmentReader) readSingleChunk(reader *bytes.Reader) (int, SegmentChunk) {
	// Peek the next chunk flags for alignment
	n := 0
	for {
		flag, err := reader.ReadByte()
		if err == io.EOF {
			return n, nil
		}
		utils.PanicIfErr(err, "Unexpected error when reading chunk header")
		if flag != alignmentFlag {
			// It's a valid header
			_ = reader.UnreadByte()
			break
		}
		n++
	}

	if reader.Len() < chunkHeaderSize {
		return n, nil
	}

	header, err := readChunkHeader(reader, s.headerBuf)
	// TODO: Support moving forward for corrupted files
	utils.PanicIfErr(err, "CRC validation failed")

	if reader.Len() < int(header.BodyLength) {
		// Rewind to the header position
		reader.Seek(-int64(chunkHeaderSize), io.SeekCurrent)
		return n, nil
	}

	n += chunkHeaderSize

	// TODO: read buffer pooling
	readBuffer := make([]byte, int(header.BodyLength))
	nBody, _ := reader.Read(readBuffer)
	n += nBody

	chunk := &ReadSegmentChunk{
		buffer: readBuffer,
		start:  header.Start,
		length: header.RecordLength,
	}
	return n, chunk
}

func readChunkHeader(reader *bytes.Reader, buf []byte) (*chunkHeader, error) {
	header := &chunkHeader{}
	_, err := reader.Read(buf)
	utils.PanicIfErr(err, "Unexpected EOF when reading chunk header")

	// The checksum is in the last position of the header
	expectedChecksum := crc32.ChecksumIEEE(buf[:chunkHeaderSize-4])

	err = binary.Read(bytes.NewReader(buf), conf.Endianness, header)
	utils.PanicIfErr(err, "Unexpected EOF when reading chunk header from new reader")

	if expectedChecksum != header.Crc {
		return nil, fmt.Errorf("Checksum mismatch")
	}

	if header.Start < 0 {
		return nil, fmt.Errorf("Invalid length")
	}

	return header, nil
}
