package data

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog/log"
)

const (
	DirectoryPermissions os.FileMode = 0755
	FilePermissions      os.FileMode = 0644
)

const streamBufferLength = 2 // Amount of buffers for file streaming

type Datalog interface {
	Initializer

	// Seeks the position and fills the buffer with chunks until maxSize or maxRecords is reached.
	// Opens and close the file handle. It may issue several reads to reach to the position.
	ReadFileFrom(
		buf []byte,
		maxSize int,
		segmentId int64,
		startOffset int64,
		maxRecords int,
		topic *TopicDataId,
	) ([]byte, error)

	// Blocks until there's an available buffer to be used to stream.
	// After use, it should be released
	StreamBuffer() []byte

	// Releases the stream buffer
	ReleaseStreamBuffer(buf []byte)
}

func NewDatalog(config conf.DatalogConfig) Datalog {
	streamBufferChan := make(chan []byte, 2)
	// Add a couple of buffers by default
	for i := 0; i < streamBufferLength; i++ {
		streamBufferChan <- make([]byte, config.StreamBufferSize())
	}

	return &datalog{
		config:           config,
		streamBufferChan: streamBufferChan,
	}
}

type datalog struct {
	config           conf.DatalogConfig
	streamBufferChan chan []byte
}

func (d *datalog) Init() error {
	return nil
}

func (d *datalog) StreamBuffer() []byte {
	return <-d.streamBufferChan
}

func (d *datalog) ReleaseStreamBuffer(buf []byte) {
	d.streamBufferChan <- buf
}

func (d *datalog) ReadFileFrom(
	buf []byte,
	maxSize int,
	segmentId int64,
	startOffset int64,
	maxRecords int,
	topic *TopicDataId,
) ([]byte, error) {
	basePath := d.config.DatalogPath(topic)
	fileOffset := tryReadIndexFile(basePath, conf.SegmentFilePrefix(segmentId), startOffset)
	fileName := conf.SegmentFileName(segmentId)

	if maxSize < len(buf) {
		buf = buf[:maxSize]
	}

	file, err := os.OpenFile(filepath.Join(basePath, fileName), conf.SegmentFileReadFlags, 0)
	if err != nil {
		log.Err(err).Msgf("Could not open file %s/%s", basePath, fileName)
		return nil, err
	}
	defer file.Close()

	if fileOffset > 0 {
		if _, err := file.Seek(fileOffset, io.SeekStart); err != nil {
			log.Err(err).Msgf("Could not seek position in file %s/%s", basePath, fileName)
			return nil, err
		}
	}

	remainderIndex := 0

	// read chunks until a segment containing startOffset is found
	for {
		readBuf, alignOffset := alignBuffer(buf[remainderIndex:])
		n, err := file.Read(readBuf)
		if err != nil && err != io.EOF {
			log.Err(err).Msgf("Could not read file %s/%s", basePath, fileName)
			return nil, err
		}

		totalRead := remainderIndex + n
		if totalRead < chunkHeaderSize {
			return nil, nil
		}

		if remainderIndex > 0 {
			// Move the initial bytes to the position before aligned offset
			copy(buf[alignOffset:], buf[0:remainderIndex])
		}

		// Create a slice from alignOffset+remainderIndex-remainderIndex
		dataBuf := buf[alignOffset : remainderIndex+alignOffset+n]

		if chunksBuf, completeChunk, err := readChunksUntil(dataBuf, startOffset, maxRecords); err != nil {
			log.Err(err).Msgf("Error reading chunks in file %s/%s", basePath, fileName)
			return nil, err
		} else if completeChunk {
			return chunksBuf, nil
		} else {
			// Partial chunk
			copy(buf, chunksBuf)
			remainderIndex = len(chunksBuf)
		}

		if err == io.EOF {
			return nil, nil
		}
	}
}

// Returns a slice of the given buffer containing the chunks when completed is true.
// When completed is false, it returns the remaining
func readChunksUntil(buf []byte, startOffset int64, maxRecords int) ([]byte, bool, error) {
	headerBuf := make([]byte, chunkHeaderSize)
	for len(buf) > 0 {
		header, alignment, err := readNextChunk(buf, headerBuf)
		if err != nil {
			return nil, false, err
		}

		if header == nil {
			// Incomplete chunk
			return buf, false, nil
		}

		if startOffset >= header.Start && startOffset < header.Start+int64(header.RecordLength) {
			// We found the starting chunk
			end := 0
			maxOffset := startOffset + int64(maxRecords) - 1
			initialAlignment := alignment
			for {
				end += alignment + chunkHeaderSize + int(header.BodyLength)
				header, alignment, _ = readNextChunk(buf[end:], headerBuf)
				if header == nil || header.Start > maxOffset {
					// Either there is no next chunk in buffer
					// Or the next chunk is not needed to be returned
					break
				}
			}
			return buf[initialAlignment:end], true, nil
		}

		// Skip the chunk
		buf = buf[alignment+chunkHeaderSize+int(header.BodyLength):]
	}
	return nil, false, nil
}

// Returns a header when it's contained in buf, otherwise it returns nil
func readNextChunk(buf []byte, headerBuf []byte) (*chunkHeader, int, error) {
	alignment := 0
	for i := 0; i < len(buf); i++ {
		if buf[i] != alignmentFlag {
			break
		}
		alignment++
	}

	buf = buf[alignment:]

	if chunkHeaderSize > len(buf) {
		// Incomplete header
		return nil, 0, nil
	}

	// Read through the buffer
	header, err := readChunkHeader(bytes.NewReader(buf), headerBuf)
	if err != nil {
		return nil, 0, err
	}

	if int(header.BodyLength)+chunkHeaderSize > len(buf) {
		// Incomplete chunk
		return nil, 0, nil
	}

	return header, alignment, nil
}

// Gets the address offset of the buffer relative to the alignment
func addressAlignment(buf []byte) int {
	return int(uintptr(unsafe.Pointer(&buf[0])) & uintptr(alignmentSize-1))
}

// Creates an aligned buffer of at least "length" in size
func makeAlignedBuffer(length int) []byte {
	// Align initial length
	length = length - (length % alignmentSize)
	buf := make([]byte, length+alignmentSize)

	// Align address
	index := addressAlignment(buf)
	offset := 0
	if index != 0 {
		offset = alignmentSize - index
	}
	buf = buf[offset : offset+length]
	if addressAlignment(buf) != 0 {
		panic("Failed to align buffer")
	}
	return buf
}

// Creates a bytes.Buffer backed by an aligned buffer with initial length zero.
func createAlignedByteBuffer(capacity int) *bytes.Buffer {
	buf := makeAlignedBuffer(capacity)
	return bytes.NewBuffer(buf[:0])
}

// Aligns an existing buffer moving the index and length of the buffer.
// Returns the new slice and the offset that was moved from the original.
func alignBuffer(buf []byte) ([]byte, int) {
	index := addressAlignment(buf)
	offset := 0
	if index != 0 {
		offset = alignmentSize - index
	}
	length := len(buf)
	endRem := (length - offset) % alignmentSize
	end := length - endRem
	buf = buf[offset:end]

	if addressAlignment(buf) != 0 {
		panic("Failed to align buffer")
	}
	return buf, offset
}
