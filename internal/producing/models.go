package producing

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/metrics"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// Set of buffers used to coalesce and write the records
type coalescerBuffers struct {
	group      [writeConcurrencyLevel]*bytes.Buffer
	compressor [writeConcurrencyLevel]*zstd.Encoder
	bodyReader []byte // Used to read from the body for line-separated bodies
}

// Represents one or more records depending on the format
type recordItem struct {
	replication ReplicationInfo
	length      uint32 // Body length
	timestamp   int64  // Timestamp in micros
	contentType string // The content type detailing the format of the
	body        io.Reader
	response    chan error
}

func (r *recordItem) marshal(w io.Writer, readerBuffer []byte) (totalRecords int, err error) {
	if r.contentType == ContentTypeNDJSON {
		return r.marshalRecordsByLine(w, readerBuffer)
	}

	return 1, marshalRecord(w, r.timestamp, r.length, r.body, readerBuffer)
}

func (r *recordItem) marshalRecordsByLine(w io.Writer, readerBuffer []byte) (totalRecords int, err error) {
	scanner := bufio.NewScanner(r.body)
	// Write chunks by chunks
	scanner.Buffer(readerBuffer, len(readerBuffer))

	for scanner.Scan() {
		length := len(scanner.Bytes())
		if length == 0 {
			continue
		}
		if err := marshalRecordProps(r.timestamp, uint32(length), w); err != nil {
			return 0, err
		}
		if err := utils.WriteBytes(w, scanner.Bytes()); err != nil {
			return 0, err
		}
		totalRecords++
	}

	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			log.Info().Msgf("Message size is greater than %d bytes", len(readerBuffer))
		}
		return 0, err
	}
	return
}

func marshalRecord(w io.Writer, timestamp int64, length uint32, body io.Reader, readBuffer []byte) error {
	if err := marshalRecordProps(timestamp, length, w); err != nil {
		return err
	}
	// Avoid using io.CopyBuffer() to avoid zstd.Encoder's ReadFrom() implementation
	for {
		n, err := body.Read(readBuffer)
		if n > 0 {
			if err = utils.WriteBytes(w, readBuffer[0:n]); err != nil {
				return err
			}
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}

func marshalRecordProps(timestamp int64, length uint32, w io.Writer) error {
	if err := binary.Write(w, conf.Endianness, timestamp); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, length); err != nil {
		return err
	}
	return nil
}

// Represents a group of `recordItem` that get compressed and written into a single chunk
type coalescerGroup struct {
	items        []recordItem
	offset       int64 // The start offset of the group
	byteSize     int64 // The total size in bytes of the group
	maxGroupSize int
}

func newCoalescerGroup(offset int64, maxGroupSize int) *coalescerGroup {
	return &coalescerGroup{
		items:        make([]recordItem, 0, 4),
		offset:       offset,
		byteSize:     0,
		maxGroupSize: maxGroupSize,
	}
}

func (g *coalescerGroup) sendResponse(err error) {
	for _, r := range g.items {
		r.response <- err
	}
}

// Attempts to add a new item to the group and returns nil when it was appended.
func (g *coalescerGroup) tryAdd(item *recordItem) *recordItem {
	itemSize := int64(item.length)
	if g.byteSize+itemSize > int64(g.maxGroupSize) {
		// Return a non-nil record as a signal that it was not appended
		return item
	}
	g.byteSize += itemSize
	g.items = append(g.items, *item)
	metrics.CoalescerMessagesProcessed.Inc()
	return nil
}
