package producing

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/metrics"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
)

// Set of buffers used to coalesce and write the records
type coalescerBuffers struct {
	group      [writeConcurrencyLevel]*bytes.Buffer
	compressor [writeConcurrencyLevel]*zstd.Encoder
}

// Represents one or more records depending on the format
type recordItem struct {
	replication ReplicationInfo
	length      uint32 // Body length
	timestamp   int64  // Timestamp in micros
	contentType string // The content type detailing the format of the
	buffers     [][]byte
	response    chan error
}

func (r *recordItem) marshal(w io.Writer) (totalRecords int, err error) {
	if r.contentType == MIMETypeNDJSON {
		return r.marshalRecordsByLine(w, r.length)
	}

	if r.contentType == MIMETypeProducerBinary {
		return r.marshalFramedRecords(w, r.length)
	}

	return 1, marshalRecord(w, r.timestamp, r.length, r.buffers)
}

func (r *recordItem) marshalRecordsByLine(w io.Writer, length uint32) (totalRecords int, err error) {
	recordBodyLength := 0
	recordBody := make([][]byte, 0, len(r.buffers))

	// keep track of remaining, r.buffers might contain more bytes than the actual message length
	totalMessageRemaining := int(length)

	for _, rawBuf := range r.buffers {
		n := len(rawBuf)
		if totalMessageRemaining < n {
			n = totalMessageRemaining
		}
		totalMessageRemaining -= n

		// Use only the part of the buffer that can be read (rawBuf can be over-dimensioned)
		buf := rawBuf[0:n]
		index := 0

		for index < len(buf) {
			advance, token, _ := bufio.ScanLines(buf[index:], true)
			index += advance
			if len(token) > 0 {
				recordBody = append(recordBody, token)
				recordBodyLength += len(token)
			}
			if index < len(buf) && recordBodyLength > 0 {
				if err := marshalRecord(w, r.timestamp, uint32(recordBodyLength), recordBody); err != nil {
					return totalRecords, err
				}
				totalRecords++
				recordBody = recordBody[:0]
				recordBodyLength = 0
			}
		}
	}

	if recordBodyLength > 0 {
		totalRecords++
		if err := marshalRecord(w, r.timestamp, uint32(recordBodyLength), recordBody); err != nil {
			return totalRecords, err
		}
	}
	return
}

func (r *recordItem) marshalFramedRecords(w io.Writer, length uint32) (totalRecords int, err error) {
	bufferIndex := 0
	index := 0
	totalRead := 0
	for totalRead < int(length) {
		recordLength := readUint32(r.buffers, &bufferIndex, &index)
		totalRead += 4
		if err := marshalRecordProps(r.timestamp, recordLength, w); err != nil {
			return totalRecords, err
		}
		totalRecords++
		remaining := int(recordLength)
		for remaining > 0 {
			buf := r.buffers[bufferIndex][index:]
			n := len(buf)
			if remaining < n {
				n = remaining
			}
			remaining -= n

			if err := utils.WriteBytes(w, buf[0:n]); err != nil {
				return totalRecords, err
			}
			if remaining > 0 {
				index = 0
				bufferIndex++
			} else {
				index += n
			}
		}
		totalRead += int(recordLength)
	}
	return totalRecords, nil
}

func marshalRecord(w io.Writer, timestamp int64, length uint32, buffers [][]byte) error {
	if err := marshalRecordProps(timestamp, length, w); err != nil {
		return err
	}

	// keep track of remaining, r.buffers might contain more bytes than the actual message length
	remaining := int(length)

	// Don't io.CopyBuffer() to avoid zstd.Encoder's ReadFrom() implementation
	for _, buf := range buffers {
		n := len(buf)
		if remaining < n {
			n = remaining
		}
		remaining -= n
		if err := utils.WriteBytes(w, buf[0:n]); err != nil {
			return err
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

func readUint32(buffers [][]byte, bufferIndex *int, index *int) uint32 {
	buf := buffers[*bufferIndex][*index:]
	if len(buf) < 4 {
		*bufferIndex++
		*index = 4 - len(buf)
		buf = append(buf, buffers[*bufferIndex]...)
	} else {
		*index += 4
	}
	return conf.Endianness.Uint32(buf)
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
