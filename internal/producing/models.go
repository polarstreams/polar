package producing

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
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
	offset      int64 // TODO: Remove
	response    chan error
}

func (r *recordItem) marshal(w io.Writer, readerBuffer []byte) error {
	if r.contentType == ContentTypeNDJSON {
		return r.marshalRecordsByLine(w, readerBuffer)
	}

	return marshalRecord(r.timestamp, r.length, r.body, w)
}

func (r *recordItem) marshalRecordsByLine(w io.Writer, readerBuffer []byte) error {
	scanner := bufio.NewScanner(r.body)
	// Write chunks by chunks
	scanner.Buffer(readerBuffer, len(readerBuffer))

	for scanner.Scan() {
		length := len(scanner.Bytes())
		if length == 0 {
			continue
		}
		err := marshalRecord(r.timestamp, uint32(length), bytes.NewReader(scanner.Bytes()), w)
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			log.Info().Msgf("Message size is greater than %d bytes", len(readerBuffer))
		}
		return err
	}
	return nil
}

func marshalRecord(timestamp int64, length uint32, body io.Reader, w io.Writer) error {
	if err := binary.Write(w, conf.Endianness, timestamp); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, length); err != nil {
		return err
	}
	if _, err := io.Copy(w, body); err != nil {
		return err
	}
	return nil
}
