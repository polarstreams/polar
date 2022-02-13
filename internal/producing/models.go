package producing

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/klauspost/compress/zstd"
)

type record struct {
	replication ReplicationInfo
	length      uint32 // Body length
	timestamp   int64  // Timestamp in micros
	body        io.ReadCloser
	offset      uint64 // Record offset
	response    chan error
}

func (r *record) marshal(w io.Writer) error {
	if err := binary.Write(w, conf.Endianness, r.timestamp); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, r.length); err != nil {
		return err
	}
	if _, err := io.Copy(w, r.body); err != nil {
		return err
	}
	return nil
}

type buffers struct {
	group      [writeConcurrencyLevel]*bytes.Buffer
	compressor [writeConcurrencyLevel]*zstd.Encoder
}
