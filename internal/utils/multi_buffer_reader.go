package utils

import (
	"io"

	"github.com/polarstreams/polar/internal/conf"
)

type multiBufferReader struct {
	buffers     [][]byte
	bufferIndex int
	index       int
}

// Represents a reader that attempts to read without allocating new buffers.
type MultiBufferReader interface {
	ReadUint32() (uint32, error)
	ReadUint64() (uint64, error)
}

// Returns a reader that wraps multiple []byte buffers and tries to read without allocating new buffers.
func NewMultiBufferReader() MultiBufferReader {
	return &multiBufferReader{}
}

func (r *multiBufferReader) ReadUint32() (uint32, error) {
	buf, err := r.getBuffer(4)
	if err != nil {
		return 0, err
	}

	return conf.Endianness.Uint32(buf), nil
}

func (r *multiBufferReader) ReadUint64() (uint64, error) {
	buf, err := r.getBuffer(8)
	if err != nil {
		return 0, err
	}

	return conf.Endianness.Uint64(buf), nil
}

func (r *multiBufferReader) getBuffer(length int) ([]byte, error) {
	buf := r.buffers[r.bufferIndex][r.index:]
	if len(buf) >= length {
		r.index += length
		return buf[:length], nil
	}

	result := make([]byte, length)
	copied := copy(result, buf)
	for copied < length {
		r.bufferIndex++
		r.index = 0
		if r.bufferIndex >= len(r.buffers) {
			return nil, io.EOF
		}
		buf = r.buffers[r.bufferIndex]
		c := copy(result[copied:], buf)
		r.index += c
		copied += c
	}
	return result, nil
}
