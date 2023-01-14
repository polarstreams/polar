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
	io.Reader
	ReadUint8() (uint8, error)
	ReadUint32() (uint32, error)
	ReadUint64() (uint64, error)
	ReadString(length int) (string, error)
	ReadStringBytes() (string, error)

	// Returns the unread portion of the buffers and the total length
	Bytes() ([][]byte, int)
}

// Returns a reader that wraps multiple []byte buffers and tries to read without allocating new buffers.
func NewMultiBufferReader(buffers [][]byte, bufferSize int, length int) MultiBufferReader {
	mod := length % bufferSize
	if mod > 0 {
		lastBufferIndex := len(buffers) - 1
		// Crop the last buffer
		buffers[lastBufferIndex] = buffers[lastBufferIndex][:mod]
	}

	return &multiBufferReader{
		buffers:     buffers,
		bufferIndex: 0,
		index:       0,
	}
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

func (r *multiBufferReader) ReadUint8() (uint8, error) {
	buf, err := r.getBuffer(1)
	if err != nil {
		return 0, err
	}

	return buf[0], nil
}

func (r *multiBufferReader) ReadString(length int) (string, error) {
	buf, err := r.getBuffer(length)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (r *multiBufferReader) ReadStringBytes() (string, error) {
	length, err := r.ReadUint8()
	if err != nil {
		return "", err
	}

	return r.ReadString(int(length))
}

func (r *multiBufferReader) Bytes() ([][]byte, int) {
	remainingLength := len(r.buffers) - r.bufferIndex
	if remainingLength == 0 {
		return [][]byte{}, 0
	}
	result := make([][]byte, remainingLength)
	result[0] = r.buffers[r.bufferIndex][r.index:]
	totalLength := len(result[0])
	for i := 1; i < remainingLength; i++ {
		buf := r.buffers[r.bufferIndex+i]
		result[i] = buf
		totalLength += len(buf)
	}
	if len(result[0]) == 0 {
		return result[1:], totalLength
	}
	return result, totalLength
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
func (r *multiBufferReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Don't return 0 without an error
	buf := r.getNextBuffer(len(p))
	if len(buf) == 0 {
		return 0, io.EOF
	}
	copy(p, buf)
	return len(buf), nil
}

// Returns unread portion, one buffer at a time, advancing the indices.
// An empty buffer signals EOF.
func (r *multiBufferReader) getNextBuffer(length int) []byte {
	if r.bufferIndex >= len(r.buffers) {
		return nil
	}

	buf := r.buffers[r.bufferIndex][r.index:]
	if len(buf) == 0 {
		r.bufferIndex++
		r.index = 0
		if r.bufferIndex >= len(r.buffers) {
			return nil
		}
		buf = r.buffers[r.bufferIndex]
	}

	if len(buf) > length {
		buf = buf[:length]
	}
	r.index += len(buf)
	return buf
}

func (r *multiBufferReader) getBuffer(length int) ([]byte, error) {
	if r.bufferIndex >= len(r.buffers) {
		return nil, io.EOF
	}
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
