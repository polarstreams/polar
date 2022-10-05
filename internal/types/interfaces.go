package types

import "io"

type Initializer interface {
	Init() error
}

type Closer interface {
	Close()
}

// Represents a writer backed by an in-memory buffer
type BufferBackedWriter interface {
	io.Writer
	io.StringWriter

	// Bytes returns a slice holding the unread portion of the internal buffer.
	// The slice aliases the buffer content at least until the next buffer modification.
	Bytes() []byte
}
