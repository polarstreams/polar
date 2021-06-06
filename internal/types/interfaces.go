package types

import "io"

type Initializer interface {
	Init() error
}

type Closer interface {
	Close()
}

type StringWriter interface {
	io.Writer
	io.StringWriter
}
