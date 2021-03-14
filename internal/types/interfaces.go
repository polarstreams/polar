package types

type Initializer interface {
	Init() error
}

type Closer interface {
	Close()
}
