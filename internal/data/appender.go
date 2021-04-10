package data

// Appender is responsible for creating topic segments and appending blocks to it
type Appender interface {
	Append(block []byte) error
}

type appender struct {
	topic string
}

func newAppender() Appender {
	return &appender{}
}

func (a *appender) Append(block []byte) error {
	return nil
}
