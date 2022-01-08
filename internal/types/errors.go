package types

import "fmt"

type HttpError interface {
	error
	StatusCode() int
}

// Represents an error while producing that we are certain it caused side effect in the data store.
type ProducingError interface {
	error

	WasWriteAttempted() bool
}

func NewHttpError(code int, message string) HttpError {
	return &httpError{code, message}
}

func NewHttpErrorf(code int, message string, a ...interface{}) HttpError {
	return &httpError{code, fmt.Sprintf(message, a...)}
}

func NewNoWriteAttemptedError(message string, a ...interface{}) ProducingError {
	return &producingError{
		message:           fmt.Sprintf(message, a...),
		wasWriteAttempted: false,
	}
}

type httpError struct {
	code    int
	message string
}

func (e *httpError) Error() string {
	return e.message
}

func (e *httpError) StatusCode() int {
	return e.code
}

type producingError struct {
	message           string
	wasWriteAttempted bool
}

func (e *producingError) Error() string {
	return e.message
}

func (e *producingError) WasWriteAttempted() bool {
	return e.wasWriteAttempted
}
