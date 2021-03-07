package types

import "fmt"

type HttpError interface {
	error
	StatusCode() int
}

func NewHttpError(code int, message string) HttpError {
	return &httpError{code, message}
}

func NewHttpErrorf(code int, message string, a ...interface{}) HttpError {
	return &httpError{code, fmt.Sprintf(message, a...)}
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
