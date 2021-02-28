package types

type HttpError interface {
	error
	StatusCode() int
}

func NewHttpError(code int, message string) HttpError {
	return &httpError{code, message}
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
