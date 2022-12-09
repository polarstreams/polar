package types

import "github.com/polarstreams/polar/internal/metrics"

type FlowController interface {
	// Allocate makes sure that buffer memory usage remains within the predefined boundaries.
	Allocate(length int)

	// Marks the provided length as ready to be allocated.
	Free(length int)
}

type flowControl struct {
	requestChannel chan allocRequest
	freeChannel    chan int
	maxLength      int
	remaining      int
}

type allocRequest struct {
	length int
	sender chan<- bool
}

func NewFlowControl(maxLength int) FlowController {
	f := &flowControl{
		requestChannel: make(chan allocRequest),
		freeChannel:    make(chan int, 1024),
		maxLength:      maxLength,
		remaining:      maxLength,
	}

	metrics.AllocationPoolAvailableBytes.Set(float64(f.remaining))

	go f.startReceiving()

	return f
}

// Receives in loop
func (f *flowControl) startReceiving() {
	// requestChannel blocks until the next request
	for r := range f.requestChannel {
		// Receive all pending without blocking
		f.receiveFreed()

		metrics.AllocationPoolAvailableBytes.Set(float64(f.remaining))

		// Wait until we get enough space
		f.ensureLength(r.length)

		f.remaining -= r.length
		metrics.AllocationPoolAvailableBytes.Set(float64(f.remaining))

		r.sender <- true

		// Continue receiving before possibly blocking
		f.receiveFreed()
	}
}

func (f *flowControl) receiveFreed() {
	hasItems := true
	for hasItems {
		select {
		case length := <-f.freeChannel:
			f.remaining += length
		default:
			// receiving would block
			hasItems = false
		}
	}
}

func (f *flowControl) ensureLength(length int) {
	for f.remaining < length {
		f.remaining += <-f.freeChannel
	}
}

func (f *flowControl) Allocate(length int) {
	response := make(chan bool, 1)
	r := allocRequest{
		length: length,
		sender: response,
	}
	f.requestChannel <- r
	<-response
}

func (f *flowControl) Free(length int) {
	f.freeChannel <- length
}
