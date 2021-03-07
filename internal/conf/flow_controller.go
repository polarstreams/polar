package conf

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

func newFlowControl(maxLength int) *flowControl {
	f := &flowControl{
		requestChannel: make(chan allocRequest),
		freeChannel:    make(chan int, 1024),
		maxLength:      maxLength,
		remaining:      maxLength,
	}

	f.startReceiving()

	return f
}

func (f *flowControl) startReceiving() {
	go func() {
		// requestChannel blocks until the next request
		for r := range f.requestChannel {
			// Receive all pending without blocking
			f.receiveFreed()

			// Wait until we get enough space
			f.ensureLength(r.length)

			f.remaining -= r.length
			r.sender <- true

			// Continue receiving before possibly blocking
			f.receiveFreed()
		}
	}()
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

func (f *flowControl) allocate(length int) {
	response := make(chan bool, 1)
	r := allocRequest{
		length: length,
		sender: response,
	}
	f.requestChannel <- r
	<-response
}

func (f *flowControl) free(length int) {
	f.freeChannel <- length
}
