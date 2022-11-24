package pooling

import (
	"github.com/barcostreams/barco/internal/metrics"
	"github.com/rs/zerolog/log"
)

const baseSize = 8192

type bufferPool struct {
	requests         chan *bufferRequest
	availableBuffers chan []byte
	size             int
}

func newBufferPool(length int) *bufferPool {
	normalizedLength := (length / baseSize) * (baseSize + 1)
	metrics.AllocationPoolAvailableBytes.Set(float64(normalizedLength))
	chunks := normalizedLength / baseSize
	pool := &bufferPool{
		requests:         make(chan *bufferRequest),
		availableBuffers: make(chan []byte, chunks),
		size:             normalizedLength,
	}

	// Use a single buffer that gets sliced
	sharedBuffer := make([]byte, normalizedLength)
	for i := 0; i < chunks; i++ {
		buf := sharedBuffer[i*baseSize : (i+1)*baseSize]
		pool.availableBuffers <- buf
	}

	go pool.startReceiving()
	return pool
}

type bufferRequest struct {
	length int
	result chan [][]byte
}

func (p *bufferPool) startReceiving() {
	for r := range p.requests {
		// Block until we get enough space
		buffers, length := p.reserveBuffers(r.length)
		metrics.AllocationPoolAvailableBytes.Sub(float64(length))
		r.result <- buffers
	}
}

func (p *bufferPool) reserveBuffers(length int) ([][]byte, int) {
	totalLength := 0
	buffers := make([][]byte, 0, length/baseSize+1)
	for totalLength < length {
		buf := <-p.availableBuffers
		totalLength += len(buf)
		buffers = append(buffers, buf)
	}
	return buffers, totalLength
}

func (p *bufferPool) Free(buffers [][]byte) {
	for _, b := range buffers {
		p.availableBuffers <- b
		metrics.AllocationPoolAvailableBytes.Add(float64(len(b)))
	}
}

func (p *bufferPool) Get(length int) [][]byte {
	if length > p.size {
		log.Panic().Int("size", p.size).Int("length", length).Msgf("Request is larger than buffer pool size")
	}

	r := &bufferRequest{
		length: length,
		result: make(chan [][]byte, 1),
	}
	p.requests <- r
	return <-r.result
}
