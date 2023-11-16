package kafka_api

import (
	"encoding/binary"
	"io"

	"github.com/rs/zerolog/log"
)

// Header V1
type requestHeader struct {
	// Fixed values for quickly write decoders/encoders
	fixed    requestHeaderFixed
	clientId string
}

type apiKey int16

const (
	produce     apiKey = 0
	fetch       apiKey = 1
	listOffsets apiKey = 2
	metadata    apiKey = 3
)

type requestHeaderFixed struct {
	// Order of values according to the protocol
	Size          int32
	ApiKey        apiKey
	ApiVersion    int16
	CorrelationId int32
}

type request interface {
	Process(header *requestHeader, s *kafkaServer) (response, error)
}

type response interface {
	Write(w io.Writer) error
	Size() int32
	CorrelationId() int32
}

var endianess = binary.BigEndian

type metadataRequest struct {
	topics []string
}

func (r *metadataRequest) Process(header *requestHeader, s *kafkaServer) (response, error) {
	// TODO: IMPLEMENT
	response := &metadataResponseV1{
		correlationId: header.fixed.CorrelationId,
		brokers: []brokerMetadataV1{{
			nodeId: 0,
			host:   "abc",
			port:   9092,
			rack:   "rack1",
		}},
		controllerId: 0,
		topics:       []any{},
	}
	return response, nil
}

type metadataResponseV1 struct {
	correlationId int32
	brokers       []brokerMetadataV1
	controllerId  int32
	topics        []any
}

func (r *metadataResponseV1) Write(w io.Writer) error {
	log.Debug().Msgf("----Writing metadata response")
	// TODO: MOVE THIS TO writer
	// size := int32(4 + 4 + 4)
	// for _, b := range r.brokers {
	// 	size += b.size()
	// }
	// size+correlation id size
	// if err := binary.Write(w, endianess, size+4); err != nil {
	// 	return err
	// }
	// // CORRELATION ID
	// if err := binary.Write(w, endianess, int32(1)); err != nil {
	// 	return err
	// }

	if err := binary.Write(w, endianess, int32(len(r.brokers))); err != nil {
		return err
	}
	for _, b := range r.brokers {
		if err := b.write(w); err != nil {
			return err
		}
	}
	if err := binary.Write(w, endianess, r.controllerId); err != nil {
		return err
	}
	if err := binary.Write(w, endianess, int32(-1)); err != nil {
		return err
	}
	return nil
}

func (r *metadataResponseV1) Size() int32 {
	size := int32(4 + 4 + 4)
	for _, b := range r.brokers {
		size += b.size()
	}
	return size
}

func (r *metadataResponseV1) CorrelationId() int32 {
	return r.correlationId
}

type brokerMetadataV1 struct {
	nodeId int32
	host   string
	port   int32
	rack   string
}

func (b *brokerMetadataV1) size() int32 {
	return 4 + 4 + sizeOfString(b.host) + sizeOfString(b.rack)
}

func (b *brokerMetadataV1) write(w io.Writer) error {
	if err := binary.Write(w, endianess, b.nodeId); err != nil {
		return err
	}
	if err := writeString(w, b.host); err != nil {
		return err
	}
	if err := binary.Write(w, endianess, b.port); err != nil {
		return err
	}
	if err := writeString(w, b.rack); err != nil {
		return err
	}
	return nil
}

func writeString(w io.Writer, value string) error {
	if err := binary.Write(w, endianess, int16(len(value))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(value)); err != nil {
		return err
	}
	return nil
}

func sizeOfString(s string) int32 {
	return 2 + int32(len(s))
}
