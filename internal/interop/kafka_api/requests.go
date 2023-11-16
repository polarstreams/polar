package kafka_api

import (
	"encoding/binary"
	"fmt"
	"io"
)

func readRequestHeader(reader io.Reader, buffer []byte) (*requestHeader, error) {
	header := requestHeader{}
	if err := binary.Read(reader, endianess, &header.fixed); err != nil {
		return nil, err
	}

	if clientId, err := readString(reader, buffer); err != nil {
		return nil, err
	} else {
		header.clientId = clientId
	}

	return &header, nil
}

func readRequest(reader io.Reader, header *requestHeader, buffer []byte) (request, error) {
	switch header.fixed.ApiKey {
	case metadata:
		return readMetadataRequest(reader, header, buffer)
	default:
		return nil, fmt.Errorf("Kafka api key %d is not supported", header.fixed.ApiKey)
	}
}

func readString(reader io.Reader, buffer []byte) (string, error) {
	var length int16
	if err := binary.Read(reader, endianess, &length); err != nil {
		return "", err
	}
	if length > 0 {
		if _, err := io.ReadFull(reader, buffer[:length]); err != nil {
			return "", err
		}
		return string(buffer[:length]), nil
	}
	return "", nil
}

func readMetadataRequest(reader io.Reader, header *requestHeader, buffer []byte) (request, error) {
	var topicLength int32
	if err := binary.Read(reader, endianess, &topicLength); err != nil {
		return nil, err
	}

	topics := make([]string, 0)
	if topicLength > 0 {
		for i := 0; i < int(topicLength); i++ {
			if t, err := readString(reader, buffer); err != nil {
				return nil, err
			} else {
				topics = append(topics, t)
			}
		}
	}

	r := &metadataRequest{
		topics: topics,
	}

	return r, nil
}
