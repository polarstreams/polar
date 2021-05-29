package conf

import "encoding/binary"

const (
	// Url for posting and consuming messages
	TopicMessageUrl = "/v1/topic/:topic/messages"

	StatusUrl = "/status"
)

const MaxTopicLength = 255

var Endianness = binary.BigEndian
