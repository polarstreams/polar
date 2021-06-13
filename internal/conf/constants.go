package conf

import "encoding/binary"

const (
	StatusUrl = "/status"
	// Producer Urls

	// Url for posting and consuming messages
	TopicMessageUrl = "/v1/topic/:topic/messages"

	// Gossip Urls

	// Url for getting/setting the generation by token
	GossipGenerationUrl = "/v1/generation/%s"
)

const MaxTopicLength = 255

var Endianness = binary.BigEndian
