package conf

import "encoding/binary"

const (
	StatusUrl = "/status"
	// Producer Urls

	// Url for producing messages
	TopicMessageUrl = "/v1/topic/:topic/messages"

	// Consumer Urls

	// Url consuming messages
	ConsumerRegisterUrl = "/v1/consumer/register"
	ConsumerPollUrl     = "/v1/consumer/poll"

	// Gossip Urls

	// Url for getting/setting the generation by token
	GossipGenerationUrl = "/v1/generation/%s"
	// Url for setting the generation as proposed/accepted for token
	GossipGenerationProposeUrl = "/v1/generation/%s/propose"
	// Url for setting the generation and transaction as committed for token
	GossipGenerationCommmitUrl = "/v1/generation/%s/commit"
	GossipTokenHasHistoryUrl   = "/v1/token/%s/has-history"
	GossipTokenInRange         = "/v1/token/%s/in-range"
)

const MaxTopicLength = 255

var Endianness = binary.BigEndian
