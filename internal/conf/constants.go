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
	// Url for setting one generation as proposed/accepted or two generations as accepted.
	// Token is part of the route but ignored.
	GossipGenerationProposeUrl = "/v1/generation/%s/propose"
	// Url for setting the generation and transaction as committed for token
	GossipGenerationCommmitUrl = "/v1/generation/%s/commit"
	// Url for requesting the token range to be split as a consequence of scaling up
	GossipGenerationSplitUrl = "/v1/token/split"

	GossipTokenHasHistoryUrl    = "/v1/token/%s/has-history"
	GossipTokenGetHistoryUrl    = "/v1/token/%s/history"
	GossipTokenInRange          = "/v1/token/%s/in-range"
	GossipBrokerIdentifyUrl     = "/v1/broker/identify" // Send/receive my info to the peer
	GossipHostIsUpUrl           = "/v1/broker/%s/is-up"
	GossipConsumerGroupsInfoUrl = "/v1/consumer/groups-info"        // Send/receive consumer groups info
	GossipConsumerOffsetUrl     = "/v1/consumer/offsets"            // Send/receive consumer offsets
	GossipReadProducerOffsetUrl = "/v1/producer/offset/%s/%s/%s/%s" // Reads the producer offset, with params: topic, token, range, version

	// Routing Urls (using gossip http/2 interface)

	RoutingMessageUrl = "/v1/routing/topic/%s/messages"
)

const MaxTopicLength = 255

var Endianness = binary.BigEndian
