package data

import "github.com/jorgebay/soda/internal/types"

type TopicHandler interface {
	TopicGetter
}

type TopicGetter interface {
	Get(topic string) types.TopicInfo
	Exists(topic string) bool
}
