package topics

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

type TopicHandler interface {
	types.Initializer
	TopicGetter
}

type TopicGetter interface {
	Get(topic string) *types.TopicInfo
	Exists(topic string) bool
}

func NewHandler(config conf.Config) TopicHandler {
	return &topicHandler{config}
}

type topicHandler struct {
	config conf.Config
}

func (h *topicHandler) Init() error {
	return nil
}

func (h *topicHandler) Get(topic string) *types.TopicInfo {
	return nil
}

func (h *topicHandler) Exists(topic string) bool {
	return false
}
