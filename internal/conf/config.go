package conf

const allocationPoolSize = 100 * 1024 * 1024 // 100Mib

// Config represents the application configuration
type Config interface {
	ProducerPort() int
	ConsumerPort() int
	AdminPort() int
	MaxMessageSize() int
	FlowController() FlowController
}

func NewConfig() Config {
	return &config{
		flowControl: newFlowControl(allocationPoolSize),
	}
}

type config struct {
	flowControl *flowControl
}

func (c *config) ProducerPort() int {
	return 8081
}

func (c *config) ConsumerPort() int {
	return 8082
}

func (c *config) AdminPort() int {
	return 8083
}

func (c *config) MaxMessageSize() int {
	return 1024 * 1024
}

func (c *config) FlowController() FlowController {
	return c.flowControl
}
