package conf

import (
	"os"
	"path/filepath"

	"github.com/jorgebay/soda/internal/types"
)

const Mib = 1024 * 1024
const allocationPoolSize = 100 * Mib
const filePermissions = 0755

// Config represents the application configuration
type Config interface {
	LocalDbConfig
	GossipConfig
	ProducerConfig
	ConsumerPort() int
	AdminPort() int
	HomePath() string
	CreateAllDirs() error
}

type LocalDbConfig interface {
	LocalDbPath() string
}

type DatalogConfig interface {
	DatalogPath(topic string, token types.Token, genId string) string
	MaxSegmentSize() int
}

type ProducerConfig interface {
	DatalogConfig
	ProducerPort() int
	FlowController() FlowController
	MaxMessageSize() int
	// MaxGroupSize is the maximum size of an uncompressed group of messages
	MaxGroupSize() int
}

type GossipConfig interface {
	GossipPort() int
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

func (c *config) GossipPort() int {
	return 8084
}

func (c *config) MaxMessageSize() int {
	return Mib
}

func (c *config) MaxGroupSize() int {
	return 2 * Mib
}

func (c *config) MaxSegmentSize() int {
	return 1024 * Mib
}

func (c *config) FlowController() FlowController {
	return c.flowControl
}

func (c *config) HomePath() string {
	homePath := os.Getenv("SODA_HOME")
	if homePath == "" {
		return filepath.Join("/var", "lib", "soda")
	}
	return homePath
}

func (c *config) dataPath() string {
	return filepath.Join(c.HomePath(), "data")
}

func (c *config) LocalDbPath() string {
	return filepath.Join(c.dataPath(), "local.db")
}

func (c *config) DatalogPath(topic string, token types.Token, genId string) string {
	// Pattern: /var/lib/soda/data/datalog/{topic}/{token}/{genId}/
	return filepath.Join(c.dataPath(), "datalog", topic, token.String(), genId)
}

func (c *config) CreateAllDirs() error {
	return os.MkdirAll(c.dataPath(), filePermissions)
}
