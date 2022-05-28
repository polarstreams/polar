package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/barcostreams/barco/internal/types"
)

const (
	MiB                    = 1024 * 1024
	allocationPoolSize     = 32 * MiB
	filePermissions        = 0755
	SegmentFileExtension   = "dlog"
	IndexFileExtension     = "index"
	ProducerOffsetFileName = "producer.offset"
	TopologyFileName       = "topology.txt" // Used for non-k8s envs
)

const (
	envHome                    = "BARCO_HOME"
	envListenOnAllAddresses    = "BARCO_LISTEN_ON_ALL"
	envGossipPort              = "BARCO_GOSSIP_PORT"
	envGossipDataPort          = "BARCO_GOSSIP_DATA_PORT"
	envSegmentFlushIntervalMs  = "BARCO_SEGMENT_FLUSH_INTERVAL_MS"
	envMaxSegmentSize          = "BARCO_MAX_SEGMENT_FILE_SIZE"
	envConsumerAddDelay        = "BARCO_CONSUMER_ADD_DELAY_MS"
	envConsumerRanges          = "BARCO_CONSUMER_RANGES"
	envTopologyFilePollDelayMs = "BARCO_TOPOLOGY_FILE_POLL_DELAY_MS"
	envShutdownDelaySecs       = "BARCO_SHUTDOWN_DELAY_SECS"
	envDevMode                 = "BARCO_DEV_MODE"
	envServiceName             = "BARCO_SERVICE_NAME"
	envPodName                 = "BARCO_POD_NAME"
	envPodNamespace            = "BARCO_POD_NAMESPACE"
	EnvBarcoDebug              = "BARCO_DEBUG"
)

// Port defaults
const (
	DefaultClientDiscoveryPort = 9250
	DefaultProducerPort        = 9251
	DefaultConsumerPort        = 9252
	DefaultMetricsPort         = 9253
	DefaultGossipPort          = 9254
	DefaultGossipDataPort      = 9255
)

var hostRegex = regexp.MustCompile(`([\w\-.]+?)-(\d+)`)

// Config represents the application configuration
type Config interface {
	Initializer
	LocalDbConfig
	GossipConfig
	ProducerConfig
	ConsumerConfig
	DiscovererConfig
	MetricsPort() int
	CreateAllDirs() error
}

type BasicConfig interface {
	HomePath() string
	ListenOnAllAddresses() bool
	DevMode() bool       // Determines whether we are running a single instance in dev mode
	ConsumerRanges() int // The number of ranges to partition any token range.
	ShutdownDelay() time.Duration
	ProducerPort() int
	ConsumerPort() int
}

type LocalDbConfig interface {
	LocalDbPath() string
}

type DatalogConfig interface {
	DatalogPath(topicDataId *TopicDataId) string
	MaxSegmentSize() int    // Maximum file size in bytes
	SegmentBufferSize() int // The amount of bytes that the segment buffer can hold
	MaxMessageSize() int
	MaxGroupSize() int  // MaxGroupSize is the maximum size of an uncompressed group of messages
	ReadAheadSize() int // The amount of bytes to read each time from a segment file
	AutoCommitInterval() time.Duration
	IndexFilePeriodBytes() int // How frequently write to the index file based on the segment size.
	SegmentFlushInterval() time.Duration
	StreamBufferSize() int // Max size of the file stream buffers (2 of them atm)
}

type DiscovererConfig interface {
	BasicConfig
	Ordinal() int
	ClientDiscoveryPort() int // port number of the HTTP discovery service to expose to client libraries
	// BaseHostName is name prefix that should be concatenated with the ordinal to
	// return the host name of a replica
	BaseHostName() string
	ServiceName() string                       // Name of the K8S service for pod stable names
	PodName() string                           // Name of the pod barco is running
	PodNamespace() string                      // Name of the namespace of the barco pod
	FixedTopologyFilePollDelay() time.Duration // The delay between attempts to read file for changes in topology
}

type ProducerConfig interface {
	BasicConfig
	DatalogConfig
	FlowController() FlowController
}

type ConsumerConfig interface {
	BasicConfig
	DatalogConfig
	ConsumerAddDelay() time.Duration
	ConsumerReadThreshold() int // The minimum amount of bytes once reached the consumer poll is fullfilled
}

type GossipConfig interface {
	BasicConfig
	DatalogConfig
	GossipPort() int
	GossipDataPort() int
	// MaxDataBodyLength is the maximum size of an interbroker data body
	MaxDataBodyLength() int
}

func NewConfig(devMode bool) Config {
	hostName, _ := os.Hostname()
	baseHostName, ordinal := parseHostName(hostName)
	return &config{
		flowControl:  newFlowControl(allocationPoolSize),
		baseHostName: baseHostName,
		devModeFlag:  devMode,
		ordinal:      ordinal,
	}
}

type config struct {
	flowControl  *flowControl
	baseHostName string
	ordinal      int
	devModeFlag  bool
}

func parseHostName(hostName string) (baseHostName string, ordinal int) {
	if hostName == "" {
		return "barco-", 0
	}

	matches := hostRegex.FindAllStringSubmatch(hostName, -1)

	if len(matches) == 0 {
		return "barco-", 0
	}

	m := matches[0]
	ordinal, _ = strconv.Atoi(m[2])
	return m[1] + "-", ordinal
}

func (c *config) Init() error {
	if c.ReadAheadSize() < c.MaxGroupSize() {
		return fmt.Errorf("ReadAheadSize can not be lower than MaxGroupSize")
	}
	if c.StreamBufferSize() < c.MaxGroupSize() {
		return fmt.Errorf("StreamBufferSize can not be lower than MaxGroupSize")
	}
	if c.ConsumerRanges() < 2 && c.ConsumerRanges()%2 != 0 {
		return fmt.Errorf("ConsumerRanges should be a positive even number")
	}
	return nil
}

func (c *config) ProducerPort() int {
	return DefaultProducerPort
}

func (c *config) ConsumerPort() int {
	return DefaultConsumerPort
}

func (c *config) ClientDiscoveryPort() int {
	return DefaultClientDiscoveryPort
}

func (c *config) MetricsPort() int {
	return DefaultMetricsPort
}

func (c *config) GossipPort() int {
	return envInt(envGossipPort, DefaultGossipPort)
}

func (c *config) GossipDataPort() int {
	return envInt(envGossipDataPort, DefaultGossipDataPort)
}

func (c *config) ListenOnAllAddresses() bool {
	return os.Getenv(envListenOnAllAddresses) != "false"
}

func (c *config) DevMode() bool {
	return c.devModeFlag || os.Getenv(envDevMode) == "true"
}

func (c *config) ConsumerRanges() int {
	return envInt(envConsumerRanges, 4)
}

func (c *config) MaxMessageSize() int {
	return MiB
}

func (c *config) MaxGroupSize() int {
	return 2 * MiB
}

func (c *config) ReadAheadSize() int {
	return c.MaxGroupSize() * 8
}

func (c *config) AutoCommitInterval() time.Duration {
	return 5 * time.Second
}

func (c *config) ConsumerAddDelay() time.Duration {
	ms := envInt(envConsumerAddDelay, 10000)
	return time.Duration(ms) * time.Millisecond
}

func (c *config) ConsumerReadThreshold() int {
	return c.MaxGroupSize()
}

func (c *config) IndexFilePeriodBytes() int {
	return int(0.05 * float64(c.MaxSegmentSize()))
}

func (c *config) SegmentFlushInterval() time.Duration {
	ms := envInt(envSegmentFlushIntervalMs, 2000)
	return time.Duration(ms) * time.Millisecond
}

func (c *config) ShutdownDelay() time.Duration {
	if c.DevMode() {
		return 0
	}
	secs := envInt(envShutdownDelaySecs, 30)
	return time.Duration(secs) * time.Second
}

func (c *config) MaxSegmentSize() int {
	return envInt(envMaxSegmentSize, 1024*MiB)
}

func (c *config) SegmentBufferSize() int {
	return 8 * MiB
}

func (c *config) StreamBufferSize() int {
	return c.SegmentBufferSize()
}

func (c *config) MaxDataBodyLength() int {
	// It's a different setting but points to the same value
	// The amount of the data sent in a single message is the size of a group of records
	return c.MaxGroupSize()
}

func (c *config) FlowController() FlowController {
	return c.flowControl
}

func (c *config) HomePath() string {
	return env(envHome, filepath.Join("/var", "lib", "barco"))
}

func (c *config) dataPath() string {
	return filepath.Join(c.HomePath(), "data")
}

func (c *config) LocalDbPath() string {
	return filepath.Join(c.dataPath(), "local.db")
}

func (c *config) DatalogPath(t *TopicDataId) string {
	// Pattern: /var/lib/barco/data/datalog/{topic}/{token}/{rangeIndex}/{genVersion}
	return filepath.Join(c.dataPath(), "datalog", t.Name, t.Token.String(), t.RangeIndex.String(), t.Version.String())
}

func (c *config) CreateAllDirs() error {
	return os.MkdirAll(c.dataPath(), filePermissions)
}

func (c *config) Ordinal() int {
	return c.ordinal
}

func (c *config) BaseHostName() string {
	return c.baseHostName
}

func (c *config) ServiceName() string {
	return env(envServiceName, "barco")
}

func (c *config) PodName() string {
	return env(envPodName, "")
}

func (c *config) PodNamespace() string {
	return env(envPodNamespace, "")
}

func (c *config) FixedTopologyFilePollDelay() time.Duration {
	ms := envInt(envTopologyFilePollDelayMs, 10000)
	return time.Duration(ms) * time.Millisecond
}

func env(name string, defaultValue string) string {
	value := os.Getenv(name)
	if value == "" {
		value = defaultValue
	}
	return value
}

func envInt(name string, defaultValue int) int {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return intValue
}

// Gets the formatted file name based on the segment id
func SegmentFileName(segmentId int64) string {
	return fmt.Sprintf("%s.%s", SegmentFilePrefix(segmentId), SegmentFileExtension)
}

// Gets the formatted value of the id for the file name (%020d) w/o the extension, e.g. 123 -> "00000000000000000123"
func SegmentFilePrefix(segmentId int64) string {
	return fmt.Sprintf("%020d", segmentId)
}

func SegmentIdFromName(fileName string) int64 {
	parts := strings.Split(fileName, ".")
	value, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Invalid fileName '%s': %s", fileName, err.Error()))
	}
	return value
}
