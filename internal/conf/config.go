package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/polarstreams/polar/internal/types"
)

const (
	MiB                    = 1024 * 1024
	filePermissions        = 0755
	SegmentFileExtension   = "dlog"
	IndexFileExtension     = "index"
	ProducerOffsetFileName = "producer.offset"
	TopologyFileName       = "topology.txt" // Used for non-k8s envs
)

const (
	envHome                            = "POLAR_HOME"
	envListenOnAllAddresses            = "POLAR_LISTEN_ON_ALL"
	envProducerPort                    = "POLAR_PRODUCER_PORT"
	envConsumerPort                    = "POLAR_CONSUMER_PORT"
	envClientDiscoveryPort             = "POLAR_CLIENT_DISCOVERY_PORT"
	envMetricsPort                     = "POLAR_METRICS_PORT"
	envGossipPort                      = "POLAR_GOSSIP_PORT"
	envGossipDataPort                  = "POLAR_GOSSIP_DATA_PORT"
	envSegmentFlushIntervalMs          = "POLAR_SEGMENT_FLUSH_INTERVAL_MS"
	envLogRetentionDuration            = "POLAR_LOG_RETENTION_DURATION"
	envReplicationTimeoutDuration      = "POLAR_REPLICATION_TIMEOUT_DURATION"
	envReplicationWriteTimeoutDuration = "POLAR_REPLICATION_WRITE_TIMEOUT_DURATION"
	envMaxSegmentSize                  = "POLAR_MAX_SEGMENT_FILE_SIZE"
	envProducerBufferPoolSize          = "POLAR_PRODUCER_BUFFER_POOL_SIZE"
	envConsumerAddDelay                = "POLAR_CONSUMER_ADD_DELAY_MS"
	envConsumerReadTimeout             = "POLAR_CONSUMER_READ_TIMEOUT_MS"
	envConsumerRanges                  = "POLAR_CONSUMER_RANGES"
	envTopologyFilePollDelayMs         = "POLAR_TOPOLOGY_FILE_POLL_DELAY_MS"
	envShutdownDelaySecs               = "POLAR_SHUTDOWN_DELAY_SECS"
	envDevMode                         = "POLAR_DEV_MODE"
	envServiceName                     = "POLAR_SERVICE_NAME"
	envPodName                         = "POLAR_POD_NAME"
	envPodNamespace                    = "POLAR_POD_NAMESPACE"
	EnvDebug                           = "POLAR_DEBUG"
	envMaxMessageSize                  = "POLAR_MAX_MESSAGE_SIZE"
	envMaxGroupSize                    = "POLAR_MAX_GROUP_SIZE"
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

const (
	defaultLogRetention            = "168h" // 7 days
	defaultReplicationTimeout      = "1s"
	defaultReplicationWriteTimeout = "500ms"
	defaultProducerBufferPoolSize  = 32 * MiB
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
	DatalogSegmentsPath() string
	MaxSegmentSize() int    // Maximum file size in bytes
	SegmentBufferSize() int // The amount of bytes that the segment buffer can hold
	MaxMessageSize() int
	MaxGroupSize() int  // MaxGroupSize is the maximum size of an uncompressed group of messages
	ReadAheadSize() int // The amount of bytes to read each time from a segment file
	AutoCommitInterval() time.Duration
	IndexFilePeriodBytes() int // How frequently write to the index file based on the segment size.
	SegmentFlushInterval() time.Duration
	LogRetentionDuration() *time.Duration // The amount of time to keep a log file before deleting it (default = 7d)
	StreamBufferSize() int                // Max size of the file stream buffers (2 of them atm)
}

type DiscovererConfig interface {
	BasicConfig
	Ordinal() int
	ClientDiscoveryPort() int // port number of the HTTP discovery service to expose to client libraries
	// BaseHostName is name prefix that should be concatenated with the ordinal to
	// return the host name of a replica
	BaseHostName() string
	ServiceName() string                       // Name of the K8S service for pod stable names
	PodName() string                           // Name of the pod the broker is running
	PodNamespace() string                      // Name of the namespace of the broker pod
	FixedTopologyFilePollDelay() time.Duration // The delay between attempts to read file for changes in topology
}

type ProducerConfig interface {
	BasicConfig
	DatalogConfig
	ProducerBufferPoolSize() int
}

type ConsumerConfig interface {
	BasicConfig
	DatalogConfig
	ConsumerAddDelay() time.Duration
	ConsumerReadTimeout() time.Duration // The interval to set the deadline in the consumer connection
	ConsumerReadThreshold() int         // The minimum amount of bytes once reached the consumer poll is fulfilled
}

type GossipConfig interface {
	BasicConfig
	DatalogConfig
	GossipPort() int
	GossipDataPort() int
	ReplicationTimeout() time.Duration
	ReplicationWriteTimeout() time.Duration
	// MaxDataBodyLength is the maximum size of an interbroker data body
	MaxDataBodyLength() int
}

func NewConfig(devMode bool) Config {
	hostName, _ := os.Hostname()
	baseHostName, ordinal := parseHostName(hostName)
	c := &config{
		baseHostName:            baseHostName,
		devModeFlag:             devMode,
		ordinal:                 ordinal,
		replicationTimeout:      parseDuration(envReplicationTimeoutDuration, defaultReplicationTimeout),
		replicationWriteTimeout: parseDuration(envReplicationWriteTimeoutDuration, defaultReplicationWriteTimeout),
	}
	return c
}

type config struct {
	baseHostName            string
	ordinal                 int
	devModeFlag             bool
	replicationTimeout      time.Duration // Cache parsed to avoid doing it per call
	replicationWriteTimeout time.Duration
}

func parseHostName(hostName string) (baseHostName string, ordinal int) {
	if hostName == "" {
		return "polar-", 0
	}

	matches := hostRegex.FindAllStringSubmatch(hostName, -1)

	if len(matches) == 0 {
		return "polar-", 0
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
	if c.ConsumerRanges() < 2 || c.ConsumerRanges()%2 != 0 || c.ConsumerRanges() > 1000 {
		return fmt.Errorf("ConsumerRanges should be a positive even number, less than or equal to 1000")
	}
	value := env(envLogRetentionDuration, defaultLogRetention)
	if _, err := time.ParseDuration(value); err != nil && value != "null" {
		return fmt.Errorf("Log retention duration '%s' is not a valid value", value)
	}
	if c.replicationTimeout <= 0 || c.replicationWriteTimeout <= 0 || c.replicationWriteTimeout > c.replicationTimeout {
		return fmt.Errorf("Invalid replication timeouts")
	}

	return nil
}

func (c *config) ProducerPort() int {
	return envInt(envProducerPort, DefaultProducerPort)
}

func (c *config) ConsumerPort() int {
	return envInt(envConsumerPort, DefaultConsumerPort)
}

func (c *config) ClientDiscoveryPort() int {
	return envInt(envClientDiscoveryPort, DefaultClientDiscoveryPort)
}

func (c *config) MetricsPort() int {
	return envInt(envMetricsPort, DefaultMetricsPort)
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
	return envInt(envMaxMessageSize, MiB)
}

func (c *config) MaxGroupSize() int {
	return envInt(envMaxGroupSize, 2*MiB)
}

func (c *config) ReadAheadSize() int {
	return c.MaxGroupSize() * 4
}

func (c *config) AutoCommitInterval() time.Duration {
	return 5 * time.Second
}

func (c *config) ConsumerAddDelay() time.Duration {
	ms := envInt(envConsumerAddDelay, 10000)
	return time.Duration(ms) * time.Millisecond
}

func (c *config) ConsumerReadTimeout() time.Duration {
	ms := envInt(envConsumerReadTimeout, 120000)
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

func (c *config) LogRetentionDuration() *time.Duration {
	value := env(envLogRetentionDuration, defaultLogRetention)
	if value == "null" {
		return nil
	}
	t, err := time.ParseDuration(value)
	if err != nil {
		panic(err)
	}

	return &t
}

func (c *config) ReplicationTimeout() time.Duration {
	return c.replicationTimeout
}

func (c *config) ReplicationWriteTimeout() time.Duration {
	return c.replicationWriteTimeout
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

func (c *config) ProducerBufferPoolSize() int {
	return envInt(envProducerBufferPoolSize, defaultProducerBufferPoolSize)
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

func (c *config) HomePath() string {
	return env(envHome, filepath.Join("/var", "lib", "polar"))
}

func (c *config) dataPath() string {
	return filepath.Join(c.HomePath(), "data")
}

func (c *config) LocalDbPath() string {
	return filepath.Join(c.dataPath(), "local.db")
}

func (c *config) DatalogPath(t *TopicDataId) string {
	// Pattern: /var/lib/polar/data/datalog/{topic}/{token}/{rangeIndex}/{genVersion}
	return filepath.Join(c.DatalogSegmentsPath(), t.Name, t.Token.String(), t.RangeIndex.String(), t.Version.String())
}

func (c *config) DatalogSegmentsPath() string {
	// Example: /var/lib/polar/data/datalog/
	return filepath.Join(c.dataPath(), "datalog")
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
	return env(envServiceName, "polar")
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

func parseDuration(envName, defaultValue string) time.Duration {
	value := env(envName, defaultValue)
	t, err := time.ParseDuration(value)
	if err != nil {
		panic(err)
	}
	return t
}
