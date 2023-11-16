//go:build integration
// +build integration

package integration

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/conf"
	"github.com/rs/zerolog/log"
)

const (
	SegmentFlushInterval = 500 * time.Millisecond
	ConsumerAddDelay     = 200 * time.Millisecond
)

// Represents a broker process
type TestBroker struct {
	ordinal    int
	cmd        *exec.Cmd
	mu         sync.RWMutex
	startChan  chan bool
	brokerName string
	output     []string
	options    *TestBrokerOptions
}

type TestBrokerOptions struct {
	InitialClusterSize int
	DevMode            bool
	EnableKafkaApi     bool
}

// Creates and starts a broker
func NewTestBroker(ordinal int, options ...*TestBrokerOptions) *TestBroker {
	if len(options) > 1 {
		panic("Only 1 set of options is supported")
	}

	brokerOptions := &TestBrokerOptions{}
	if len(options) == 1 {
		brokerOptions = options[0]
	}

	b := TestBroker{
		ordinal:    ordinal,
		startChan:  make(chan bool, 1),
		brokerName: fmt.Sprintf("Broker%d", ordinal),
		options:    brokerOptions,
	}

	// Clean the home directory of the new broker
	err := os.RemoveAll(fmt.Sprintf("./home%d", b.ordinal))
	Expect(err).NotTo(HaveOccurred(), "Could not remove home%d directory", b.ordinal)

	b.Start()
	return &b
}

func (b *TestBroker) Start() {
	buildOutput, err := exec.Command("go", "build", "-o", "polar.exe", "../../../.").CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "Build failed: %s", string(buildOutput))

	logPretty := ""
	if os.Getenv("POLAR_TEST_LOG_PRETTY") == "true" {
		logPretty = "-pretty"
	}

	cmd := exec.Command("./polar.exe", "-debug", logPretty)

	names := make([]string, 0)
	brokerLength := 3
	if b.options.InitialClusterSize > 0 {
		brokerLength = b.options.InitialClusterSize
	}
	for i := 1; i <= brokerLength; i++ {
		names = append(names, fmt.Sprintf("127.0.0.%d", i))
	}

	// Basic test env variables
	envs := append(os.Environ(),
		fmt.Sprintf("POLAR_HOME=home%d", b.ordinal),
		fmt.Sprintf("POLAR_SEGMENT_FLUSH_INTERVAL_MS=%d", SegmentFlushInterval.Milliseconds()),
		fmt.Sprintf("POLAR_CONSUMER_ADD_DELAY_MS=%d", ConsumerAddDelay.Milliseconds()),
		"POLAR_CONSUMER_RANGES=4",
		"POLAR_TOPOLOGY_FILE_POLL_DELAY_MS=400",
		"POLAR_MAX_SEGMENT_FILE_SIZE=16777216", // 16MiB
		"POLAR_SHUTDOWN_DELAY_SECS=2")

	if !b.options.DevMode {
		envs = append(envs,
			fmt.Sprintf("POLAR_ORDINAL=%d", b.ordinal),
			fmt.Sprintf("POLAR_BROKER_NAMES=%s", strings.Join(names, ",")),
			"POLAR_LISTEN_ON_ALL=false")
	} else {
		envs = append(envs, "POLAR_DEV_MODE=true")
	}

	if b.options.EnableKafkaApi {
		envs = append(envs, "BARCO_KAFKA_API_ENABLED=true")
	}

	cmd.Env = envs
	stderr, err := cmd.StderrPipe()
	Expect(err).NotTo(HaveOccurred())

	const maxOutput = 200
	b.output = make([]string, 0, maxOutput)

	scanner := bufio.NewScanner(stderr)
	go func() {
		started := false
		for scanner.Scan() {
			value := scanner.Text()
			if log.Debug().Enabled() {
				focus := os.Getenv("POLAR_TEST_OUTPUT_FOCUS")
				if focus == "" || b.brokerName == focus {
					fmt.Printf("%s > %s\n", b.brokerName, value)
				}
			}
			if !started && strings.Contains(value, "PolarStreams started") {
				started = true
				b.startChan <- true
			}

			b.mu.Lock()
			if len(b.output) >= maxOutput {
				b.output = b.output[1:]
			}
			b.output = append(b.output, value)
			b.mu.Unlock()
		}
	}()

	err = cmd.Start()
	Expect(err).NotTo(HaveOccurred())

	b.cmd = cmd
}

func (b *TestBroker) UpdateTopologyFile(brokerLength int) {
	names := make([]string, 0)
	for i := 1; i <= brokerLength; i++ {
		names = append(names, fmt.Sprintf("127.0.0.%d", i))
	}
	os.WriteFile(
		filepath.Join(fmt.Sprintf("home%d", b.ordinal), conf.TopologyFileName),
		[]byte(strings.Join(names, ",")),
		0644)
}

func (b *TestBroker) WaitForStart() *TestBroker {
	timerChannel := time.After(5 * time.Second)
	started := false

	select {
	case started = <-b.startChan:
		log.Debug().Msgf("%s started", b.brokerName)
	case <-timerChannel:
		log.Error().Msgf("%s start timed out", b.brokerName)
	}

	if !started {
		b.Kill()
		Fail(fmt.Sprintf("Broker %d could not be started", b.ordinal))
	}

	return b
}

// Reads the last 100 lines of the output looking for a match
func (b *TestBroker) WaitOutput(format string, a ...interface{}) {
	start := time.Now()
	found := false
	pattern := fmt.Sprintf(format, a...)
	for time.Since(start) < 5*time.Second {
		output := b.getOutput()
		r, err := regexp.Compile(pattern)
		if err != nil {
			log.Panic().Err(err).Msgf("Invalid search pattern")
		}
		if found, _ = b.match(output, r); found {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	Expect(found).To(BeTrue(), "Waited 5 seconds for B%d output of '%s'", b.ordinal, pattern)
}

// Gets a copy of the current output
func (b *TestBroker) getOutput() []string {
	b.mu.RLock()
	output := make([]string, len(b.output))
	copy(output, b.output)
	b.mu.RUnlock()
	return output
}

// Checks for output messages in the last n messages
func (b *TestBroker) LookForErrors(nMessages int) {
	output := b.getOutput()
	startIndex := len(output) - nMessages
	if startIndex < 0 {
		startIndex = 0
	}
	output = output[startIndex:]
	r, err := regexp.Compile(`\"level\":\"error\"`)
	if err != nil {
		log.Panic().Err(err).Msgf("Invalid search pattern")
	}
	if found, occurrence := b.match(output, r); found {
		Fail(fmt.Sprintf("Found error: %s", occurrence))
	}
}

func (b *TestBroker) match(output []string, r *regexp.Regexp) (bool, string) {
	for i := len(output) - 1; i >= 0; i-- {
		text := output[i]
		if r.MatchString(text) {
			return true, text
		}
	}
	return false, ""
}

// Waits for generation version 1
func (b *TestBroker) WaitForVersion1() {
	b.WaitOutput("Committ\\w{2,3} \\[.*\\] v1 with B%d as leader", b.ordinal)
}

func (b *TestBroker) StartShutdown() {
	log.Debug().Msgf("Shutting down test broker B%d", b.ordinal)
	err := b.cmd.Process.Signal(os.Interrupt)
	Expect(err).NotTo(HaveOccurred())
}

func (b *TestBroker) Shutdown() {
	b.StartShutdown()
	b.WaitForShutdownOrKill()
	b.mu.Lock()
	defer b.mu.Unlock()
	// Clear in case the TestBroker instance is reused (restarted)
	b.output = b.output[:0]
}

func (b *TestBroker) Kill() error {
	log.Debug().Msgf("Killing broker %d", b.ordinal)
	err := b.cmd.Process.Kill()
	Expect(err).NotTo(HaveOccurred())
	return b.cmd.Wait()
}

func (b *TestBroker) WaitForShutdownOrKill() {
	exited := false
	timerChan := time.After(5 * time.Second)
	exitChan := make(chan bool, 1)
	go func() {
		b.cmd.Wait()
		exitChan <- true
	}()

	select {
	case exited = <-exitChan:
		log.Debug().Msgf("%s exited", b.brokerName)
	case <-timerChan:
		log.Debug().Msgf("%s did not exit before timing out", b.brokerName)
	}

	if !exited {
		log.Error().Msgf("%s Could not be shutted down cleanly, killing process", b.brokerName)
		_ = b.Kill()
	}
}

func ShutdownInParallel(brokers ...*TestBroker) {
	for _, b := range brokers {
		b.StartShutdown()
	}

	for _, b := range brokers {
		b.WaitForShutdownOrKill()
	}
}
