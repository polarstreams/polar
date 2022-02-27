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

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
)

// Represents a broker process
type TestBroker struct {
	ordinal int
	cmd     *exec.Cmd
	mu      sync.RWMutex
	startChan chan bool
	brokerName string
	output  []string
	options *TestBrokerOptions
}

type TestBrokerOptions struct {
	InitialClusterSize int
}

// Creates and starts a broker
func NewTestBroker(ordinal int, options... *TestBrokerOptions) *TestBroker {
	if len(options) > 1 {
		panic("Only 1 set of options is supported")
	}

	brokerOptions := &TestBrokerOptions{}
	if len(options) == 1 {
		brokerOptions = options[0]
	}

	b := TestBroker{
		ordinal: ordinal,
		startChan: make(chan bool, 1),
		brokerName: fmt.Sprintf("Broker%d", ordinal),
		options: brokerOptions,
	}
	b.Start()
	return &b
}

func (b *TestBroker) Start() {
	buildOutput, err := exec.Command("go", "build", "-o", "barco.exe", "../../../.").CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "Build failed: %s", string(buildOutput))
	cmd := exec.Command("./barco.exe", "-debug")
	os.RemoveAll(fmt.Sprintf("./home%d", b.ordinal))

	names := make([]string, 0)
	brokerLength := 3
	if b.options.InitialClusterSize > 0 {
		brokerLength = b.options.InitialClusterSize
	}
	for i := 1; i <= brokerLength; i++ {
		names = append(names, fmt.Sprintf("127.0.0.%d", i))
	}

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("BARCO_ORDINAL=%d", b.ordinal),
		fmt.Sprintf("BARCO_HOME=home%d", b.ordinal),
		fmt.Sprintf("BARCO_BROKER_NAMES=%s", strings.Join(names, ",")),
		"BARCO_LISTEN_ON_ALL=false",
		"BARCO_SEGMENT_FLUSH_INTERVAL_MS=1000",
		"BARCO_CONSUMER_ADD_DELAY_MS=200",
		"BARCO_TOPOLOGY_FILE_POLL_DELAY_MS=400",
	)
	stderr, err := cmd.StderrPipe()
	Expect(err).NotTo(HaveOccurred())

	mu := sync.Mutex{}
	const maxOutput = 100
	b.output = make([]string, 0, maxOutput)

	scanner := bufio.NewScanner(stderr)
	go func() {
		started := false
		for scanner.Scan() {
			value := scanner.Text()
			if log.Debug().Enabled() {
				fmt.Printf("%s > %s\n", b.brokerName, value)
			}
			if !started && strings.Contains(value, "Barco started") {
				started = true
				b.startChan <- true
			}

			mu.Lock()
			if len(b.output) >= maxOutput {
				b.output = b.output[1:]
			}
			b.output = append(b.output, value)
			mu.Unlock()
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

func (b *TestBroker) WaitForStart() {
	timerChannel := time.After(5 * time.Second)
	started := false

    select {
    case started = <-b.startChan:
		log.Debug().Msgf("%s started", b.brokerName)
    case <-timerChannel:
		log.Debug().Msgf("%s start timed out", b.brokerName)
    }

	if !started {
		b.Kill()
		Fail(fmt.Sprintf("Broker %d could not be started", b.ordinal))
	}
}

// Reads the last 100 lines of the output looking for a match
func (b *TestBroker) WaitOutput(format string, a ...interface{}) {
	start := time.Now()
	found := false
	pattern := fmt.Sprintf(format, a...)
	for time.Since(start) < 5 * time.Second {
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
	output = output[len(output)-nMessages:]
	r, err := regexp.Compile(`\"level\":\"error\"`)
	if err != nil {
		log.Panic().Err(err).Msgf("Invalid search pattern")
	}
	if found, occurrence := b.match(output, r); found {
		Fail(fmt.Sprintf("Found error: %s", occurrence))
	}
}

func (b *TestBroker) match(output []string, r *regexp.Regexp) (bool, string) {
	for i := len(output)-1; i >= 0; i-- {
		text := output[i]
		if r.MatchString(text) {
			return true, text
		}
	}
	return false, ""
}

func (b *TestBroker) WaitForVersion1() {
	b.WaitOutput("Committing \\[.*\\] v1 with B%d as leader", b.ordinal)
}

func (b *TestBroker) Shutdown() {
	log.Debug().Msgf("Shutting down broker %d", b.ordinal)
	err := b.cmd.Process.Signal(os.Interrupt)
	Expect(err).NotTo(HaveOccurred())

	exited := false
	timerChan := time.After(2 * time.Second)
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
		b.Kill()
	}
}

func (b *TestBroker) Kill() error {
	log.Debug().Msgf("Killing broker %d", b.ordinal)
	err := b.cmd.Process.Kill()
	Expect(err).NotTo(HaveOccurred())
	return b.cmd.Wait()
}
