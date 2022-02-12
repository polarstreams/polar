// +build integration

package integration

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

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
}

// Creates and starts a broker
func NewTestBroker(ordinal int) *TestBroker {
	b := TestBroker{
		ordinal: ordinal,
		startChan: make(chan bool, 1),
		brokerName: fmt.Sprintf("Broker%d", ordinal),
	}
	b.Start()
	return &b
}

func (b *TestBroker) Start() {
	err := exec.Command("go", "build", "-o", "barco.exe", "../../../.").Run()
	Expect(err).NotTo(HaveOccurred())
	cmd := exec.Command("./barco.exe")
	os.RemoveAll(fmt.Sprintf("./home%d", b.ordinal))
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("BARCO_ORDINAL=%d", b.ordinal),
		fmt.Sprintf("BARCO_HOME=home%d", b.ordinal),
		"BARCO_LISTEN_ON_ALL=false",
		"BARCO_BROKER_NAMES=127.0.0.1,127.0.0.2,127.0.0.3",
	)
	stderr, err := cmd.StderrPipe()
	Expect(err).NotTo(HaveOccurred())

	mu := sync.Mutex{}
	const maxOutput = 100
	output := make([]string, 0, maxOutput)

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
			if len(output) >= maxOutput {
				output = output[1:(maxOutput - 1)]
			}
			output = append(output, value)
			mu.Unlock()
		}
	}()

	err = cmd.Start()
	Expect(err).NotTo(HaveOccurred())

	b.cmd = cmd
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
		Fail("Broker could not be started")
	}
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
