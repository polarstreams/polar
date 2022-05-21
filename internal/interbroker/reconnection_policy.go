package interbroker

import (
	"math"
	"time"
)

const (
	baseReconnectionDelayMs = 20
	reconnectionPlateauMs   = 2_000
	maxReconnectionDelayMs  = 30_000
)

// Represents a default reconnection strategy for gossip connections
type reconnectionPolicy struct {
	index        int
	plateauEnded bool
}

func newReconnectionPolicy() *reconnectionPolicy {
	return &reconnectionPolicy{}
}

// Returns an exponential delay with am intermediate plateau
func (p *reconnectionPolicy) next() time.Duration {
	p.index++

	if !p.plateauEnded {
		if p.index > 8 {
			if p.index <= 68 {
				// During 60 iterations, return a constant intermediate delay for better UX
				return time.Duration(reconnectionPlateauMs) * time.Millisecond
			}
			// Reset it to common exponential delay
			p.plateauEnded = true
			p.index = 9
		}
	}

	delayMs := maxReconnectionDelayMs
	if p.index < 53 {
		delayMs = int(math.Pow(2, float64(p.index))) * baseReconnectionDelayMs
		if delayMs > maxReconnectionDelayMs {
			delayMs = maxReconnectionDelayMs
		}
	}

	return time.Duration(delayMs) * time.Millisecond
}
