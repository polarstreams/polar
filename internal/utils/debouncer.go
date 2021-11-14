package utils

import (
	"sync"
	"time"
)

type debouncer struct {
	mu    sync.Mutex
	delay time.Duration
	timer *time.Timer
}

type Debouncer func(time.Duration, func())

func Debounce() Debouncer {
	d := &debouncer{}

	return func(delay time.Duration, f func()) {
		d.set(delay, f)
	}
}

func (d *debouncer) set(delay time.Duration, f func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hasStopped := false
	if d.timer != nil {
		hasStopped = d.timer.Stop()
	}

	// Reset the delay when provided delay is higher than the previous one
	if !hasStopped || d.delay < delay {
		d.delay = delay
	}
	d.timer = time.AfterFunc(d.delay, f)
}
