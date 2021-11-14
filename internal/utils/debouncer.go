package utils

import (
	"sync"
	"time"
)

type debouncer struct {
	mu             sync.Mutex
	start          time.Time
	delay          time.Duration
	thresholdDelay time.Duration
	timer          *time.Timer
}

type Debouncer func(func())

// Creates a debouncer that will stop the previous timer when the time that
// has passed is below the threshold.
// For example: if we set it to 5 mins and only few seconds passed, it will stop
// the previous one and create a new timer. On the other hand, if several minutes passed
// it will not stop the previous and only issue a new one.
func Debounce(delay time.Duration, threshold float64) Debouncer {
	thresholdDelay := time.Duration(float64(delay.Microseconds())*threshold) * time.Microsecond

	d := &debouncer{
		delay:          delay,
		thresholdDelay: thresholdDelay,
	}

	return func(f func()) {
		d.set(f)
	}
}

func (d *debouncer) set(f func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// The previous timer will be GC'ed when timer fires.
	previousTimer := d.timer
	previousStart := d.start

	// Create new timer
	d.timer = time.AfterFunc(d.delay, f)
	d.start = time.Now()

	// Stop the previous one when x percentage of the delay has passed
	if previousTimer != nil && (d.thresholdDelay == 0 || time.Since(previousStart) < d.thresholdDelay) {
		previousTimer.Stop()
	}
}
