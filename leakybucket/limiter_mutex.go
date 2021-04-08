package leakybucket

import (
	"sync"
	"time"
)

type mutexLimiter struct {
	sync.Mutex
	last       time.Time
	sleepFor   time.Duration
	perRequest time.Duration
	maxSlack   time.Duration
	clock      Clock
}

// newMutexBased returns a new atomic based limiter.
func newMutexBased(rate int, opts ...Option) *mutexLimiter {
	c := buildConfig(opts...)
	perRequest := c.per / time.Duration(rate)
	l := &mutexLimiter{
		last:       time.Time{},
		sleepFor:   0,
		perRequest: perRequest,
		maxSlack:   -1 * time.Duration(c.slack) * perRequest,
		clock:      c.clock,
	}
	return l
}

// Take blocks to ensure that the time spent between multiple
// Take calls is on average time.Second/rate.
func (l *mutexLimiter) Take() time.Time {
	l.Lock()
	defer l.Unlock()

	now := l.clock.Now()

	// first.
	if l.last.IsZero() {
		l.last = now
		return l.last
	}

	// sleepFor calculates how much time we should sleep based on
	// the perRequest budget and how long the last request took.
	// Since the request may take longer than the budget, this number
	// can get negative, and is summed across requests.
	l.sleepFor += l.perRequest - now.Sub(l.last)

	// We shouldn't allow sleepFor to get too negative, since it would mean that
	// a service that slowed down a lot for a short period of time would get
	// a much higher RPS following that.
	if l.sleepFor < l.maxSlack {
		l.sleepFor = l.maxSlack
	}

	// If sleepFor is positive, then we should sleep now.
	if l.sleepFor > 0 {
		l.clock.Sleep(l.sleepFor)
		l.last = now.Add(l.sleepFor)
		l.sleepFor = 0
	} else {
		l.last = now
	}

	return l.last
}
