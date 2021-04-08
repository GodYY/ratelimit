// leaky-bucket rate limiter.
package leakybucket

import (
	"github.com/andres-erbsen/clock"
	"time"
)

type Limiter interface {
	Take() time.Time
}

// Clock is the minimum necessary interface to instantiate a rate limiter with
// a clock or mock clock, compatible with clocks created using
// github.com/andres-erbsen/clock.
type Clock interface {
	Now() time.Time
	Sleep(duration time.Duration)
}

// config configures a limiter.
type config struct {
	clock Clock
	slack int
	per   time.Duration
}

func New(rate int, opts ...Option) Limiter {
	return newAtomicBased(rate, opts...)
}

// buildConfig combines defaults with options.
func buildConfig(opts ...Option) config {
	c := config{
		clock: clock.New(),
		slack: 10,
		per:   time.Second,
	}

	for _, opt := range opts {
		opt.apply(&c)
	}

	return c
}

// Option configures a limiter.
type Option interface {
	apply(*config)
}

type clockOption struct {
	clock Clock
}

func (o clockOption) apply(c *config) {
	c.clock = o.clock
}

// WithClock returns an option for New that provides an alternate
// Clock implementation, typically a mock Clock for testing.
func WithClock(c Clock) Option {
	return clockOption{clock: c}
}

type slackOption int

func (o slackOption) apply(c *config) {
	c.slack = int(o)
}

// WithSlack configures custom slack.
// Slack allows the limiter to accumulate "unspent" requests
// for future bursts of traffic.
func WithSlack(slack int) Option {
	return slackOption(slack)
}

// WithoutSlack configures the limiter to be strict and not to accumulate
// previously "unspent" requests for future bursts of traffic.
var WithoutSlack Option = slackOption(0)

type perOption time.Duration

func (o perOption) apply(c *config) {
	c.per = time.Duration(o)
}

func WithPer(per time.Duration) Option {
	return perOption(per)
}
