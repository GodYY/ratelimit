package tokenbucket

import (
	"math"
	"strconv"
	"sync"
	"time"
)

// The algorithm that this implementation uses does computational work
// only when tokens are removed from the bucket, and that work completes
// in short, bounded-constant time (Bucket.Wait benchmarks at 175ns on
// my laptop).
//
// Time is measured in equal measured ticks, a given interval
// (fillInterval) apart. On each tick a number of tokens (quantum) are
// added to the bucket.
//
// When any of the methods are called the bucket updates the number of
// tokens that are in the bucket, and it records the current tick
// number too. Note that it doesn't record the current time - by
// keeping things in units of whole ticks, it's easy to dish out tokens
// at exactly the right intervals as measured from the start time.
//
// This allows us to calculate the number of tokens that will be
// available at some time in the future with a few simple arithmetic
// operations.
//
// The main reason for being able to transfer multiple tokens on each tick
// is so that we can represent rates greater than 1e9 (the resolution of the Go
// time package) tokens per second, but it's also useful because
// it means we can easily represent situations like "a person gets
// five tokens an hour, replenished on the hour".

// Limiter represents a token bucket that fills at a predetermined rate.
// Methods on Limiter may be called concurrently.
type Limiter struct {
	clock Clock

	// startTime holds the moment when the bucket was
	// first created and ticks began.
	startTime time.Time

	// capacity holds the overall capacity of the bucket.
	capacity int64

	// fillInterval holds the interval between each tick.
	fillInterval time.Duration

	// quantum holds how many tokens are added on
	// each tick.
	quantum int64

	// mtx guards the fields below it.
	mtx sync.Mutex

	// availableTokens holds the number of available
	// tokens as of the associated latestTick.
	// It will be negative when there are consumers
	// waiting for tokens.
	availableTokens int64

	// latestTick holds the latest tick for which
	// we know the number of tokens in the bucket.
	latestTick int64
}

// NewLimiter returns a new token bucket that fills at the
// rate of one token every fillInterval, up to the given
// maximum capacity. Both arguments must be
// positive. The bucket is initially full.
func NewLimiter(fillInterval time.Duration, capacity int64) *Limiter {
	return NewLimiterWithClock(fillInterval, capacity, nil)
}

// NewLimiterWithClock is identical to NewLimiter but injects a testable clock
// interface.
func NewLimiterWithClock(fillInterval time.Duration, capacity int64, clock Clock) *Limiter {
	return NewLimiterWithQuantumAndClock(fillInterval, 1, capacity, clock)
}

// rateMargin specifies the allowed variance of actual
// rate from specified rate. 1% seems reasonable.
const rateMargin = 0.01

// NewLimiterWithRate returns a token bucket that fills the bucket
// at the rate of rate tokens per second up to the given
// maximum capacity. Because of limited clock resolution,
// at high rates, the actual rate may be up to 1% different from the
// specified rate.
func NewLimiterWithRate(rate float64, capacity int64) *Limiter {
	return NewLimiterWithRateAndClock(rate, capacity, nil)
}

// NewLimiterWithRateAndClock is identical to NewLimiterWithRate but injects a
// testable clock interface.
func NewLimiterWithRateAndClock(rate float64, capacity int64, clock Clock) *Limiter {
	l := NewLimiterWithQuantumAndClock(1, 1, capacity, clock)
	for quantum := int64(1); quantum < 1<<50; quantum = nextQuantum(quantum) {
		fillInterval := time.Duration(1e9 * float64(quantum) / rate)
		if fillInterval <= 0 {
			continue
		}
		l.fillInterval = fillInterval
		l.quantum = quantum
		if diff := math.Abs(l.Rate() - rate); diff/rate <= rateMargin {
			return l
		}
	}
	panic("cannot find suitable quantum for " + strconv.FormatFloat(rate, 'g', -1, 64))
}

func nextQuantum(q int64) int64 {
	quantum := q * 11 / 10
	if quantum == q {
		quantum++
	}
	return quantum
}

// NewLimiterWithQuantumAndClock is similar to NewLimiter, but allows
// the specification of the quantum size - quantum tokens
// are added every fillInterval.
func NewLimiterWithQuantum(fillInterval time.Duration, quantum, capacity int64) *Limiter {
	return NewLimiterWithQuantumAndClock(fillInterval, quantum, capacity, nil)
}

// NewLimiterWithQuantumAndClock is like NewLimiterWithQuantum, but
// also has a clock argument that allows clients to fake the passing
// of time. If clock is nil, the system clock will be used.
func NewLimiterWithQuantumAndClock(fillInterval time.Duration, quantum, capacity int64, clock Clock) *Limiter {
	if clock == nil {
		clock = realClock{}
	}
	if fillInterval <= 0 {
		panic("token bucket fill interval is not > 0")
	}
	if capacity <= 0 {
		panic("token bucket capacity is not > 0")
	}
	if quantum <= 0 {
		panic("token bucket quantum is not > 0")
	}
	return &Limiter{
		clock:           clock,
		startTime:       clock.Now(),
		latestTick:      0,
		fillInterval:    fillInterval,
		capacity:        capacity,
		quantum:         quantum,
		availableTokens: capacity,
	}
}

func (l *Limiter) Capacity() int64 {
	return l.capacity
}

func (l *Limiter) Rate() float64 {
	return 1e9 * float64(l.quantum) / float64(l.fillInterval)
}

// Available returns the number of available tokens. It will be negative
// when there are consumers waiting for tokens. Note that if this
// returns greater than zero, it does not guarantee that calls that take
// tokens from the buffer will succeed, as the number of available
// tokens could have changed in the meantime. This method is intended
// primarily for metrics reporting and debugging.
func (l *Limiter) Available() int64 {
	return l.available(l.clock.Now())
}

func (l *Limiter) available(now time.Time) int64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.adjustAvailableTokens(l.currentTick(now))
	return l.availableTokens
}

const infinityDuration = time.Duration(0x7fffffffffffffff)

// Take takes count tokens from the bucket without blocking. It returns
// the time that the caller should wait until the tokens are actually
// available.
//
// Note that if the request is irrevocable - there is no way to return
// tokens to the bucket once this method commits us to taking them.
func (l *Limiter) Take(count int64) time.Duration {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	d, _ := l.take(l.clock.Now(), count, infinityDuration)
	return d
}

// TakeMaxDuration is like Take, except that
// it will only take tokens from the bucket if the wait
// time for the tokens is no greater than maxWait.
//
// If it would take longer than maxWait for the tokens
// to become available, it does nothing and reports false,
// otherwise it returns the time that the caller should
// wait until the tokens are actually available, and reports
// true.
func (l *Limiter) TakeMaxDuration(count int64, maxWait time.Duration) (time.Duration, bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.take(l.clock.Now(), count, maxWait)
}

// TakeAvailable takes up to count immediately available tokens from the
// bucket. It returns the number of tokens removed, or zero if there are
// no available tokens. It does not block.
func (l *Limiter) TakeAvailable(count int64) int64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.takeAvailable(l.clock.Now(), count)
}

// takeAvailable is the internal version of TakeAvailable - it takes the
// current time as an argument to enable easy testing.
func (l *Limiter) takeAvailable(now time.Time, count int64) int64 {
	if count <= 0 {
		return 0
	}

	l.adjustAvailableTokens(l.currentTick(now))
	if l.availableTokens <= 0 {
		return 0
	}

	if count > l.availableTokens {
		count = l.availableTokens
	}
	l.availableTokens -= count
	return count
}

// Wait takes count tokens from the bucket, waiting until they are
// available.
func (l *Limiter) Wait(count int64) {
	if d := l.Take(count); d > 0 {
		l.clock.Sleep(d)
	}
}

// WaitMaxDuration is like Wait except that it will
// only take tokens from the bucket if it needs to wait
// for no greater than maxWait. It reports whether
// any tokens have been removed from the bucket
// If no tokens have been removed, it returns immediately.
func (l *Limiter) WaitMaxDuration(count int64, maxWait time.Duration) bool {
	d, ok := l.TakeMaxDuration(count, maxWait)
	if ok {
		l.clock.Sleep(d)
	}
	return ok
}

// take is the internal version of Take - it takes the current time as
// an argument to enable easy testing.
func (l *Limiter) take(now time.Time, count int64, maxWait time.Duration) (time.Duration, bool) {
	if count <= 0 {
		return 0, true
	}

	tick := l.currentTick(now)
	l.adjustAvailableTokens(tick)
	avail := l.availableTokens - count
	if avail >= 0 {
		l.availableTokens = avail
		return 0, true
	}

	endTick := tick + (-avail+l.quantum-1)/l.quantum
	endTime := l.startTime.Add(time.Duration(endTick) * l.fillInterval)
	waitTime := endTime.Sub(now)
	if waitTime > maxWait {
		return 0, false
	}

	l.availableTokens = avail
	return waitTime, true
}

// currentTick returns the current time tick, measured
// from tb.startTime.
func (l *Limiter) currentTick(now time.Time) int64 {
	return int64(now.Sub(l.startTime) / l.fillInterval)
}

// adjustavailableTokens adjusts the current number of tokens
// available in the bucket at the given time, which must
// be in the future (positive) with respect to tb.latestTick.
func (l *Limiter) adjustAvailableTokens(tick int64) {
	lastTick := l.latestTick
	l.latestTick = tick
	if l.availableTokens >= l.capacity {
		return
	}

	l.availableTokens += (tick - lastTick) * l.quantum
	if l.availableTokens > l.capacity {
		l.availableTokens = l.capacity
	}
}

type Clock interface {
	Now() time.Time

	Sleep(time.Duration)
}

// realClock implements Clock in terms of standard time functions.
type realClock struct{}

// Now implements Clock.Now by calling time.Now.
func (realClock) Now() time.Time {
	return time.Now()
}

// Now implements Clock.Sleep by calling time.Sleep.
func (realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}
