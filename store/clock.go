package store

import "time"

// Clock is the time source of the Hub and of stores. Production code uses RealClock; tests inject
// a manual clock so that timeouts, heartbeats and TTLs are deterministic.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
}

// Timer mirrors *time.Timer with Go 1.23 semantics: after Stop or Reset returns, no stale value
// is received from C.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Ticker mirrors *time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// minTickerPeriod replaces non-positive ticker periods: time.NewTicker panics on them and
// library code must never panic.
const minTickerPeriod = time.Millisecond

// RealClock is the wall clock. Its zero value is ready to use.
type RealClock struct{}

// Now returns time.Now().
func (RealClock) Now() time.Time { return time.Now() }

// NewTimer returns a timer backed by time.NewTimer.
func (RealClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

// NewTicker returns a ticker backed by time.NewTicker. Non-positive periods are raised to 1 ms.
func (RealClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		d = minTickerPeriod
	}
	return realTicker{time.NewTicker(d)}
}

type realTimer struct{ t *time.Timer }

// C returns the channel the timer fires on.
func (r realTimer) C() <-chan time.Time { return r.t.C }

// Stop prevents the timer from firing and reports whether it was active.
func (r realTimer) Stop() bool { return r.t.Stop() }

// Reset re-arms the timer to fire after d and reports whether it was active.
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

type realTicker struct{ t *time.Ticker }

// C returns the channel the ticks are delivered on.
func (r realTicker) C() <-chan time.Time { return r.t.C }

// Stop turns the ticker off.
func (r realTicker) Stop() { r.t.Stop() }
