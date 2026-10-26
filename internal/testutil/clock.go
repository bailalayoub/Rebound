package testutil

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bailalayoub/rebound/store"
)

// minTickerPeriod mirrors store.RealClock: non-positive ticker periods are raised to 1 ms.
const minTickerPeriod = time.Millisecond

// awaitTimeout bounds AwaitWaiters so a goroutine that never arms its timer fails the test.
const awaitTimeout = 5 * time.Second

// Clock is a manual store.Clock. Time moves only when Advance is called; timers and tickers that
// fall due fire inside Advance, in deadline order, with the deadline as their value. Channels hold
// one value and drop the rest, like the standard library's tickers. It is safe for concurrent use.
type Clock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter // armed timers and tickers, in creation order
}

var _ store.Clock = (*Clock)(nil)

// NewClock returns a clock reading start.
func NewClock(start time.Time) *Clock {
	return &Clock{now: start}
}

// Now returns the clock's current time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d, firing every timer and ticker that falls due on the way.
// Negative durations are ignored: the clock never goes back.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d < 0 {
		d = 0
	}
	target := c.now.Add(d)
	for {
		w := c.nextDueLocked(target)
		if w == nil {
			break
		}
		c.now = w.when
		c.fireLocked(w)
	}
	c.now = target
}

// Waiters returns the number of armed timers and tickers.
func (c *Clock) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// AwaitWaiters blocks until at least n timers or tickers are armed, so that a test advances the
// clock only after the code under test has started waiting. It fails t after 5 s.
func (c *Clock) AwaitWaiters(t testing.TB, n int) {
	t.Helper()
	Eventually(t, func() bool { return c.Waiters() >= n }, awaitTimeout)
}

// NewTimer returns a timer that fires once the clock has advanced by d. A non-positive d fires
// immediately.
func (c *Clock) NewTimer(d time.Duration) store.Timer {
	w := &waiter{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armLocked(w, d)
	return fakeTimer{w}
}

// NewTicker returns a ticker that fires every d of clock time. Non-positive periods are raised to
// 1 ms, as in store.RealClock.
func (c *Clock) NewTicker(d time.Duration) store.Ticker {
	if d <= 0 {
		d = minTickerPeriod
	}
	w := &waiter{clock: c, ch: make(chan time.Time, 1), period: d}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armLocked(w, d)
	return fakeTicker{w}
}

// nextDueLocked returns the armed waiter with the earliest deadline not after target; ties go to
// the oldest waiter.
func (c *Clock) nextDueLocked(target time.Time) *waiter {
	var next *waiter
	for _, w := range c.waiters {
		if w.when.After(target) {
			continue
		}
		if next == nil || w.when.Before(next.when) {
			next = w
		}
	}
	return next
}

// fireLocked delivers w's deadline, then re-arms a ticker or disarms a timer.
func (c *Clock) fireLocked(w *waiter) {
	select {
	case w.ch <- w.when:
	default: // channel full: drop the tick, as time.Ticker does.
	}
	if w.period > 0 {
		w.when = w.when.Add(w.period)
		return
	}
	c.disarmLocked(w)
}

func (c *Clock) armLocked(w *waiter, d time.Duration) {
	w.when = c.now.Add(d)
	if d <= 0 {
		c.fireLocked(w)
		return
	}
	if !c.armedLocked(w) {
		c.waiters = append(c.waiters, w)
	}
}

func (c *Clock) disarmLocked(w *waiter) bool {
	i := slices.Index(c.waiters, w)
	if i < 0 {
		return false
	}
	c.waiters = slices.Delete(c.waiters, i, i+1)
	return true
}

func (c *Clock) armedLocked(w *waiter) bool {
	return slices.Contains(c.waiters, w)
}

// waiter is an armed or disarmed deadline, shared by fakeTimer and fakeTicker.
type waiter struct {
	clock  *Clock
	ch     chan time.Time
	when   time.Time     // next deadline; guarded by clock.mu
	period time.Duration // 0 for timers
}

// stop disarms the waiter and drops any pending value (Go 1.23 timer semantics). It reports
// whether the waiter was armed.
func (w *waiter) stop() bool {
	w.clock.mu.Lock()
	defer w.clock.mu.Unlock()
	w.drain()
	return w.clock.disarmLocked(w)
}

// reset re-arms the waiter to fire after d, dropping any pending value. It reports whether the
// waiter was armed.
func (w *waiter) reset(d time.Duration) bool {
	w.clock.mu.Lock()
	defer w.clock.mu.Unlock()
	w.drain()
	wasArmed := w.clock.armedLocked(w)
	w.clock.armLocked(w, d)
	return wasArmed
}

func (w *waiter) drain() {
	select {
	case <-w.ch:
	default:
	}
}

// fakeTimer is the store.Timer returned by Clock.NewTimer.
type fakeTimer struct{ w *waiter }

// C returns the channel the timer fires on.
func (t fakeTimer) C() <-chan time.Time { return t.w.ch }

// Stop disarms the timer and reports whether it was armed.
func (t fakeTimer) Stop() bool { return t.w.stop() }

// Reset re-arms the timer to fire after d and reports whether it was armed.
func (t fakeTimer) Reset(d time.Duration) bool { return t.w.reset(d) }

// fakeTicker is the store.Ticker returned by Clock.NewTicker.
type fakeTicker struct{ w *waiter }

// C returns the channel the ticks are delivered on.
func (t fakeTicker) C() <-chan time.Time { return t.w.ch }

// Stop disarms the ticker.
func (t fakeTicker) Stop() { t.w.stop() }
