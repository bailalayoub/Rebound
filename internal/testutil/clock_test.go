package testutil_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
)

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

// recv returns the pending value of ch, or false if there is none. Fake-clock channels are filled
// synchronously by Advance, so no waiting is needed.
func recv(ch <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		return time.Time{}, false
	}
}

func TestClock_NowAndAdvance(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	assert.Equal(t, t0, c.Now())
	c.Advance(90 * time.Second)
	assert.Equal(t, t0.Add(90*time.Second), c.Now())
	c.Advance(-time.Hour)
	assert.Equal(t, t0.Add(90*time.Second), c.Now(), "the clock never goes back")
}

func TestClock_TimerFiresExactlyAtDeadline(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	tm := c.NewTimer(time.Second)
	assert.Equal(t, 1, c.Waiters())

	c.Advance(time.Second - time.Nanosecond)
	_, fired := recv(tm.C())
	require.False(t, fired, "fired before its deadline")

	c.Advance(time.Nanosecond)
	v, fired := recv(tm.C())
	require.True(t, fired)
	assert.Equal(t, t0.Add(time.Second), v)
	assert.Zero(t, c.Waiters(), "a fired timer is disarmed")

	c.Advance(time.Hour)
	_, fired = recv(tm.C())
	assert.False(t, fired, "a timer fires once")
}

func TestClock_TimerNonPositiveFiresImmediately(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	for _, d := range []time.Duration{0, -time.Second} {
		v, fired := recv(c.NewTimer(d).C())
		require.True(t, fired, "duration %v", d)
		assert.Equal(t, t0.Add(d), v)
	}
	assert.Zero(t, c.Waiters())
}

func TestClock_TimerStop(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	tm := c.NewTimer(time.Second)
	assert.True(t, tm.Stop(), "stopping an armed timer")
	assert.False(t, tm.Stop(), "stopping twice")
	c.Advance(time.Hour)
	_, fired := recv(tm.C())
	assert.False(t, fired)

	fired2 := c.NewTimer(time.Second)
	c.Advance(time.Second)
	assert.False(t, fired2.Stop(), "stopping a fired timer reports inactive")
	_, pending := recv(fired2.C())
	assert.False(t, pending, "Stop drops the pending value (Go 1.23 semantics)")
}

func TestClock_TimerReset(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	tm := c.NewTimer(time.Second)
	assert.True(t, tm.Reset(3*time.Second), "reset of an armed timer")
	c.Advance(2 * time.Second)
	_, fired := recv(tm.C())
	require.False(t, fired, "old deadline must not fire")
	c.Advance(time.Second)
	v, fired := recv(tm.C())
	require.True(t, fired)
	assert.Equal(t, t0.Add(3*time.Second), v)

	assert.False(t, tm.Reset(time.Second), "reset of a fired timer")
	c.Advance(time.Second)
	_, fired = recv(tm.C())
	assert.True(t, fired, "a reset timer fires again")

	c.Advance(time.Second) // nothing armed
	tm2 := c.NewTimer(time.Second)
	c.Advance(time.Second)
	assert.False(t, tm2.Reset(time.Minute), "fired, not yet received")
	_, pending := recv(tm2.C())
	assert.False(t, pending, "Reset drops the stale value")
	assert.True(t, tm2.Reset(0), "armed by the previous Reset")
	_, fired = recv(tm2.C())
	assert.True(t, fired, "reset to zero fires immediately")
	assert.Zero(t, c.Waiters())
}

func TestClock_TickerPeriodAndDrops(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	tk := c.NewTicker(time.Second)

	c.Advance(time.Second)
	v, ok := recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(time.Second), v)

	c.Advance(time.Second)
	v, ok = recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(2*time.Second), v)

	// Three periods without reading: the channel keeps the first tick and drops the others.
	c.Advance(3 * time.Second)
	v, ok = recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(3*time.Second), v)
	_, ok = recv(tk.C())
	assert.False(t, ok)

	tk.Stop()
	assert.Zero(t, c.Waiters())
	c.Advance(time.Hour)
	_, ok = recv(tk.C())
	assert.False(t, ok, "a stopped ticker never fires")
}

func TestClock_TickerNonPositivePeriod(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	tk := c.NewTicker(0)
	c.Advance(time.Millisecond)
	v, ok := recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(time.Millisecond), v)
}

func TestClock_FiresInDeadlineOrderWithDeadlineValues(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	late := c.NewTimer(2 * time.Second)
	early := c.NewTimer(time.Second)
	tk := c.NewTicker(1500 * time.Millisecond)
	c.Advance(3 * time.Second)

	v, ok := recv(early.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(time.Second), v)
	v, ok = recv(late.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(2*time.Second), v)
	v, ok = recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, t0.Add(1500*time.Millisecond), v, "first tick kept, second (3s) dropped")
	assert.Equal(t, t0.Add(3*time.Second), c.Now())
}

func TestClock_AwaitWaiters(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	got := make(chan time.Time)
	go func() {
		tm := c.NewTimer(time.Minute)
		got <- <-tm.C()
	}()
	c.AwaitWaiters(t, 1)
	c.Advance(time.Minute)
	select {
	case v := <-got:
		assert.Equal(t, t0.Add(time.Minute), v)
	case <-time.After(5 * time.Second):
		t.Fatal("timer goroutine did not observe the fire")
	}
}

func TestClock_ConcurrentUse(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				tm := c.NewTimer(time.Millisecond)
				_ = c.Now()
				tm.Reset(2 * time.Millisecond)
				tm.Stop()
				tk := c.NewTicker(time.Millisecond)
				tk.Stop()
			}
		}()
	}
	for range 100 {
		c.Advance(time.Millisecond)
	}
	wg.Wait()
	assert.Zero(t, c.Waiters())
}
