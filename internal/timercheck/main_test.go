package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fakeTimers makes every wait of d take d*slowdown on a clock that only moves when waiting.
type fakeTimers struct {
	now      time.Time
	slowdown int
}

func (f *fakeTimers) Now() time.Time { return f.now }

func (f *fakeTimers) Wait(d time.Duration) { f.now = f.now.Add(d * time.Duration(f.slowdown)) }

// A Docker Desktop VM up for hours fired a 10 ms timer after 92 ms, and timing
// tests and the conformance run failed with timeouts for no code reason. The check measures what
// one 10 ms wait really takes and warns above 20 ms.
func TestMeasure(t *testing.T) {
	t.Parallel()
	healthy := &fakeTimers{now: time.Unix(0, 0), slowdown: 1}
	assert.Equal(t, 10*time.Millisecond, measure(healthy, 10*time.Millisecond, 10))
	degraded := &fakeTimers{now: time.Unix(0, 0), slowdown: 9}
	assert.Equal(t, 180*time.Millisecond, measure(degraded, 20*time.Millisecond, 5))
}

func TestWarning(t *testing.T) {
	t.Parallel()
	assert.Empty(t, warning(11*time.Millisecond))
	assert.Empty(t, warning(20*time.Millisecond))
	w := warning(92 * time.Millisecond)
	assert.Contains(t, w, "a 10ms timer takes 92ms")
	assert.Contains(t, w, "wsl --shutdown")
}

func TestReport(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "timercheck: a 10ms timer takes 10.12ms (ok)\n", report(10123*time.Microsecond))
	assert.Equal(t, "timercheck: a 10ms timer takes 92ms (SLOW)\n", report(92*time.Millisecond))
}
