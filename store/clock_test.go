package store_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bailalayoub/rebound/store"
)

// waitLimit bounds every receive on a real timer so a broken clock fails instead of hanging.
const waitLimit = 5 * time.Second

func TestRealClockNow(t *testing.T) {
	t.Parallel()
	before := time.Now()
	got := store.RealClock{}.Now()
	assert.False(t, got.Before(before))
	assert.False(t, got.After(time.Now()))
}

func TestRealClockTimer(t *testing.T) {
	t.Parallel()
	var c store.Clock = store.RealClock{}

	fired := c.NewTimer(0)
	select {
	case <-fired.C():
	case <-time.After(waitLimit):
		t.Fatal("zero-duration timer did not fire")
	}
	assert.False(t, fired.Stop(), "Stop after fire reports inactive")

	long := c.NewTimer(time.Hour)
	assert.True(t, long.Stop(), "Stop before fire reports active")
	assert.False(t, long.Reset(time.Millisecond), "Reset of a stopped timer reports inactive")
	select {
	case <-long.C():
	case <-time.After(waitLimit):
		t.Fatal("reset timer did not fire")
	}
}

func TestRealClockTicker(t *testing.T) {
	t.Parallel()
	for _, period := range []time.Duration{time.Millisecond, 0, -time.Second} {
		tk := store.RealClock{}.NewTicker(period)
		for range 2 {
			select {
			case <-tk.C():
			case <-time.After(waitLimit):
				t.Fatalf("ticker with period %v did not tick", period)
			}
		}
		tk.Stop()
	}
}
