package testutil

import (
	"testing"
	"time"
)

// pollInterval is the step of Eventually (docs/07: 10 ms polling helpers are allowed).
const pollInterval = 10 * time.Millisecond

// Eventually polls cond every 10 ms until it returns true, and fails t if that has not happened
// within timeout. It reports whether cond became true. Use it only for goroutine hand-offs that
// cannot be observed through a channel or the fake clock.
func Eventually(t testing.TB, cond func() bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("testutil: condition not met within %v", timeout)
			return false
		}
		<-tick.C
	}
}
