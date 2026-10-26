// Command timercheck warns when this machine's Go timers fire late. A Docker Desktop VM up for
// hours was seen firing a 10 ms timer after 92 ms: timing tests and the conformance run then fail
// with timeouts although the code is fine. scripts/in-docker.sh runs it before every target. It
// only warns (stderr) and always exits 0.
package main

import (
	"fmt"
	"os"
	"time"
)

const (
	sample  = 10 * time.Millisecond
	samples = 10
)

// timers is the clock measure uses; tests replace it.
type timers interface {
	Now() time.Time
	Wait(d time.Duration)
}

type realTimers struct{}

func (realTimers) Now() time.Time { return time.Now() }

func (realTimers) Wait(d time.Duration) {
	t := time.NewTimer(d)
	<-t.C
}

// measure returns the mean duration of n waits of d.
func measure(clock timers, d time.Duration, n int) time.Duration {
	start := clock.Now()
	for range n {
		clock.Wait(d)
	}
	return clock.Now().Sub(start) / time.Duration(n)
}

// warning explains a slow timer, or returns "" when a wait of sample took at most twice as long.
func warning(took time.Duration) string {
	if took <= 2*sample {
		return ""
	}
	return fmt.Sprintf("timercheck: timers are slow here: a %v timer takes %v.\n"+
		"timercheck: timing tests and the conformance run may fail with timeouts for no code reason.\n"+
		"timercheck: on Docker Desktop, restart its VM: docker desktop stop; wsl --shutdown; docker desktop start\n",
		sample, took.Round(time.Millisecond))
}

func main() {
	if w := warning(measure(realTimers{}, sample, samples)); w != "" {
		_, _ = fmt.Fprint(os.Stderr, w) // best effort: a warning, never a failure
	}
}
