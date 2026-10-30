package core

// Regression tests for defects found in the pre-release review of v0.1.0. Each one failed before
// its fix; the comments point to the ADR that records the decision when there is one.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

// stuckProducer models a producer blocked in a call that does not take its context (an SDK call,
// a channel receive): it returns only when released.
func stuckProducer(release <-chan struct{}, wrote chan<- error) ProducerFunc {
	return func(_ context.Context, w Writer) error {
		if err := w.Write([]byte("partial")); err != nil {
			wrote <- err // the test reads wrote on every path: a failure, never a hang
			return err
		}
		<-release
		wrote <- w.Write([]byte("too late"))
		return nil
	}
}

// startStuck starts stuckProducer and returns once it is stuck: after its first event is stored
// and its supervisor timers are armed. A cancel, timeout or Close issued earlier could beat that
// first write, which then fails and ends the producer before it is stuck (this made the
// stuck-producer tests flaky: one hung once in make check, another failed 2 times in 600).
func startStuck(t *testing.T, x *harness, release <-chan struct{}, wrote chan<- error) string {
	t.Helper()
	id := x.start(stuckProducer(release, wrote))
	x.clock.AwaitWaiters(t, supervisorWaiters)
	testutil.Eventually(t, func() bool {
		st, err := x.hub.Status(context.Background(), id)
		require.NoError(t, err)
		return st.LastSeq == 1
	}, waitLimit)
	return id
}

// advanceBeating advances the clock n times by step, waiting after each step until the producer's
// heartbeat has recorded the new time (as it would on a real clock) or the stream has ended.
func advanceBeating(t *testing.T, x *harness, id string, n int, step time.Duration) {
	t.Helper()
	for range n {
		x.clock.Advance(step)
		now := x.clock.Now()
		testutil.Eventually(t, func() bool {
			st, err := x.hub.Status(context.Background(), id)
			require.NoError(t, err)
			return st.State.IsTerminal() || !st.LastActivity.Before(now)
		}, waitLimit)
	}
}

// G5, spec §3.1: a producer that ignores its context is abandoned one HeartbeatInterval after
// the cause, the terminal for the cause is written, heartbeats stop, and its late writes fail.
func TestRegression_StuckProducerIsAbandoned(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		code string
		// cause triggers the cancellation and waits until the Hub has armed its grace timer, so the
		// fake clock never moves past the grace period before the timer exists.
		cause func(t *testing.T, x *harness, id string)
		// bound is when the terminal must exist: the cause plus one HeartbeatInterval of grace.
		bound time.Duration
	}{
		{"timeout", codeTimeout, func(t *testing.T, x *harness, id string) {
			advanceBeating(t, x, id, 30, 2*time.Second) // to MaxDuration: the deadline timer fires
			x.clock.AwaitWaiters(t, 2)                  // heartbeat + grace (the deadline is spent)
		}, time.Minute + 2*time.Second},
		{"cancel", codeCancelled, func(t *testing.T, x *harness, id string) {
			require.NoError(t, x.hub.Cancel(context.Background(), id))
			x.clock.AwaitWaiters(t, supervisorWaiters+1) // heartbeat + deadline + grace
		}, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, WithMaxDuration(time.Minute))
			release := make(chan struct{})
			wrote := make(chan error, 1)
			id := startStuck(t, x, release, wrote)
			tc.cause(t, x, id)
			advanceBeating(t, x, id, 1, 2*time.Second) // the grace period
			st := x.awaitTerminal(id)
			assert.Equal(t, tc.code, st.ErrorCode)
			assert.Equal(t, uint64(2), st.LastSeq, "one data event, then the terminal")
			assert.LessOrEqual(t, st.LastActivity.Sub(t0), tc.bound, "terminal written in time (G5)")
			testutil.Eventually(t, func() bool { return x.clock.Waiters() == 0 }, waitLimit) // no heartbeat left
			close(release)
			require.ErrorIs(t, <-wrote, ErrClosed, "the abandoned producer's writes fail")
		})
	}
}

// Instance B's clock is 5 s ahead (StaleAfter 3 s). A healthy producer on A is not declared
// lost by a reader on B, and a dead one still is, after StaleAfter of B's own observation.
func TestRegression_ClockSkewDoesNotEndHealthyStreams(t *testing.T) {
	t.Parallel()
	instances := miniredisInstances(t)
	cA := testutil.NewClock(t0)
	cB := testutil.NewClock(t0.Add(5 * time.Second))
	opts := []Option{WithHeartbeatInterval(time.Second), WithStaleAfter(3 * time.Second), WithIdleTTL(time.Hour)}
	a := newHarnessOn(t, cA, instances(t, cA), opts...)
	b := newHarnessOn(t, cB, instances(t, cB), opts...)
	ctx := context.Background()

	id := a.start(func(pctx context.Context, w Writer) error {
		if err := w.Write([]byte("hello")); err != nil {
			return err
		}
		<-pctx.Done() // alive; no time passes on A, so its liveness is current
		return nil
	})
	testutil.Eventually(t, func() bool {
		st, err := a.hub.Status(ctx, id)
		require.NoError(t, err)
		return st.LastSeq == 1
	}, waitLimit)
	sub, err := b.hub.Subscribe(ctx, id, 1)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	nctx, cancel := context.WithTimeout(ctx, 3*time.Second) // B's read block is StaleAfter/3 = 1 s
	defer cancel()
	ev, err := sub.Next(nctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "healthy producer ended by a skewed reader: %+v", ev)

	// A producer that is really gone: an orphan stream written with A's clock, read on B.
	const orphan = "orphan"
	require.NoError(t, a.store.Create(ctx, orphan, store.Meta{CreatedAt: cA.Now(), IdleTTL: time.Hour, Retention: time.Hour}))
	_, err = a.store.Append(ctx, orphan, store.AppendRequest{Kind: "data", Data: []byte("last"), At: cA.Now()})
	require.NoError(t, err)
	osub, err := b.hub.Subscribe(ctx, orphan, 1)
	require.NoError(t, err)
	defer func() { _ = osub.Close() }()
	start := cB.Now()
	got := make(chan Event, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ev, err := osub.Next(ctx)
		assert.NoError(t, err)
		got <- ev
	}()
	advanceUntil(t, cB, 500*time.Millisecond, done)
	code, _ := terminalCode(t, <-got)
	assert.Equal(t, codeProducerLost, code)
	assert.Greater(t, cB.Now().Sub(start), 3*time.Second, "declared lost only after StaleAfter of B's own time")
}

// Close(ctx) returns about one second after its deadline even when the store hangs on the
// server_shutdown writes (they ran one by one with no deadline before).
func TestRegression_CloseReturnsNearItsDeadlineOnHangingStore(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	hang := make(chan struct{})
	fs := &faultyStore{Store: mem}
	fs.append = func(ctx context.Context, id string, ev store.AppendRequest) (uint64, error) {
		if ev.Force {
			select { // a store call that honours its context, as go-redis does
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-hang:
			}
		}
		return mem.Append(ctx, id, ev)
	}
	x := newHarnessOn(t, c, fs)
	t.Cleanup(func() { close(hang) }) // runs before the harness cleanup (LIFO)
	for range 40 {                    // more than maxShutdownWriters
		x.start(blockUntilCancelled)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- x.hub.Close(ctx) }()
	select {
	case err := <-closed:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(shutdownWriteTimeout + time.Second):
		t.Fatal("Close did not return within shutdownWriteTimeout of its deadline")
	}
}

// OnStreamEnd reports the terminal event that clients received, also when another writer won
// the terminal race (158 of 200 races disagreed before).
func TestRegression_OnStreamEndReportsStoredTerminal(t *testing.T) {
	t.Parallel()
	sc := stressStores()[0]
	mismatches := []string{}
	for range 200 {
		ev, reason := terminalRace(t, sc)
		stored := ev.Kind
		if ev.Kind == store.KindError {
			stored, _ = terminalCode(t, ev)
		}
		if stored != reason {
			mismatches = append(mismatches, "hook="+reason+" stored="+stored)
		}
	}
	assert.Zero(t, len(mismatches), "%d of 200 races: %s", len(mismatches), strings.Join(mismatches[:min(5, len(mismatches))], ", "))
}

// ADR 0004: MaxEvents is a hard cap. A producer that exceeds it gets ErrTooManyEvents (and
// ErrClosed), the stream ends with max_events_exceeded, nothing is trimmed, and every reader,
// including one starting from 0, reads it completely.
func TestRegression_MaxEventsEndsTheStreamAndKeepsItReadableFromZero(t *testing.T) {
	t.Parallel()
	for _, sc := range stressStores() {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			c := testutil.NewClock(t0)
			x := newHarnessOn(t, c, sc.open(t, c), WithMaxEvents(5))
			writeErr := make(chan error, 1)
			id := x.start(func(_ context.Context, w Writer) error {
				for i := range 300 {
					if err := w.Write([]byte{byte('a' + i%26)}); err != nil {
						writeErr <- err
						return err
					}
				}
				return nil
			})
			err := <-writeErr
			require.ErrorIs(t, err, ErrTooManyEvents)
			require.ErrorIs(t, err, ErrClosed)
			st := x.awaitTerminal(id)
			assert.Equal(t, codeMaxEvents, st.ErrorCode)
			assert.Equal(t, uint64(1), st.FirstSeq, "never trimmed")
			assert.Equal(t, uint64(6), st.LastSeq, "5 events and the terminal")

			evs := x.collect(id, 0) // the restart from 0 that the old 410 recovery could not do
			require.Len(t, evs, 6)
			assert.Equal(t, "abcde", string(evs[0].Data)+string(evs[1].Data)+string(evs[2].Data)+string(evs[3].Data)+string(evs[4].Data))
			code, _ := terminalCode(t, evs[5])
			assert.Equal(t, codeMaxEvents, code)
		})
	}
}

// ADR 0005: a store error that matches io.EOF (go-redis reports a closed connection that
// way) never reaches the caller of Next as io.EOF, the end-of-stream signal.
func TestRegression_StoreEOFIsNotEndOfStream(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	x := newHarnessOn(t, c, &eofStore{Store: mem})
	id := x.start(blockUntilCancelled)
	sub, err := x.hub.Subscribe(context.Background(), id, 0)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	_, err = sub.Next(context.Background())
	require.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF), "Next returned %q, which matches io.EOF", err)
	assert.Contains(t, err.Error(), "EOF", "the cause is still described")
}

// eofStore fails every Read the way go-redis reports a connection closed by the server.
type eofStore struct{ store.Store }

func (eofStore) Read(context.Context, string, uint64, int, time.Duration) ([]store.Event, error) {
	return nil, io.EOF
}

// ADR 0008: an IdleTTL shorter than StaleAfter + HeartbeatInterval is rejected by New; a live
// producer's stream could otherwise expire between two heartbeats.
func TestRegression_IdleTTLShorterThanLivenessIsRejected(t *testing.T) {
	t.Parallel()
	_, err := New(memory.New(), WithHeartbeatInterval(2*time.Second), WithStaleAfter(15*time.Second), WithIdleTTL(time.Second))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IdleTTL")
	_, err = New(memory.New(), WithHeartbeatInterval(2*time.Second), WithStaleAfter(15*time.Second), WithIdleTTL(17*time.Second))
	require.NoError(t, err, "StaleAfter + HeartbeatInterval is enough")
}

// ADR 0007: payloads are normalized to UTF-8 with LF before they are stored, so readers get
// exactly what is stored, whatever the transport.
func TestRegression_PayloadsAreNormalizedBeforeStorage(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	in := []string{"a\rb", "c\r\nd", "e\n", "\xff\xfe ok", "déjà vu", ""}
	want := []string{"a\nb", "c\nd", "e\n", "� ok", "déjà vu", ""}
	id := x.start(writeWords(in...))
	x.awaitTerminal(id)
	evs := x.collect(id, 0)
	require.Len(t, evs, len(in)+1)
	for i, w := range want {
		assert.Equal(t, w, string(evs[i].Data), "event %d", i+1)
	}
}

func writeWords(words ...string) ProducerFunc {
	return func(_ context.Context, w Writer) error {
		for _, word := range words {
			if err := w.Write([]byte(word)); err != nil {
				return err
			}
		}
		return nil
	}
}

// ADR 0008: Status, Subscribe and Cancel answer ErrNotFound for malformed ids without
// calling the store.
func TestRegression_MalformedIDsNeverReachTheStore(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	calls := 0
	fs := &faultyStore{Store: mem, status: func(ctx context.Context, id string) (store.Status, error) {
		calls++
		return mem.Status(ctx, id)
	}}
	x := newHarnessOn(t, c, fs)
	ctx := context.Background()
	for _, id := range []string{"", "a}b", "{x}", "a/b", "é", strings.Repeat("a", 129)} {
		_, err := x.hub.Status(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, "%q", id)
		_, err = x.hub.Subscribe(ctx, id, 0)
		require.ErrorIs(t, err, ErrNotFound, "%q", id)
		require.ErrorIs(t, x.hub.Cancel(ctx, id), ErrNotFound, "%q", id)
	}
	assert.Zero(t, calls)
}

// ADR 0006: after Close's deadline, a producer that ignores its context stops heartbeating
// one HeartbeatInterval later, even though its goroutine is still running.
func TestRegression_CloseStopsHeartbeatsOfAbandonedProducers(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	var touches countingTouches
	fs := &faultyStore{Store: mem, touch: func(ctx context.Context, id string, at time.Time) error {
		touches.inc()
		return mem.Touch(ctx, id, at)
	}}
	x := newHarnessOn(t, c, fs)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	startStuck(t, x, release, make(chan error, 1))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, x.hub.Close(ctx), context.Canceled)
	x.clock.AwaitWaiters(t, supervisorWaiters+1) // heartbeat, MaxDuration and the grace timer
	x.clock.Advance(2 * time.Second)             // the grace period: one HeartbeatInterval
	testutil.Eventually(t, func() bool { return x.clock.Waiters() == 0 }, waitLimit)
	before := touches.get()
	x.clock.Advance(time.Minute)
	assert.Equal(t, before, touches.get(), "no heartbeat after the producer was abandoned")
}

type countingTouches struct {
	mu sync.Mutex
	n  int
}

func (c *countingTouches) inc() { c.mu.Lock(); c.n++; c.mu.Unlock() }

func (c *countingTouches) get() int { c.mu.Lock(); defer c.mu.Unlock(); return c.n }
