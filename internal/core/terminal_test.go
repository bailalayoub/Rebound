package core

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

// supervisorWaiters is the number of clock waiters of a running producer: heartbeat + deadline.
const supervisorWaiters = 2

// lastEvent returns the terminal event of a finished stream.
func (x *harness) lastEvent(id string) Event {
	x.t.Helper()
	evs := x.collect(id, 0)
	require.NotEmpty(x.t, evs)
	return evs[len(evs)-1]
}

// advanceUntil advances the clock by step until done is closed, failing after waitLimit.
func advanceUntil(t *testing.T, c *testutil.Clock, step time.Duration, done <-chan struct{}) {
	t.Helper()
	deadline := time.After(waitLimit)
	for {
		select {
		case <-done:
			return
		case <-deadline:
			t.Fatal("condition not reached while advancing the clock")
		case <-time.After(time.Millisecond):
			c.Advance(step)
		}
	}
}

func TestTerminal_ProducerFailed_HidesMessageByDefault(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	id := x.start(func(context.Context, Writer) error { return errors.New("db password wrong") })
	st := x.awaitTerminal(id)
	assert.Equal(t, StateError, st.State)
	assert.Equal(t, codeProducerFailed, st.ErrorCode)
	code, msg := terminalCode(t, x.lastEvent(id))
	assert.Equal(t, codeProducerFailed, code)
	assert.Equal(t, "producer failed", msg)
}

func TestTerminal_ProducerFailed_ExposedMessage(t *testing.T) {
	t.Parallel()
	x := newHarness(t, WithExposeProducerErrors(true))
	id := x.start(func(context.Context, Writer) error { return errors.New("model overloaded") })
	x.awaitTerminal(id)
	code, msg := terminalCode(t, x.lastEvent(id))
	assert.Equal(t, codeProducerFailed, code)
	assert.Equal(t, "model overloaded", msg)
}

func TestTerminal_ProducerPanic(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	id := x.start(func(_ context.Context, w Writer) error {
		if err := w.Write([]byte("partial")); err != nil {
			return err
		}
		panic("nil map write")
	})
	st := x.awaitTerminal(id)
	assert.Equal(t, codeProducerPanic, st.ErrorCode)
	evs := x.collect(id, 0)
	require.Len(t, evs, 2)
	code, msg := terminalCode(t, evs[1])
	assert.Equal(t, codeProducerPanic, code)
	assert.Equal(t, "producer panicked", msg, "the panic value is not sent to clients")
}

func TestTerminal_Timeout(t *testing.T) {
	t.Parallel()
	x := newHarness(t, WithMaxDuration(time.Minute))
	writeAfter := make(chan error, 1)
	id := x.start(func(ctx context.Context, w Writer) error {
		<-ctx.Done()
		writeAfter <- w.Write([]byte("too late"))
		return ctx.Err()
	})
	x.clock.AwaitWaiters(t, supervisorWaiters)
	x.clock.Advance(time.Minute - time.Millisecond)
	st, err := x.hub.Status(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, StateRunning, st.State, "still running just before MaxDuration")

	x.clock.Advance(time.Millisecond)
	require.ErrorIs(t, <-writeAfter, ErrClosed)
	assert.Equal(t, codeTimeout, x.awaitTerminal(id).ErrorCode)
	code, _ := terminalCode(t, x.lastEvent(id))
	assert.Equal(t, codeTimeout, code)
}

func TestTerminal_Timeout_PerStreamOverride(t *testing.T) {
	t.Parallel()
	x := newHarness(t, WithMaxDuration(time.Hour))
	id := x.start(blockUntilCancelled, WithStreamMaxDuration(10*time.Second))
	x.clock.AwaitWaiters(t, supervisorWaiters)
	x.clock.Advance(10 * time.Second)
	assert.Equal(t, codeTimeout, x.awaitTerminal(id).ErrorCode)
}

func TestTerminal_Cancelled_LocalFastPath(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	writeAfter := make(chan error, 1)
	id := x.start(func(ctx context.Context, w Writer) error {
		<-ctx.Done()
		writeAfter <- w.Write([]byte("ignored"))
		return nil
	})
	require.NoError(t, x.hub.Cancel(context.Background(), id))
	require.NoError(t, x.hub.Cancel(context.Background(), id), "Cancel is idempotent")

	err := <-writeAfter
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, err, ErrCancelled)
	st := x.awaitTerminal(id)
	assert.True(t, st.Cancelled)
	assert.Equal(t, codeCancelled, st.ErrorCode)
	code, msg := terminalCode(t, x.lastEvent(id))
	assert.Equal(t, codeCancelled, code)
	assert.Equal(t, "cancelled by client", msg)
}

// Cancel from another Hub (another instance) reaches the producer through its heartbeat.
func TestTerminal_Cancelled_FromAnotherHub(t *testing.T) {
	t.Parallel()
	a := newHarness(t, WithHeartbeatInterval(2*time.Second))
	b := newHarnessOn(t, a.clock, a.store)
	id := a.start(blockUntilCancelled)
	a.clock.AwaitWaiters(t, supervisorWaiters)

	require.NoError(t, b.hub.Cancel(context.Background(), id))
	st, err := a.hub.Status(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, StateRunning, st.State, "the producer only notices at its next heartbeat")

	a.clock.Advance(2 * time.Second)
	assert.Equal(t, codeCancelled, a.awaitTerminal(id).ErrorCode)
}

// A cancel flag set behind the Hub's back is noticed by the next write.
func TestTerminal_Cancelled_SeenByWrite(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	wrote, cancelled := make(chan struct{}), make(chan struct{})
	secondWrite := make(chan error, 1)
	id := x.start(func(_ context.Context, w Writer) error {
		if err := w.Write([]byte("a")); err != nil {
			return err
		}
		close(wrote)
		<-cancelled
		err := w.Write([]byte("b"))
		secondWrite <- err
		return err
	})
	<-wrote
	require.NoError(t, x.store.Cancel(context.Background(), id))
	close(cancelled)

	err := <-secondWrite
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, err, ErrCancelled)
	assert.Equal(t, codeCancelled, x.awaitTerminal(id).ErrorCode)
	assert.Equal(t, uint64(2), x.awaitTerminal(id).LastSeq, "one data event, then the terminal")
}

// G5: with no producer alive, a reader ends the stream after StaleAfter of silence.
func TestTerminal_ProducerLost(t *testing.T) {
	t.Parallel()
	x := newHarness(t, WithStaleAfter(15*time.Second))
	ctx := context.Background()
	const id = "orphan"
	require.NoError(t, x.store.Create(ctx, id, store.Meta{CreatedAt: t0, IdleTTL: time.Hour, Retention: time.Hour}))
	_, err := x.store.Append(ctx, id, store.AppendRequest{Kind: "data", Data: []byte("last words"), At: t0})
	require.NoError(t, err)

	sub, err := x.hub.Subscribe(ctx, id, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, sub.Close()) }()
	first, err := sub.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, "last words", string(first.Data))

	got := make(chan Event, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ev, err := sub.Next(ctx)
		assert.NoError(t, err)
		got <- ev
	}()
	advanceUntil(t, x.clock, time.Second, done)

	ev := <-got
	assert.Equal(t, uint64(2), ev.Seq)
	code, _ := terminalCode(t, ev)
	assert.Equal(t, codeProducerLost, code)
	assert.True(t, x.clock.Now().Sub(t0) > 15*time.Second, "not declared lost before StaleAfter")
	_, err = sub.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, codeProducerLost, x.awaitTerminal(id).ErrorCode)
}

// A live producer keeps its stream fresh with heartbeats, so readers never declare it lost.
func TestHeartbeat_KeepsStreamAlive(t *testing.T) {
	t.Parallel()
	x := newHarness(t, WithHeartbeatInterval(2*time.Second), WithStaleAfter(6*time.Second))
	id := x.start(blockUntilCancelled)
	x.clock.AwaitWaiters(t, supervisorWaiters)

	for i := 1; i <= 10; i++ {
		x.clock.Advance(2 * time.Second)
		want := t0.Add(time.Duration(i) * 2 * time.Second)
		testutil.Eventually(t, func() bool {
			st, err := x.hub.Status(context.Background(), id)
			require.NoError(t, err)
			return st.LastActivity.Equal(want)
		}, waitLimit)
	}
	st, err := x.hub.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, st.State)
}

func TestTerminal_ServerShutdown(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	x := newHarness(t, WithHooks(rec.hooks()))
	release := make(chan struct{})
	id := x.start(func(context.Context, Writer) error {
		<-release // ignores its context on purpose: Close must not wait forever
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := x.hub.Close(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "1 producers")
	st := x.awaitTerminal(id)
	assert.Equal(t, codeServerShutdown, st.ErrorCode)
	code, msg := terminalCode(t, x.lastEvent(id))
	assert.Equal(t, codeServerShutdown, code)
	assert.Equal(t, "server shutting down", msg)

	close(release) // the producer returns late; its terminal write loses the race (G7)
	testutil.Eventually(t, func() bool { return len(rec.endReasons()) == 1 }, waitLimit)
	assert.Equal(t, []string{codeServerShutdown}, rec.endReasons())
	assert.Equal(t, uint64(1), x.awaitTerminal(id).LastSeq, "exactly one terminal event")
}

// If another writer ended the stream, the producer stops and writes nothing more (G7).
func TestTerminal_ClosedElsewhere(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	x := newHarness(t, WithHooks(rec.hooks()))
	id := x.start(blockUntilCancelled)
	x.clock.AwaitWaiters(t, supervisorWaiters)
	lost := errorEvent(codeProducerLost, "")
	_, err := x.store.Append(context.Background(), id, store.AppendRequest{Kind: lost.kind, Data: lost.data, Force: true})
	require.NoError(t, err)

	x.clock.Advance(x.hub.cfg.heartbeat)
	testutil.Eventually(t, func() bool { return len(rec.endReasons()) == 1 }, waitLimit)
	assert.Equal(t, []string{codeProducerLost}, rec.endReasons(), "the hook reports the stored terminal (ADR 0006)")
	st := x.awaitTerminal(id)
	assert.Equal(t, codeProducerLost, st.ErrorCode)
	assert.Equal(t, uint64(1), st.LastSeq)
}

func TestHeartbeat_StoreErrors(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	var (
		touchErr, statusErr     errSwitch
		touchCalls, statusCalls atomic.Int32
	)
	fs := &faultyStore{Store: mem}
	fs.touch = func(ctx context.Context, id string, at time.Time) error {
		defer touchCalls.Add(1)
		if err := touchErr.get(); err != nil {
			return err
		}
		return mem.Touch(ctx, id, at)
	}
	fs.status = func(ctx context.Context, id string) (store.Status, error) {
		defer statusCalls.Add(1)
		if err := statusErr.get(); err != nil {
			return store.Status{}, err
		}
		return mem.Status(ctx, id)
	}
	rec := &recorder{}
	x := newHarnessOn(t, c, fs, WithHooks(rec.hooks()))
	id := x.start(blockUntilCancelled)
	c.AwaitWaiters(t, supervisorWaiters)
	beat := x.hub.cfg.heartbeat
	// beatWith advances one heartbeat and waits until the beat has made the call it fails on.
	beatWith := func(calls *atomic.Int32) {
		before := calls.Load()
		c.Advance(beat)
		testutil.Eventually(t, func() bool { return calls.Load() > before }, waitLimit)
	}

	// Transient failures are logged and retried; the producer keeps running.
	touchErr.set(errors.New("touch timeout"))
	beatWith(&touchCalls)
	touchErr.set(nil)
	statusErr.set(errors.New("status timeout"))
	beatWith(&statusCalls)
	statusErr.set(nil)
	beatWith(&statusCalls)
	st, err := mem.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, t0.Add(3*beat), st.LastActivity)
	assert.Equal(t, StateRunning, st.State)
	assert.Empty(t, rec.endReasons())

	// A vanished stream stops the producer without a terminal write.
	touchErr.set(store.ErrNotFound)
	c.Advance(beat)
	testutil.Eventually(t, func() bool { return len(rec.endReasons()) == 1 }, waitLimit)
	assert.Equal(t, []string{"closed_elsewhere"}, rec.endReasons())
}

// errSwitch is an error a test turns on and off while a store fake reads it.
type errSwitch struct {
	mu  sync.Mutex
	err error
}

func (e *errSwitch) set(err error) { e.mu.Lock(); e.err = err; e.mu.Unlock() }

func (e *errSwitch) get() error { e.mu.Lock(); defer e.mu.Unlock(); return e.err }

func TestTerminal_WriteFailureIsLoggedNotFatal(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	fs := &faultyStore{Store: mem}
	fs.append = func(ctx context.Context, id string, ev store.AppendRequest) (uint64, error) {
		if store.IsTerminal(ev.Kind) {
			return 0, errors.New("store down")
		}
		return mem.Append(ctx, id, ev)
	}
	rec := &recorder{}
	x := newHarnessOn(t, c, fs, WithHooks(rec.hooks()))
	id := x.start(func(_ context.Context, w Writer) error { return w.Write([]byte("x")) })
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		testutil.Eventually(t, func() bool { return len(rec.endReasons()) == 1 }, waitLimit)
	}()
	advanceUntil(t, x.clock, 100*time.Millisecond, ended) // drives the retry backoff (ADR 0005)
	assert.Equal(t, []string{"done"}, rec.endReasons(), "the outcome it tried to store")
	assert.LessOrEqual(t, x.clock.Now().Sub(t0), x.hub.cfg.staleAfter/2+time.Second, "retries are bounded by StaleAfter/2")
	st, err := mem.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, st.State, "a terminal write failing for StaleAfter/2 is given up and logged")
}

func TestErrorEvent_Payload(t *testing.T) {
	t.Parallel()
	for _, code := range []string{codeCancelled, codeProducerFailed, codeProducerPanic, codeTimeout, codeProducerLost, codeServerShutdown} {
		term := errorEvent(code, "")
		got, msg := terminalCode(t, Event{Kind: term.kind, Data: term.data})
		assert.Equal(t, code, got)
		assert.Equal(t, defaultMessage(code), msg)
		assert.Equal(t, code, term.reason)
	}
	got, msg := terminalCode(t, Event{Kind: "error", Data: errorEvent(codeProducerFailed, "custom \"quoted\"").data})
	assert.Equal(t, codeProducerFailed, got)
	assert.Equal(t, `custom "quoted"`, msg)
}
