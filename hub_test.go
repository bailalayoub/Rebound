package rebound

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

func TestNew_Defaults(t *testing.T) {
	t.Parallel()
	h, err := New(memory.New())
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, h.cfg.maxDuration)
	assert.Equal(t, 2*time.Second, h.cfg.heartbeat)
	assert.Equal(t, 15*time.Second, h.cfg.staleAfter)
	assert.Equal(t, 10*time.Minute, h.cfg.retention)
	assert.Equal(t, 10*time.Minute+15*time.Second+time.Minute, h.cfg.idleTTL)
	assert.Equal(t, 10_000, h.cfg.maxEvents)
	assert.Equal(t, 64<<10, h.cfg.maxEventBytes)
	assert.Equal(t, 5*time.Second, h.readBlock)
	assert.False(t, h.cfg.exposeErrors)
}

func TestNew_ExplicitOptions(t *testing.T) {
	t.Parallel()
	h, err := New(memory.New(),
		WithMaxDuration(time.Minute), WithHeartbeatInterval(time.Second), WithStaleAfter(6*time.Second),
		WithRetention(time.Hour), WithIdleTTL(2*time.Hour), WithMaxEvents(7), WithExposeProducerErrors(true),
		WithIDGenerator(func() string { return "fixed" }), WithHooks(Hooks{}), WithLogger(quietLogger()),
		WithClock(store.RealClock{}))
	require.NoError(t, err)
	assert.Equal(t, time.Minute, h.cfg.maxDuration)
	assert.Equal(t, 2*time.Hour, h.cfg.idleTTL)
	assert.Equal(t, 2*time.Second, h.readBlock, "StaleAfter/3 below the 5 s cap")
	assert.Equal(t, 7, h.cfg.maxEvents)
	assert.True(t, h.cfg.exposeErrors)
	id, err := h.cfg.idGen()
	require.NoError(t, err)
	assert.Equal(t, "fixed", id)
}

func TestNew_RejectsInvalidOptions(t *testing.T) {
	t.Parallel()
	_, err := New(nil)
	require.Error(t, err)

	cases := map[string]Option{
		"nil logger":         WithLogger(nil),
		"nil clock":          WithClock(nil),
		"nil id generator":   WithIDGenerator(nil),
		"zero max duration":  WithMaxDuration(0),
		"negative heartbeat": WithHeartbeatInterval(-time.Second),
		"zero stale after":   WithStaleAfter(0),
		"zero retention":     WithRetention(0),
		"zero idle ttl":      WithIdleTTL(0),
		"zero max events":    WithMaxEvents(0),
		"stale below heartbeat": func(c *config) error {
			c.staleAfter, c.heartbeat = time.Second, 2*time.Second
			return nil
		},
	}
	for name, opt := range cases {
		_, err := New(memory.New(), opt)
		require.Error(t, err, name)
		assert.True(t, strings.HasPrefix(err.Error(), "rebound: "), "%s: %v", name, err)
	}
}

// G3: the producer runs to completion with nobody listening; a late reader replays everything.
func TestStart_ProducerRunsWithoutSubscriber(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	x := newHarness(t, WithHooks(rec.hooks()))
	id := x.start(func(_ context.Context, w Writer) error {
		require.NoError(t, w.Write([]byte("Bon")))
		require.NoError(t, w.WriteEvent("meta", []byte(`{"instance":"app-1"}`)))
		require.NoError(t, w.WriteJSON(map[string]string{"text": "jour"}))
		return nil
	}, WithMeta(map[string]string{"user_id": "u1"}))

	st := x.awaitTerminal(id)
	assert.Equal(t, StateDone, st.State)
	assert.Equal(t, uint64(4), st.LastSeq)
	assert.Equal(t, map[string]string{"user_id": "u1"}, st.Meta)
	assert.Equal(t, t0, st.CreatedAt)

	evs := x.collect(id, 0)
	require.Len(t, evs, 4)
	assert.Equal(t, Event{Seq: 1, Kind: "data", Data: []byte("Bon")}, evs[0])
	assert.Equal(t, Event{Seq: 2, Kind: "meta", Data: []byte(`{"instance":"app-1"}`)}, evs[1])
	assert.Equal(t, Event{Seq: 3, Kind: "data", Data: []byte(`{"text":"jour"}`)}, evs[2])
	assert.Equal(t, Event{Seq: 4, Kind: "done", Data: []byte("{}")}, evs[3])

	testutil.Eventually(t, func() bool { return len(rec.endReasons()) == 1 }, waitLimit)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, []string{id}, rec.starts)
	assert.Equal(t, []string{"done"}, rec.ends)
	assert.Equal(t, []string{"data", "meta", "data"}, rec.events)
	assert.Equal(t, []uint64{0}, rec.subs)
	assert.Equal(t, 1, rec.unsubs)
}

// The request that called Start may end at once: the producer must neither see it nor stop.
func TestStart_CallerContextCancellationDoesNotStopProducer(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	ctxErrAfterRelease := make(chan error, 1)

	st, err := x.hub.Start(callerCtx, func(ctx context.Context, w Writer) error {
		close(started)
		<-release
		ctxErrAfterRelease <- ctx.Err()
		return w.Write([]byte("still here"))
	})
	require.NoError(t, err)
	<-started
	cancelCaller()
	close(release)

	assert.NoError(t, <-ctxErrAfterRelease, "producer context cancelled with the caller's")
	assert.Equal(t, StateDone, x.awaitTerminal(st.ID).State)
	evs := x.collect(st.ID, 0)
	require.Len(t, evs, 2)
	assert.Equal(t, "still here", string(evs[0].Data))
}

func TestStart_Errors(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ctx := context.Background()

	_, err := x.hub.Start(ctx, nil)
	require.Error(t, err)
	_, err = x.hub.Start(ctx, blockUntilCancelled, WithStreamMaxDuration(0))
	require.Error(t, err)
	_, err = x.hub.Start(ctx, blockUntilCancelled, WithStreamID("has space"))
	require.Error(t, err)
	_, err = x.hub.Start(ctx, blockUntilCancelled, WithStreamID(strings.Repeat("a", 129)))
	require.Error(t, err)

	id := x.start(blockUntilCancelled, WithStreamID("chat-1.a_b~C"))
	assert.Equal(t, "chat-1.a_b~C", id)
	_, err = x.hub.Start(ctx, blockUntilCancelled, WithStreamID(id))
	require.ErrorIs(t, err, ErrExists)
}

func TestStart_GeneratedIDs(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	id := x.start(blockUntilCancelled)
	assert.Len(t, id, 22)
	require.NoError(t, validateStreamID(id))
	assert.NotEqual(t, id, x.start(blockUntilCancelled))

	calls := 0
	dup := newHarness(t, WithIDGenerator(func() string { calls++; return "same" }))
	dup.start(blockUntilCancelled)
	_, err := dup.hub.Start(context.Background(), blockUntilCancelled)
	require.ErrorIs(t, err, ErrExists)
	assert.Equal(t, 1+maxIDAttempts, calls, "a collision is retried %d times", maxIDAttempts)

	bad := newHarness(t, WithIDGenerator(func() string { return "{bad}" }))
	_, err = bad.hub.Start(context.Background(), blockUntilCancelled)
	require.Error(t, err)

	failing := newHarness(t)
	failing.hub.cfg.idGen = func() (string, error) { return "", errors.New("entropy exhausted") }
	_, err = failing.hub.Start(context.Background(), blockUntilCancelled)
	require.ErrorContains(t, err, "entropy exhausted")
}

func TestStart_StoreCreateFailure(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	boom := errors.New("store down")
	fs := &faultyStore{Store: memory.New(memory.WithClock(c)), create: func(context.Context, string, store.Meta) error { return boom }}
	x := newHarnessOn(t, c, fs)
	_, err := x.hub.Start(context.Background(), blockUntilCancelled)
	require.ErrorIs(t, err, boom)
}

func TestWriter_Validation(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	results := make(chan []error, 1)
	id := x.start(func(_ context.Context, w Writer) error {
		results <- []error{
			w.WriteEvent("done", nil),
			w.WriteEvent("Bad", nil),
			w.Write(make([]byte, 64<<10+1)),
			w.WriteJSON(make(chan int)),
			w.Write(make([]byte, 64<<10)), // exactly the limit is fine
		}
		if w.StreamID() == "" {
			return errors.New("empty stream id")
		}
		return nil
	})
	errs := <-results
	require.ErrorIs(t, errs[0], ErrInvalidKind)
	require.ErrorIs(t, errs[1], ErrInvalidKind)
	require.ErrorIs(t, errs[2], ErrTooLarge)
	require.ErrorContains(t, errs[3], "encode json")
	require.NoError(t, errs[4])
	st := x.awaitTerminal(id)
	assert.Equal(t, StateDone, st.State)
	assert.Equal(t, uint64(2), st.LastSeq, "only the valid event and the terminal are stored")
}

func TestWriter_StoreFailureIsReturned(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	boom := errors.New("store down")
	mem := memory.New(memory.WithClock(c))
	fs := &faultyStore{Store: mem}
	fs.append = func(ctx context.Context, id string, ev store.AppendRequest) (uint64, error) {
		if ev.Kind == "data" {
			return 0, boom
		}
		return mem.Append(ctx, id, ev)
	}
	x := newHarnessOn(t, c, fs)
	got := make(chan error, 1)
	returned := make(chan struct{})
	id := x.start(func(_ context.Context, w Writer) error {
		defer close(returned)
		err := w.Write([]byte("x"))
		got <- err
		return err
	})
	advanceUntil(t, x.clock, 100*time.Millisecond, returned) // drives the retry backoff (ADR 0005)
	err := <-got
	require.ErrorIs(t, err, boom, "a store failure that outlasts the retries is returned")
	require.NotErrorIs(t, err, ErrClosed)
	assert.GreaterOrEqual(t, x.clock.Now().Sub(t0), x.hub.cfg.staleAfter/2-time.Second, "retried for about StaleAfter/2")
	assert.Equal(t, codeProducerFailed, x.awaitTerminal(id).ErrorCode)
}

func TestHooks_PanicIsRecovered(t *testing.T) {
	t.Parallel()
	x := newHarness(t, WithHooks(Hooks{
		OnStreamStart: func(string) { panic("hook bug") },
		OnEvent:       func(string, uint64, string) { panic("hook bug") },
	}))
	id := x.start(func(_ context.Context, w Writer) error { return w.Write([]byte("x")) })
	assert.Equal(t, StateDone, x.awaitTerminal(id).State)
}

func TestStatusAndCancel_UnknownStream(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	_, err := x.hub.Status(context.Background(), "nope")
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, x.hub.Cancel(context.Background(), "nope"), ErrNotFound)
}

func TestCancel_FinishedStreamIsNoop(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	id := x.start(func(context.Context, Writer) error { return nil })
	x.awaitTerminal(id)
	require.NoError(t, x.hub.Cancel(context.Background(), id))
	assert.Equal(t, StateDone, x.awaitTerminal(id).State)
}

func TestClose_WaitsForProducersToFinish(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	release := make(chan struct{})
	id := x.start(func(_ context.Context, w Writer) error {
		<-release
		return w.Write([]byte("finished during shutdown"))
	})

	closed := make(chan error, 1)
	go func() { closed <- x.hub.Close(context.Background()) }()
	testutil.Eventually(t, x.hub.isClosed, waitLimit)
	_, err := x.hub.Start(context.Background(), blockUntilCancelled)
	require.ErrorIs(t, err, ErrHubClosed)

	close(release)
	require.NoError(t, <-closed)
	st := x.awaitTerminal(id)
	assert.Equal(t, StateDone, st.State, "a producer finishing before the deadline ends normally")
	require.NoError(t, x.hub.Close(context.Background()), "Close is idempotent")
}

func TestStart_LosesRaceWithClose(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	var x *harness
	fs := &faultyStore{Store: mem}
	fs.create = func(ctx context.Context, id string, m store.Meta) error {
		require.NoError(t, x.hub.Close(ctx)) // no producer registered yet: returns at once
		return mem.Create(ctx, id, m)
	}
	x = newHarnessOn(t, c, fs)
	_, err := x.hub.Start(context.Background(), blockUntilCancelled, WithStreamID("raced"))
	require.ErrorIs(t, err, ErrHubClosed)
	st := x.awaitTerminal("raced")
	assert.Equal(t, codeServerShutdown, st.ErrorCode)
}

func TestValidateStreamID(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"a", "A9", "x-y_z.w~v", strings.Repeat("z", 128)} {
		require.NoError(t, validateStreamID(ok), ok)
	}
	for _, bad := range []string{"", strings.Repeat("z", 129), "a/b", "a b", "{a}", "a:b", "é", "a\n"} {
		require.Error(t, validateStreamID(bad), "%q", bad)
	}
}
