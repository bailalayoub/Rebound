package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

// writeN is a producer that writes "0".."n-1" then returns.
func writeN(n int) ProducerFunc {
	return func(_ context.Context, w Writer) error {
		for i := range n {
			if err := w.Write([]byte(fmt.Sprint(i))); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestSubscribe_CursorErrors(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	x := newHarnessOn(t, c, &trimmingStore{Store: memory.New(memory.WithClock(c)), first: 17})
	ctx := context.Background()

	_, err := x.hub.Subscribe(ctx, "missing", 0)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = x.hub.Subscribe(ctx, "not/an id", 0)
	require.ErrorIs(t, err, ErrNotFound, "malformed ids are unknown ids")

	id := x.start(writeN(20))
	st := x.awaitTerminal(id) // 20 data + done = 21 events; the store trimmed 1..16 after the end
	require.Equal(t, uint64(21), st.LastSeq)
	require.Equal(t, uint64(17), st.FirstSeq)

	_, err = x.hub.Subscribe(ctx, id, 22)
	require.ErrorIs(t, err, ErrInvalidCursor)
	_, err = x.hub.Subscribe(ctx, id, 0)
	require.ErrorIs(t, err, ErrGap)
	_, err = x.hub.Subscribe(ctx, id, 15)
	require.ErrorIs(t, err, ErrGap)

	evs := x.collect(id, 16)
	require.Len(t, evs, 5)
	assert.Equal(t, uint64(17), evs[0].Seq)
	assert.Equal(t, "done", evs[4].Kind)
}

func TestSubscribe_AtTerminalCursorIsImmediateEOF(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	id := x.start(writeN(2))
	st := x.awaitTerminal(id)
	sub, err := x.hub.Subscribe(context.Background(), id, st.LastSeq)
	require.NoError(t, err)
	_, err = sub.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	_, err = sub.Next(context.Background())
	require.ErrorIs(t, err, io.EOF, "EOF is sticky")
	require.NoError(t, sub.Close())
}

// A reader joining mid-stream gets the past events, then the live ones, with no seam (G1, G2).
func TestSubscribe_ReplayThenLive(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	step := make(chan struct{})
	id := x.start(func(_ context.Context, w Writer) error {
		for i := range 6 {
			if i == 3 {
				<-step // pause after three events, until the reader has caught up
			}
			if err := w.Write([]byte(fmt.Sprint(i))); err != nil {
				return err
			}
		}
		return nil
	})
	testutil.Eventually(t, func() bool {
		st, err := x.hub.Status(context.Background(), id)
		require.NoError(t, err)
		return st.LastSeq == 3
	}, waitLimit)

	sub, err := x.hub.Subscribe(context.Background(), id, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, sub.Close()) }()
	for want := uint64(2); want <= 3; want++ {
		ev, err := sub.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, want, ev.Seq, "replayed event")
	}
	close(step)
	evs := drain(t, sub)
	require.Len(t, evs, 4)
	for i, ev := range evs[:3] {
		assert.Equal(t, uint64(4+i), ev.Seq)
		assert.Equal(t, fmt.Sprint(3+i), string(ev.Data))
	}
	assert.Equal(t, Event{Seq: 7, Kind: "done", Data: []byte("{}")}, evs[3])
}

func TestSubscription_CloseInterruptsNext(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	x := newHarness(t, WithHooks(rec.hooks()))
	id := x.start(blockUntilCancelled)
	sub, err := x.hub.Subscribe(context.Background(), id, 0)
	require.NoError(t, err)

	nextErr := make(chan error, 1)
	go func() {
		_, err := sub.Next(context.Background())
		nextErr <- err
	}()
	x.clock.AwaitWaiters(t, supervisorWaiters+1) // the read's block timer is armed
	require.NoError(t, sub.Close())
	require.NoError(t, sub.Close(), "Close is idempotent")
	require.ErrorIs(t, <-nextErr, errSubscriptionClosed)
	_, err = sub.Next(context.Background())
	require.ErrorIs(t, err, errSubscriptionClosed)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Equal(t, 1, rec.unsubs)
	st, err := x.hub.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, st.State, "closing a subscription never affects the stream")
}

func TestSubscription_NextHonoursContext(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	id := x.start(blockUntilCancelled)
	sub, err := x.hub.Subscribe(context.Background(), id, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, sub.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	nextErr := make(chan error, 1)
	go func() {
		_, err := sub.Next(ctx)
		nextErr <- err
	}()
	x.clock.AwaitWaiters(t, supervisorWaiters+1)
	cancel()
	require.ErrorIs(t, <-nextErr, context.Canceled)
}

func TestSubscription_StoreErrors(t *testing.T) {
	t.Parallel()
	c := testutil.NewClock(t0)
	mem := memory.New(memory.WithClock(c))
	readErr := errors.New("connection reset")
	var failRead, failStatus errSwitch
	fs := &faultyStore{Store: mem}
	fs.status = func(ctx context.Context, id string) (store.Status, error) {
		if err := failStatus.get(); err != nil {
			return store.Status{}, err
		}
		return mem.Status(ctx, id)
	}
	x := newHarnessOn(t, c, &readFailingStore{faultyStore: fs, err: &failRead})
	id := x.start(blockUntilCancelled)

	failRead.set(readErr)
	sub, err := x.hub.Subscribe(context.Background(), id, 0)
	require.NoError(t, err)
	_, err = sub.Next(context.Background())
	require.ErrorIs(t, err, readErr)

	// A read that times out empty, then a failing liveness check.
	failRead.set(nil)
	failStatus.set(errors.New("status down"))
	nextErr := make(chan error, 1)
	go func() {
		_, err := sub.Next(context.Background())
		nextErr <- err
	}()
	x.clock.AwaitWaiters(t, supervisorWaiters+1)
	x.clock.Advance(x.hub.readBlock)
	require.ErrorContains(t, <-nextErr, "status down")
	require.NoError(t, sub.Close())
}

// readFailingStore makes Read fail while err is set.
type readFailingStore struct {
	*faultyStore
	err *errSwitch
}

func (r *readFailingStore) Read(ctx context.Context, id string, after uint64, limit int, block time.Duration) ([]store.Event, error) {
	if err := r.err.get(); err != nil {
		return nil, err
	}
	return r.faultyStore.Read(ctx, id, after, limit, block)
}
