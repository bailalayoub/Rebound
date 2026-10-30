// Tests of the facade: every exported function forwards to internal/core, where the behaviour
// itself is tested.
package rebound_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

func newHub(t *testing.T, opts ...rebound.Option) *rebound.Hub {
	t.Helper()
	hub, err := rebound.New(memory.New(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hub.Close(context.Background()) }) // already closed in some tests
	return hub
}

// readAll follows a stream from after until its terminal event.
func readAll(t *testing.T, hub *rebound.Hub, id string, after uint64) []rebound.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sub, err := hub.Subscribe(ctx, id, after)
	require.NoError(t, err)
	defer func() { require.NoError(t, sub.Close()) }()
	var events []rebound.Event
	for {
		ev, err := sub.Next(ctx)
		if errors.Is(err, io.EOF) {
			return events
		}
		require.NoError(t, err)
		events = append(events, ev)
	}
}

func TestFacade_StartAndResume(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var started, ended []string
	hub := newHub(t,
		rebound.WithLogger(slog.New(slog.DiscardHandler)),
		rebound.WithClock(store.RealClock{}),
		rebound.WithIDGenerator(func() string { return "unused" }),
		rebound.WithMaxDuration(time.Minute),
		rebound.WithHeartbeatInterval(time.Second),
		rebound.WithStaleAfter(5*time.Second),
		rebound.WithRetention(time.Minute),
		rebound.WithIdleTTL(2*time.Minute),
		rebound.WithMaxEvents(100),
		rebound.WithExposeProducerErrors(true),
		rebound.WithHooks(rebound.Hooks{
			OnStreamStart: func(id string) { mu.Lock(); started = append(started, id); mu.Unlock() },
			OnStreamEnd:   func(id, _ string, _ time.Duration) { mu.Lock(); ended = append(ended, id); mu.Unlock() },
		}),
	)

	st, err := hub.Start(context.Background(), func(_ context.Context, w rebound.Writer) error {
		if err := w.Write([]byte("a")); err != nil {
			return err
		}
		if err := w.WriteEvent("tool", []byte("b")); err != nil {
			return err
		}
		return w.WriteJSON(map[string]string{"id": w.StreamID()})
	}, rebound.WithStreamID("s1"), rebound.WithMeta(map[string]string{"user": "u"}),
		rebound.WithStreamMaxDuration(30*time.Second))
	require.NoError(t, err)
	assert.Equal(t, "s1", st.ID)

	all := readAll(t, hub, st.ID, 0)
	require.Len(t, all, 4)
	assert.Equal(t, []string{"data", "tool", "data", "done"},
		[]string{all[0].Kind, all[1].Kind, all[2].Kind, all[3].Kind})
	assert.JSONEq(t, `{"id":"s1"}`, string(all[2].Data))
	assert.True(t, rebound.IsTerminal(all[3].Kind))

	// Resuming after event 2 replays exactly 3 and 4.
	resumed := readAll(t, hub, st.ID, 2)
	assert.Equal(t, all[2:], resumed)

	status, err := hub.Status(context.Background(), st.ID)
	require.NoError(t, err)
	assert.Equal(t, rebound.StateDone, status.State)
	assert.Equal(t, "u", status.Meta["user"])
	require.NoError(t, hub.Cancel(context.Background(), st.ID), "cancel is idempotent on finished streams")

	require.NoError(t, hub.Close(context.Background()))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"s1"}, started)
	assert.Equal(t, []string{"s1"}, ended)
}

func TestFacade_Errors(t *testing.T) {
	t.Parallel()
	_, err := rebound.New(nil)
	require.Error(t, err)
	_, err = rebound.New(memory.New(), rebound.WithMaxEvents(0))
	require.Error(t, err)

	hub := newHub(t)
	ctx := context.Background()
	_, err = hub.Start(ctx, nil)
	require.Error(t, err, "nil producer")
	noop := func(context.Context, rebound.Writer) error { return nil }
	_, err = hub.Start(ctx, noop, rebound.WithStreamID(""))
	require.Error(t, err, "invalid stream id")
	_, err = hub.Subscribe(ctx, "missing", 0)
	require.ErrorIs(t, err, rebound.ErrNotFound)

	require.NoError(t, hub.Close(ctx))
	_, err = hub.Start(ctx, noop)
	require.ErrorIs(t, err, rebound.ErrHubClosed)
}
