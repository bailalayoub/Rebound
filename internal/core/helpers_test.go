package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

// waitLimit bounds real-time waits so a broken hand-off fails instead of hanging.
const waitLimit = 5 * time.Second

type harness struct {
	t     *testing.T
	clock *testutil.Clock
	store store.Store
	hub   *Hub
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newHarness builds a Hub over a fresh memory store, both on one manual clock. At cleanup the
// Hub is closed with an expired context, so producers still blocked are ended.
func newHarness(t *testing.T, opts ...Option) *harness {
	t.Helper()
	c := testutil.NewClock(t0)
	s := memory.New(memory.WithClock(c))
	return newHarnessOn(t, c, s, opts...)
}

func newHarnessOn(t *testing.T, c *testutil.Clock, s store.Store, opts ...Option) *harness {
	t.Helper()
	all := append([]Option{WithClock(c), WithLogger(quietLogger())}, opts...)
	h, err := New(s, all...)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = h.Close(ctx) // an error only reports producers ended at the deadline, expected here
	})
	return &harness{t: t, clock: c, store: s, hub: h}
}

func (x *harness) start(produce ProducerFunc, opts ...StartOption) string {
	x.t.Helper()
	st, err := x.hub.Start(context.Background(), produce, opts...)
	require.NoError(x.t, err)
	return st.ID
}

// awaitTerminal waits until the stream has its terminal event and returns its status.
func (x *harness) awaitTerminal(id string) Status {
	x.t.Helper()
	var st Status
	testutil.Eventually(x.t, func() bool {
		var err error
		st, err = x.hub.Status(context.Background(), id)
		require.NoError(x.t, err)
		return st.State.IsTerminal()
	}, waitLimit)
	return st
}

// collect subscribes after afterSeq and returns every event up to io.EOF.
func (x *harness) collect(id string, afterSeq uint64) []Event {
	x.t.Helper()
	sub, err := x.hub.Subscribe(context.Background(), id, afterSeq)
	require.NoError(x.t, err)
	defer func() { require.NoError(x.t, sub.Close()) }()
	return drain(x.t, sub)
}

func drain(t *testing.T, sub *Subscription) []Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	var evs []Event
	for {
		ev, err := sub.Next(ctx)
		if errors.Is(err, io.EOF) {
			return evs
		}
		require.NoError(t, err)
		evs = append(evs, ev)
	}
}

// terminalCode decodes the code of a terminal error event.
func terminalCode(t *testing.T, ev Event) (code, message string) {
	t.Helper()
	require.Equal(t, "error", ev.Kind, "terminal event %d is %q", ev.Seq, ev.Kind)
	var p struct{ Code, Message string }
	require.NoError(t, json.Unmarshal(ev.Data, &p))
	return p.Code, p.Message
}

// blockUntilCancelled is a producer that waits for its context.
func blockUntilCancelled(ctx context.Context, _ Writer) error {
	<-ctx.Done()
	return ctx.Err()
}

// recorder collects hook calls.
type recorder struct {
	mu     sync.Mutex
	starts []string
	ends   []string
	subs   []uint64
	unsubs int
	events []string
}

func (r *recorder) hooks() Hooks {
	return Hooks{
		OnStreamStart: func(id string) { r.mu.Lock(); r.starts = append(r.starts, id); r.mu.Unlock() },
		OnStreamEnd: func(_, reason string, _ time.Duration) {
			r.mu.Lock()
			r.ends = append(r.ends, reason)
			r.mu.Unlock()
		},
		OnSubscribe:   func(_ string, after uint64) { r.mu.Lock(); r.subs = append(r.subs, after); r.mu.Unlock() },
		OnUnsubscribe: func(string) { r.mu.Lock(); r.unsubs++; r.mu.Unlock() },
		OnEvent: func(_ string, _ uint64, kind string) {
			r.mu.Lock()
			r.events = append(r.events, kind)
			r.mu.Unlock()
		},
	}
}

func (r *recorder) endReasons() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ends...)
}

// faultyStore wraps a Store and lets a test replace individual methods.
type faultyStore struct {
	store.Store
	create func(ctx context.Context, id string, m store.Meta) error
	append func(ctx context.Context, id string, ev store.AppendRequest) (uint64, error)
	touch  func(ctx context.Context, id string, at time.Time) error
	status func(ctx context.Context, id string) (store.Status, error)
}

func (f *faultyStore) Create(ctx context.Context, id string, m store.Meta) error {
	if f.create != nil {
		return f.create(ctx, id, m)
	}
	return f.Store.Create(ctx, id, m)
}

func (f *faultyStore) Append(ctx context.Context, id string, ev store.AppendRequest) (uint64, error) {
	if f.append != nil {
		return f.append(ctx, id, ev)
	}
	return f.Store.Append(ctx, id, ev)
}

func (f *faultyStore) Touch(ctx context.Context, id string, at time.Time) error {
	if f.touch != nil {
		return f.touch(ctx, id, at)
	}
	return f.Store.Touch(ctx, id, at)
}

func (f *faultyStore) Status(ctx context.Context, id string) (store.Status, error) {
	if f.status != nil {
		return f.status(ctx, id)
	}
	return f.Store.Status(ctx, id)
}

// trimmingStore simulates a store that trimmed a terminal stream during retention, which ADR 0004
// still allows (the built-in stores never trim): events before first are gone.
type trimmingStore struct {
	store.Store
	first uint64
}

func (s *trimmingStore) Status(ctx context.Context, id string) (store.Status, error) {
	st, err := s.Store.Status(ctx, id)
	if err == nil && st.State.IsTerminal() {
		st.FirstSeq = max(st.FirstSeq, s.first)
	}
	return st, err
}

func (s *trimmingStore) Read(ctx context.Context, id string, after uint64, limit int, block time.Duration) ([]store.Event, error) {
	st, err := s.Status(ctx, id)
	if err == nil && st.FirstSeq > 0 && after+1 < st.FirstSeq {
		return nil, fmt.Errorf("trimming store: read %s: %w", id, store.ErrGap)
	}
	return s.Store.Read(ctx, id, after, limit, block)
}
