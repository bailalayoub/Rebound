package httpx

// Regression tests for defects found in the pre-release review of v0.1.0. Each one failed before
// its fix; the comments point to the ADR that records the decision when there is one.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/store"
)

// ADR 0004: a stream that hits MaxEvents ends with max_events_exceeded and is never trimmed,
// so the request without a cursor (a late joiner, a reload) and any old cursor are served; before,
// both got 410 and the client gave up.
func TestRegression_StreamBeyondMaxEventsIsServedFromZero(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{hubOpts: []rebound.Option{rebound.WithMaxEvents(5)}})
	id := e.start(writeWords("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
	st := e.awaitTerminal(id)
	require.Equal(t, "max_events_exceeded", st.ErrorCode)
	require.Equal(t, uint64(1), st.FirstSeq)

	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusOK, resp.status, "restart from 0 is served")
	evs := events(frames(t, resp))
	require.Len(t, evs, 6)
	assert.Equal(t, "0", evs[0].Data)
	assert.Equal(t, "error", evs[5].Event)
	assert.JSONEq(t, `{"code":"max_events_exceeded","message":"too many events"}`, evs[5].Data)

	resp = e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": "2"})
	require.Equal(t, http.StatusOK, resp.status, "an old cursor is served")
	assert.Equal(t, "3", events(frames(t, resp))[0].ID)
}

// ADR 0007: what an SSE client receives is exactly what is stored, which is the producer's
// payload normalized to UTF-8 with LF line endings.
func TestRegression_PayloadsArriveExactlyAsStored(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	sent := []string{"a\rb", "c\r\nd", "e\n", "\xff ok", "déjà vu"}
	id := e.start(writeWords(sent...))
	e.awaitTerminal(id)
	sub, err := e.hub.Subscribe(context.Background(), id, 0)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	got := events(frames(t, e.get(context.Background(), "/streams/"+id, nil)))
	require.Len(t, got, len(sent)+1)
	want := []string{"a\nb", "c\nd", "e\n", "� ok", "déjà vu"}
	for i := range sent {
		ev, err := sub.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, want[i], string(ev.Data), "stored event %d", i+1)
		assert.Equal(t, string(ev.Data), got[i].Data, "SSE event %d differs from the stored one", i+1)
	}
}

// ADR 0008: store failures answered 503 and mid-stream read failures are logged with the
// stream id.
func TestRegression_StoreFailuresAreLogged(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	var failStatus, failRead failSwitch
	e := newEnv(t, envConfig{
		handlerOpts: []HandlerOption{WithLogger(logger)},
		wrap: func(s store.Store) store.Store {
			return &readFaults{Store: &statusFaults{Store: s, failStatus: &failStatus, failCancel: &failSwitch{}}, fail: &failRead}
		},
	})
	id := e.start(blockUntilCancelled)

	failStatus.set(true)
	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.status)
	assert.Contains(t, logs.String(), fmt.Sprintf("msg=\"httpx: store unavailable\" stream_id=%s op=status error=\"rebound: status %s: store down\"", id, id))

	failStatus.set(false)
	failRead.set(true)
	resp = e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusOK, resp.status)
	frames(t, resp) // returns when the handler closes the response
	assert.Contains(t, logs.String(), "msg=\"httpx: stream read failed; closing the response\" stream_id="+id)
	assert.Same(t, slog.Default(), NewHandler(nil, WithLogger(nil)).logger, "nil keeps the default")
}

// syncBuffer is a bytes.Buffer safe for the handler's goroutines and the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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
