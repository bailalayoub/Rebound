package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/internal/sseclient"
	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

const waitLimit = 5 * time.Second

// env is a Hub, a Handler and an HTTP server sharing one manual clock.
type env struct {
	t       *testing.T
	clock   *testutil.Clock
	store   store.Store
	hub     *rebound.Hub
	handler *Handler
	srv     *httptest.Server
	unsubs  *counter
}

type counter struct {
	mu sync.Mutex
	n  int
}

func (c *counter) inc() { c.mu.Lock(); c.n++; c.mu.Unlock() }

func (c *counter) get() int { c.mu.Lock(); defer c.mu.Unlock(); return c.n }

type envConfig struct {
	hubOpts     []rebound.Option
	handlerOpts []HandlerOption
	wrap        func(store.Store) store.Store
}

func newEnv(t *testing.T, cfg envConfig) *env {
	t.Helper()
	c := testutil.NewClock(t0)
	var s store.Store = memory.New(memory.WithClock(c))
	if cfg.wrap != nil {
		s = cfg.wrap(s)
	}
	unsubs := &counter{}
	opts := append([]rebound.Option{
		rebound.WithClock(c),
		rebound.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		rebound.WithHooks(rebound.Hooks{OnUnsubscribe: func(string) { unsubs.inc() }}),
	}, cfg.hubOpts...)
	hub, err := rebound.New(s, opts...)
	require.NoError(t, err)
	x := NewHandler(hub, cfg.handlerOpts...)
	x.clock = c

	mux := http.NewServeMux()
	mux.Handle("GET /streams/{id}", x.Stream(PathValue("id")))
	mux.Handle("POST /streams/{id}/cancel", x.Cancel(PathValue("id")))
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = hub.Close(ctx) // ends producers still blocked; the error only reports that
	})
	return &env{t: t, clock: c, store: s, hub: hub, handler: x, srv: srv, unsubs: unsubs}
}

func (e *env) start(produce rebound.ProducerFunc, opts ...rebound.StartOption) string {
	e.t.Helper()
	st, err := e.hub.Start(context.Background(), produce, opts...)
	require.NoError(e.t, err)
	return st.ID
}

func (e *env) awaitTerminal(id string) rebound.Status {
	e.t.Helper()
	var st rebound.Status
	testutil.Eventually(e.t, func() bool {
		var err error
		st, err = e.hub.Status(context.Background(), id)
		require.NoError(e.t, err)
		return st.State.IsTerminal()
	}, waitLimit)
	return st
}

// get performs a GET on path with optional headers; the response body is closed at cleanup.
func (e *env) get(ctx context.Context, path string, headers map[string]string) *response {
	e.t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.srv.URL+path, http.NoBody)
	require.NoError(e.t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return e.do(req)
}

func (e *env) post(path string) *response {
	e.t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, e.srv.URL+path, http.NoBody)
	require.NoError(e.t, err)
	return e.do(req)
}

// response is the part of an *http.Response the tests use; its body is closed at cleanup.
type response struct {
	status int
	header http.Header
	body   io.ReadCloser
}

func (e *env) do(req *http.Request) *response {
	e.t.Helper()
	resp, err := e.srv.Client().Do(req)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = resp.Body.Close() })
	return &response{status: resp.StatusCode, header: resp.Header, body: resp.Body}
}

// frames reads every frame of resp until the server closes it.
func frames(t *testing.T, resp *response) []sseclient.Frame {
	t.Helper()
	rd := sseclient.NewReader(resp.body)
	var out []sseclient.Frame
	for {
		f, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, f)
	}
}

func events(fs []sseclient.Frame) []sseclient.Frame {
	var out []sseclient.Frame
	for _, f := range fs {
		if f.IsEvent() {
			out = append(out, f)
		}
	}
	return out
}

func jsonBody(t *testing.T, resp *response) map[string]any {
	t.Helper()
	assert.Equal(t, "application/json", resp.header.Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.body).Decode(&body))
	return body
}

func writeWords(words ...string) rebound.ProducerFunc {
	return func(_ context.Context, w rebound.Writer) error {
		for _, word := range words {
			if err := w.Write([]byte(word)); err != nil {
				return err
			}
		}
		return nil
	}
}

func blockUntilCancelled(ctx context.Context, _ rebound.Writer) error {
	<-ctx.Done()
	return nil
}

// --- 200 ---

func TestServeStream_HappyPath(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(writeWords("Bon", "jour", "multi\nline"))
	resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Accept": "text/event-stream"})

	require.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "text/event-stream; charset=utf-8", resp.header.Get("Content-Type"))
	assert.Equal(t, "no-cache, no-transform", resp.header.Get("Cache-Control"))
	assert.Equal(t, "keep-alive", resp.header.Get("Connection"))
	assert.Equal(t, "no", resp.header.Get("X-Accel-Buffering"))
	assert.Equal(t, id, resp.header.Get("X-Stream-ID"))

	fs := frames(t, resp) // returns only because the server closed the response after the terminal
	require.NotEmpty(t, fs)
	assert.True(t, fs[0].HasRetry, "retry: is sent first")
	assert.Equal(t, int64(1000), fs[0].Retry)
	evs := events(fs)
	require.Len(t, evs, 4)
	for i, want := range []string{"Bon", "jour", "multi\nline"} {
		assert.Equal(t, fmt.Sprint(i+1), evs[i].ID)
		assert.Equal(t, "data", evs[i].Event)
		assert.Equal(t, want, evs[i].Data)
	}
	assert.Equal(t, sseclient.Frame{ID: "4", HasID: true, Event: "done", Data: "{}", HasData: true}, evs[3])
	testutil.Eventually(t, func() bool { return e.unsubs.get() == 1 }, waitLimit)
}

func TestServeStream_ResumeCursorSources(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(writeWords("a", "b", "c", "d"))
	e.awaitTerminal(id)

	cases := map[string]struct {
		path    string
		headers map[string]string
		first   string
	}{
		"header":           {"/streams/" + id, map[string]string{"Last-Event-ID": "2"}, "3"},
		"query":            {"/streams/" + id + "?last_event_id=3", nil, "4"},
		"header wins":      {"/streams/" + id + "?last_event_id=1", map[string]string{"Last-Event-ID": "4"}, "5"},
		"absent from zero": {"/streams/" + id, nil, "1"},
	}
	for name, c := range cases {
		resp := e.get(context.Background(), c.path, c.headers)
		require.Equal(t, http.StatusOK, resp.status, name)
		evs := events(frames(t, resp))
		require.NotEmpty(t, evs, name)
		assert.Equal(t, c.first, evs[0].ID, name)
		assert.Equal(t, "done", evs[len(evs)-1].Event, name)
	}
}

// --- 204 ---

func TestServeStream_NoContentWhenClientHasTerminal(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(writeWords("a"))
	st := e.awaitTerminal(id)
	resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": fmt.Sprint(st.LastSeq)})
	require.Equal(t, http.StatusNoContent, resp.status)
	body, err := io.ReadAll(resp.body)
	require.NoError(t, err)
	assert.Empty(t, body)
}

// --- 400 ---

func TestServeStream_InvalidCursor(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(writeWords("a", "b"))
	st := e.awaitTerminal(id)
	for _, cursor := range []string{"abc", "-1", fmt.Sprint(st.LastSeq + 1), "99999999999999999999999"} {
		resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": cursor})
		require.Equal(t, http.StatusBadRequest, resp.status, "cursor %q", cursor)
		assert.Equal(t, map[string]any{"error": "invalid_last_event_id"}, jsonBody(t, resp))
	}
	resp := e.get(context.Background(), "/streams/"+id+"?last_event_id=x", nil)
	require.Equal(t, http.StatusBadRequest, resp.status)
}

// --- 404 ---

func TestServeStream_UnknownStream(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	resp := e.get(context.Background(), "/streams/does-not-exist", nil)
	require.Equal(t, http.StatusNotFound, resp.status)
	assert.Equal(t, map[string]any{"error": "stream_not_found"}, jsonBody(t, resp))
}

// --- 410 ---

// The built-in stores never trim (ADR 0004), but a store may trim a terminal stream during
// retention: a cursor before what it kept is 410, which clients treat as final.
func TestServeStream_GapAfterTrimming(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{wrap: func(s store.Store) store.Store { return &trimmingStore{Store: s, first: 7} }})
	id := e.start(writeWords("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
	st := e.awaitTerminal(id) // 10 data + done; the store kept seqs 7..11
	require.Equal(t, uint64(7), st.FirstSeq)

	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusGone, resp.status)
	assert.Equal(t, map[string]any{"error": "gap", "first_seq": float64(7)}, jsonBody(t, resp))

	resp = e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": "6"})
	require.Equal(t, http.StatusOK, resp.status, "resuming right before FirstSeq works")
	assert.Equal(t, "7", events(frames(t, resp))[0].ID)
}

// --- 401 / 403 ---

func TestServeStream_Authorize(t *testing.T) {
	t.Parallel()
	authorize := func(_ context.Context, r *http.Request, st rebound.Status) error {
		user := r.Header.Get("X-User")
		switch {
		case user == "":
			return ErrUnauthorized
		case user != st.Meta["user_id"]:
			return ErrForbidden
		}
		return nil
	}
	e := newEnv(t, envConfig{handlerOpts: []HandlerOption{WithAuthorize(authorize)}})
	id := e.start(writeWords("secret"), rebound.WithMeta(map[string]string{"user_id": "alice"}))
	e.awaitTerminal(id)

	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusUnauthorized, resp.status)
	assert.Equal(t, map[string]any{"error": "unauthorized"}, jsonBody(t, resp))

	resp = e.get(context.Background(), "/streams/"+id, map[string]string{"X-User": "mallory"})
	require.Equal(t, http.StatusForbidden, resp.status)
	assert.Equal(t, map[string]any{"error": "forbidden"}, jsonBody(t, resp))

	resp = e.get(context.Background(), "/streams/"+id, map[string]string{"X-User": "alice"})
	require.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "secret", events(frames(t, resp))[0].Data)

	// The cancel endpoint is guarded by the same hook.
	resp = e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusUnauthorized, resp.status)
}

func TestAuthorize_OtherErrorsAreForbidden(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{handlerOpts: []HandlerOption{WithAuthorize(
		func(context.Context, *http.Request, rebound.Status) error { return errors.New("policy engine down") },
	)}})
	id := e.start(writeWords("a"))
	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusForbidden, resp.status, "fail closed")
}

// --- pings ---

func TestServeStream_PingCadence(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{
		// A long StaleAfter keeps liveness checks out of the way: this test is about pings only.
		hubOpts:     []rebound.Option{rebound.WithStaleAfter(time.Hour)},
		handlerOpts: []HandlerOption{WithPingInterval(15 * time.Second)},
	})
	emit := make(chan string)
	id := e.start(func(ctx context.Context, w rebound.Writer) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case s := <-emit:
				if err := w.Write([]byte(s)); err != nil {
					return err
				}
			}
		}
	})
	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusOK, resp.status)
	rd := sseclient.NewReader(resp.body)
	f, err := rd.Next()
	require.NoError(t, err)
	require.True(t, f.HasRetry)

	// Producer heartbeat + MaxDuration timer, the subscription's blocking read, the ping ticker.
	e.clock.AwaitWaiters(t, 4)
	e.clock.Advance(15*time.Second - time.Millisecond)
	emit <- "before the first ping"
	f, err = rd.Next()
	require.NoError(t, err)
	assert.Equal(t, "before the first ping", f.Data, "no ping before the interval")

	for i := range 3 {
		e.clock.Advance(time.Millisecond)
		f, err = rd.Next()
		require.NoError(t, err)
		assert.Equal(t, []string{"ping"}, f.Comments, "ping %d", i)
		assert.False(t, f.HasID, "pings carry no id")
		assert.False(t, f.IsEvent())
		e.clock.Advance(15*time.Second - time.Millisecond)
	}
}

// --- disconnects ---

func TestServeStream_ClientDisconnectLeavesProducerRunning(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	release := make(chan struct{})
	id := e.start(func(_ context.Context, w rebound.Writer) error {
		if err := w.Write([]byte("before")); err != nil {
			return err
		}
		<-release
		return w.Write([]byte("after"))
	})

	ctx, cancel := context.WithCancel(context.Background())
	resp := e.get(ctx, "/streams/"+id, nil)
	rd := sseclient.NewReader(resp.body)
	for {
		f, err := rd.Next()
		require.NoError(t, err)
		if f.IsEvent() {
			require.Equal(t, "before", f.Data)
			break
		}
	}
	cancel() // the client goes away mid-stream
	testutil.Eventually(t, func() bool { return e.unsubs.get() == 1 }, waitLimit)

	st, err := e.hub.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, rebound.StateRunning, st.State, "the producer survives the disconnect")
	close(release)
	assert.Equal(t, rebound.StateDone, e.awaitTerminal(id).State)

	resp = e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": "1"})
	evs := events(frames(t, resp))
	require.Len(t, evs, 2)
	assert.Equal(t, "after", evs[0].Data)
	assert.Equal(t, "done", evs[1].Event)
}

// A store failure after the headers closes the response without a terminal event; the client
// resumes from its last id.
func TestServeStream_StoreFailureMidStreamClosesResponse(t *testing.T) {
	t.Parallel()
	var failReads failSwitch
	e := newEnv(t, envConfig{wrap: func(s store.Store) store.Store { return &readFaults{Store: s, fail: &failReads} }})
	release := make(chan struct{})
	id := e.start(func(_ context.Context, w rebound.Writer) error {
		if err := w.Write([]byte("one")); err != nil {
			return err
		}
		<-release
		return nil
	})
	resp := e.get(context.Background(), "/streams/"+id, nil)
	rd := sseclient.NewReader(resp.body)
	for {
		f, err := rd.Next()
		require.NoError(t, err)
		if f.IsEvent() {
			break
		}
	}
	failReads.set(true)
	e.clock.AwaitWaiters(t, 4)
	e.clock.Advance(5 * time.Second) // the blocking read times out, the next read fails
	_, err := rd.Next()
	require.ErrorIs(t, err, io.EOF, "response closed without a terminal event")
	close(release)
}

// --- 503 ---

func TestServeStream_StoreUnavailable(t *testing.T) {
	t.Parallel()
	var failStatus, failCancel failSwitch
	e := newEnv(t, envConfig{wrap: func(s store.Store) store.Store {
		return &statusFaults{Store: s, failStatus: &failStatus, failCancel: &failCancel}
	}})
	id := e.start(writeWords("a"))
	e.awaitTerminal(id)

	failStatus.set(true)
	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.status)
	assert.Equal(t, map[string]any{"error": "unavailable"}, jsonBody(t, resp))
	resp = e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusServiceUnavailable, resp.status)

	failStatus.set(false)
	failCancel.set(true)
	resp = e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusServiceUnavailable, resp.status)
}

func TestServeStream_NilHub(t *testing.T) {
	t.Parallel()
	x := NewHandler(nil)
	rec := httptest.NewRecorder()
	x.ServeStream(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/streams/a", http.NoBody), "a")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- cancel ---

func TestServeCancel(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(blockUntilCancelled)

	resp := e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusAccepted, resp.status)
	assert.Equal(t, map[string]any{"status": "cancelling"}, jsonBody(t, resp))
	st := e.awaitTerminal(id)
	assert.Equal(t, "cancelled", st.ErrorCode)

	resp = e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusAccepted, resp.status, "idempotent on a finished stream")

	resp = e.post("/streams/unknown/cancel")
	require.Equal(t, http.StatusNotFound, resp.status)
	assert.Equal(t, map[string]any{"error": "stream_not_found"}, jsonBody(t, resp))
}

func TestServeStream_CancelledStreamEndsWithErrorEvent(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(blockUntilCancelled)
	resp := e.get(context.Background(), "/streams/"+id, nil)
	require.Equal(t, http.StatusOK, resp.status)
	cancelResp := e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusAccepted, cancelResp.status)

	evs := events(frames(t, resp))
	require.Len(t, evs, 1)
	assert.Equal(t, "error", evs[0].Event)
	assert.JSONEq(t, `{"code":"cancelled","message":"cancelled by client"}`, evs[0].Data)
}

// --- write timeout ---

func TestServeStream_WriteTimeoutReleasesStuckClient(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{handlerOpts: []HandlerOption{WithWriteTimeout(100 * time.Millisecond)}})
	chunk := strings.Repeat("x", 60<<10)
	id := e.start(func(_ context.Context, w rebound.Writer) error {
		for range 400 { // ~24 MB: more than the socket buffers hold
			if err := w.Write([]byte(chunk)); err != nil {
				return err
			}
		}
		return nil
	})
	// A client that sends the request and then never reads.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+"/streams/"+id, http.NoBody)
	require.NoError(t, err)
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", e.srv.Listener.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, req.Write(conn))

	testutil.Eventually(t, func() bool { return e.unsubs.get() == 1 }, 20*time.Second)
}

func TestHandlerOptions(t *testing.T) {
	t.Parallel()
	x := NewHandler(nil, WithPingInterval(0), WithReconnectDelay(-1), WithWriteTimeout(-1))
	assert.Equal(t, defaultPingInterval, x.ping)
	assert.Equal(t, defaultReconnectDelay, x.retry)
	assert.Equal(t, defaultWriteTimeout, x.writeTimeout)

	x = NewHandler(nil, WithPingInterval(time.Second), WithReconnectDelay(0), WithWriteTimeout(time.Minute))
	assert.Equal(t, time.Second, x.ping)
	assert.Equal(t, time.Duration(0), x.retry)
	assert.Equal(t, time.Minute, x.writeTimeout)
}

// --- store fakes ---

type failSwitch struct {
	mu sync.Mutex
	on bool
}

func (f *failSwitch) set(on bool) { f.mu.Lock(); f.on = on; f.mu.Unlock() }

func (f *failSwitch) get() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.on }

var errStoreDown = errors.New("store down")

type readFaults struct {
	store.Store
	fail *failSwitch
}

func (r *readFaults) Read(ctx context.Context, id string, after uint64, limit int, block time.Duration) ([]store.Event, error) {
	if r.fail.get() {
		return nil, errStoreDown
	}
	return r.Store.Read(ctx, id, after, limit, block)
}

type statusFaults struct {
	store.Store
	failStatus, failCancel *failSwitch
}

func (s *statusFaults) Status(ctx context.Context, id string) (store.Status, error) {
	if s.failStatus.get() {
		return store.Status{}, errStoreDown
	}
	return s.Store.Status(ctx, id)
}

func (s *statusFaults) Cancel(ctx context.Context, id string) error {
	if s.failCancel.get() {
		return errStoreDown
	}
	return s.Store.Cancel(ctx, id)
}

// --- races between the handler's checks and Subscribe ---

// scriptedStatus lets a test rewrite the result of the n-th Status call (1-based).
type scriptedStatus struct {
	store.Store
	mu     sync.Mutex
	calls  int
	script map[int]func(store.Status, error) (store.Status, error)
	cancel error
}

func (s *scriptedStatus) Status(ctx context.Context, id string) (store.Status, error) {
	st, err := s.Store.Status(ctx, id)
	s.mu.Lock()
	s.calls++
	rewrite := s.script[s.calls]
	s.mu.Unlock()
	if rewrite != nil {
		return rewrite(st, err)
	}
	return st, err
}

func (s *scriptedStatus) Cancel(ctx context.Context, id string) error {
	if s.cancel != nil {
		return s.cancel
	}
	return s.Store.Cancel(ctx, id)
}

func TestServeStream_StreamChangesBeforeSubscribe(t *testing.T) {
	t.Parallel()
	gone := func(store.Status, error) (store.Status, error) { return store.Status{}, store.ErrNotFound }
	down := func(store.Status, error) (store.Status, error) { return store.Status{}, errStoreDown }
	shrunk := func(st store.Status, err error) (store.Status, error) { st.LastSeq = 0; return st, err }
	trimmed := func(st store.Status, err error) (store.Status, error) { st.FirstSeq = 5; return st, err }

	cases := map[string]struct {
		script     map[int]func(store.Status, error) (store.Status, error)
		wantStatus int
		wantBody   map[string]any
	}{
		// Call 1 is the handler's own Status; call 2 is inside Hub.Subscribe; call 3 is the
		// handler re-reading FirstSeq for a 410.
		"expired":           {map[int]func(store.Status, error) (store.Status, error){2: gone}, 404, map[string]any{"error": "stream_not_found"}},
		"cursor beyond end": {map[int]func(store.Status, error) (store.Status, error){2: shrunk}, 400, map[string]any{"error": "invalid_last_event_id"}},
		"trimmed":           {map[int]func(store.Status, error) (store.Status, error){2: trimmed, 3: trimmed}, 410, map[string]any{"error": "gap", "first_seq": float64(5)}},
		"trimmed then down": {map[int]func(store.Status, error) (store.Status, error){2: trimmed, 3: down}, 503, map[string]any{"error": "unavailable"}},
		"store down":        {map[int]func(store.Status, error) (store.Status, error){2: down}, 503, map[string]any{"error": "unavailable"}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scripted := &scriptedStatus{}
			e := newEnv(t, envConfig{wrap: func(s store.Store) store.Store { scripted.Store = s; return scripted }})
			id := e.start(writeWords("a", "b"))
			e.awaitTerminal(id)
			scripted.mu.Lock()
			scripted.calls, scripted.script = 0, c.script // arm the script for the request only
			scripted.mu.Unlock()

			resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": "1"})
			require.Equal(t, c.wantStatus, resp.status)
			assert.Equal(t, c.wantBody, jsonBody(t, resp))
		})
	}
}

func TestServeCancel_StreamExpiresBeforeCancel(t *testing.T) {
	t.Parallel()
	scripted := &scriptedStatus{cancel: store.ErrNotFound}
	e := newEnv(t, envConfig{wrap: func(s store.Store) store.Store { scripted.Store = s; return scripted }})
	id := e.start(writeWords("a"))
	e.awaitTerminal(id)
	resp := e.post("/streams/" + id + "/cancel")
	require.Equal(t, http.StatusNotFound, resp.status)
}

// failingResponse is a ResponseWriter whose body writes fail, as when the client disappears.
type failingResponse struct {
	header http.Header
	status int
}

func (f *failingResponse) Header() http.Header              { return f.header }
func (f *failingResponse) WriteHeader(status int)           { f.status = status }
func (f *failingResponse) Write([]byte) (int, error)        { return 0, errors.New("connection reset") }
func (f *failingResponse) Flush()                           {}
func (f *failingResponse) SetWriteDeadline(time.Time) error { return nil }

func TestServeStream_WriteFailuresEndTheHandler(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(writeWords("a"))
	e.awaitTerminal(id)

	w := &failingResponse{header: http.Header{}}
	e.handler.ServeStream(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/streams/"+id, http.NoBody), id)
	assert.Equal(t, http.StatusOK, w.status, "headers were sent, then the first write failed")
	testutil.Eventually(t, func() bool { return e.unsubs.get() == 1 }, waitLimit)

	w = &failingResponse{header: http.Header{}}
	e.handler.ServeStream(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/streams/nope", http.NoBody), "nope")
	assert.Equal(t, http.StatusNotFound, w.status, "an error body that cannot be written is dropped")
}
