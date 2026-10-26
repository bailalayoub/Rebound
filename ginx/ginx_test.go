package ginx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/ginx"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/internal/sseclient"
	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store/memory"
)

func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	os.Exit(m.Run())
}

type env struct {
	t   *testing.T
	hub *rebound.Hub
	srv *httptest.Server
}

func newEnv(t *testing.T, opts ...httpx.HandlerOption) *env {
	t.Helper()
	hub, err := rebound.New(memory.New(), rebound.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	x := httpx.NewHandler(hub, opts...)
	engine := gin.New()
	engine.GET("/streams/:id", ginx.Stream(x, "id"))
	engine.POST("/streams/:id/cancel", ginx.Cancel(x, "id"))
	srv := httptest.NewServer(engine)
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = hub.Close(ctx) // ends producers still blocked; the error only reports that
	})
	return &env{t: t, hub: hub, srv: srv}
}

func (e *env) start(produce rebound.ProducerFunc) string {
	e.t.Helper()
	st, err := e.hub.Start(context.Background(), produce)
	require.NoError(e.t, err)
	return st.ID
}

// call performs a request and returns the status, the body and the Content-Type.
func (e *env) call(method, path string, headers map[string]string) (status int, body []byte, contentType string) {
	e.t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, http.NoBody)
	require.NoError(e.t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.srv.Client().Do(req)
	require.NoError(e.t, err)
	defer func() { require.NoError(e.t, resp.Body.Close()) }()
	body, err = io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	return resp.StatusCode, body, resp.Header.Get("Content-Type")
}

func parseEvents(t *testing.T, body []byte) []sseclient.Frame {
	t.Helper()
	rd := sseclient.NewReader(bytes.NewReader(body))
	var out []sseclient.Frame
	for {
		f, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		if f.IsEvent() {
			out = append(out, f)
		}
	}
}

func words(ws ...string) rebound.ProducerFunc {
	return func(_ context.Context, w rebound.Writer) error {
		for _, word := range ws {
			if err := w.Write([]byte(word)); err != nil {
				return err
			}
		}
		return nil
	}
}

func (e *env) awaitDone(id string) {
	e.t.Helper()
	testutil.Eventually(e.t, func() bool {
		st, err := e.hub.Status(context.Background(), id)
		require.NoError(e.t, err)
		return st.State.IsTerminal()
	}, 5*time.Second)
}

func TestStream_HappyPathAndResume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.start(words("Bon", "jour"))

	status, body, ct := e.call(http.MethodGet, "/streams/"+id, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "text/event-stream; charset=utf-8", ct)
	evs := parseEvents(t, body)
	require.Len(t, evs, 3)
	assert.Equal(t, "Bon", evs[0].Data)
	assert.Equal(t, "done", evs[2].Event)

	status, body, _ = e.call(http.MethodGet, "/streams/"+id, map[string]string{"Last-Event-ID": "1"})
	require.Equal(t, http.StatusOK, status)
	evs = parseEvents(t, body)
	require.Len(t, evs, 2)
	assert.Equal(t, "2", evs[0].ID)

	status, body, _ = e.call(http.MethodGet, "/streams/"+id+"?last_event_id=3", nil)
	assert.Equal(t, http.StatusNoContent, status)
	assert.Empty(t, body)
}

func TestStream_Errors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	status, body, _ := e.call(http.MethodGet, "/streams/missing", nil)
	require.Equal(t, http.StatusNotFound, status)
	assert.JSONEq(t, `{"error":"stream_not_found"}`, string(body))

	id := e.start(words("a"))
	e.awaitDone(id)
	status, body, _ = e.call(http.MethodGet, "/streams/"+id, map[string]string{"Last-Event-ID": "abc"})
	require.Equal(t, http.StatusBadRequest, status)
	assert.JSONEq(t, `{"error":"invalid_last_event_id"}`, string(body))
}

func TestStream_Authorize(t *testing.T) {
	t.Parallel()
	e := newEnv(t, httpx.WithAuthorize(func(_ context.Context, r *http.Request, _ rebound.Status) error {
		if r.Header.Get("Authorization") == "" {
			return httpx.ErrUnauthorized
		}
		return httpx.ErrForbidden
	}))
	id := e.start(words("a"))
	status, _, _ := e.call(http.MethodGet, "/streams/"+id, nil)
	assert.Equal(t, http.StatusUnauthorized, status)
	status, _, _ = e.call(http.MethodGet, "/streams/"+id, map[string]string{"Authorization": "Bearer x"})
	assert.Equal(t, http.StatusForbidden, status)
}

func TestCancel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.start(func(ctx context.Context, _ rebound.Writer) error {
		<-ctx.Done()
		return nil
	})
	status, body, _ := e.call(http.MethodPost, "/streams/"+id+"/cancel", nil)
	require.Equal(t, http.StatusAccepted, status)
	assert.JSONEq(t, `{"status":"cancelling"}`, string(body))

	status, body, _ = e.call(http.MethodGet, "/streams/"+id, nil)
	require.Equal(t, http.StatusOK, status)
	evs := parseEvents(t, body)
	require.Len(t, evs, 1)
	var payload struct{ Code string }
	require.NoError(t, json.Unmarshal([]byte(evs[0].Data), &payload))
	assert.Equal(t, "cancelled", payload.Code)

	status, _, _ = e.call(http.MethodPost, "/streams/missing/cancel", nil)
	assert.Equal(t, http.StatusNotFound, status)
}

// Gin's ResponseWriter unwraps to the connection, so httpx's per-write deadlines apply under Gin.
func TestGinWriterSupportsWriteDeadline(t *testing.T) {
	t.Parallel()
	engine := gin.New()
	result := make(chan error, 1)
	engine.GET("/", func(c *gin.Context) {
		result <- http.NewResponseController(c.Writer).SetWriteDeadline(time.Now().Add(time.Second))
	})
	srv := httptest.NewServer(engine)
	defer srv.Close()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, http.NoBody)
	require.NoError(t, err)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, <-result)
}
