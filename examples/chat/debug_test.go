package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The diagnostics listener exposes internals with no authentication: it only ever binds to a
// loopback address.
func TestNewDebugServer_LoopbackOnly(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{"0.0.0.0:6060", ":6060", "10.0.0.1:6060", "[::]:6060", "example.com:6060", "6060"} {
		_, err := newDebugServer(addr)
		require.Error(t, err, addr)
	}
	for _, addr := range []string{"127.0.0.1:6060", "localhost:6060", "[::1]:6060"} {
		_, err := newDebugServer(addr)
		require.NoError(t, err, addr)
	}
}

func TestNewDebugServer_Serves(t *testing.T) {
	t.Parallel()
	srv, err := newDebugServer("127.0.0.1:0")
	require.NoError(t, err)
	publishProcessVars() // as run does, once
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	get := func(path string) (int, []byte) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+path, http.NoBody)
		require.NoError(t, err)
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, body
	}
	code, body := get("/debug/vars")
	require.Equal(t, http.StatusOK, code)
	var vars struct {
		Goroutines int            `json:"goroutines"`
		RSS        int64          `json:"rss_bytes"`
		FDs        int            `json:"open_fds"`
		MemStats   map[string]any `json:"memstats"`
	}
	require.NoError(t, json.Unmarshal(body, &vars))
	assert.Positive(t, vars.Goroutines)
	assert.Contains(t, vars.MemStats, "HeapAlloc")
	if runtime.GOOS == "linux" {
		assert.Positive(t, vars.RSS)
		assert.Positive(t, vars.FDs)
	} else {
		assert.Equal(t, int64(-1), vars.RSS)
		assert.Equal(t, -1, vars.FDs)
	}
	code, _ = get("/debug/pprof/")
	assert.Equal(t, http.StatusOK, code)
}
