package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/internal/loadtest"
	"github.com/bailalayoub/rebound/store/memory"
)

func runCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errs bytes.Buffer
	code = run(context.Background(), args, &out, &errs)
	return code, out.String(), errs.String()
}

func TestUsage(t *testing.T) {
	t.Parallel()
	code, _, stderr := runCLI(t)
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "usage:")
	code, _, _ = runCLI(t, "nope")
	assert.Equal(t, 2, code)
	code, _, stderr = runCLI(t, "run", "-phases", "bad")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "phase")
	code, _, _ = runCLI(t, "run", "-h")
	assert.Equal(t, 2, code)
	code, _, _ = runCLI(t, "host-sample")
	assert.Equal(t, 2, code)
}

func TestRun_AgainstRealHub(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })
	mux := http.NewServeMux()
	mux.Handle("POST /loadtest/streams", loadtest.StartHandler(hub, "solo"))
	mux.Handle("GET /streams/{id}", httpx.NewHandler(hub, httpx.WithLogger(logger)).Stream(httpx.PathValue("id")))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	out := filepath.Join(t.TempDir(), "r.json")
	code, stdout, stderr := runCLI(t, "run", "-base-url", srv.URL, "-phases", "2x1-2@150ms", "-events", "5",
		"-delay-ms", "1", "-drop", "every:1-2", "-audit", "1", "-window", "50ms", "-out", out)
	require.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "2x1-2@150ms")
	assert.Contains(t, stderr, "ev/s=")
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	var rep loadtest.Report
	require.NoError(t, json.Unmarshal(data, &rep))
	assert.True(t, rep.Passed)
}

// A server that loses events makes run exit 1.
func TestRun_FailureExitsOne(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"stream_id":"s","url":"/streams/s"}`)
			return
		}
		_, _ = io.WriteString(w, "id: 2\nevent: done\ndata: {}\n\n") // seq 1 is missing
	}))
	t.Cleanup(srv.Close)
	code, stdout, _ := runCLI(t, "run", "-base-url", srv.URL, "-phases", "1x1@50ms", "-events", "1", "-quiet")
	assert.Equal(t, 1, code)
	assert.Contains(t, stdout, "gap=")
}

func TestSample_AppendsCSV(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"goroutines":7,"rss_bytes":100,"open_fds":9,"memstats":{"HeapAlloc":1}}`)
	}))
	t.Cleanup(srv.Close)
	out := filepath.Join(t.TempDir(), "s.csv")
	for range 2 {
		code, _, stderr := runCLI(t, "sample", "-url", srv.URL, "-source", "app-1", "-every", "20ms", "-duration", "50ms", "-out", out)
		require.Equal(t, 0, code, stderr)
	}
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(string(data), "unix_s,"), "one header for two runs")
	assert.Contains(t, string(data), ",app-1,goroutines,7\n")
}

func TestAnalyzeAndChart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "s.csv")
	var b strings.Builder
	b.WriteString("unix_s,source,metric,value\n")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	for i := range 61 {
		fmt.Fprintf(&b, "%d,app-1,rss_bytes,%d\n", start+int64(i)*60, 1000+10*i) // +600 per hour
	}
	require.NoError(t, os.WriteFile(in, []byte(b.String()), 0o600))

	code, stdout, stderr := runCLI(t, "analyze", "-in", in, "-series", "app-1:rss_bytes", "-table-every", "30m")
	require.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "app-1:rss_bytes  61")
	assert.Contains(t, stdout, "600")
	assert.Contains(t, stdout, "| 30 | 1300 |")
	assert.Contains(t, stdout, "| 60 | 1600 |")

	code, stdout, _ = runCLI(t, "analyze", "-in", in, "-series", "app-1:rss_bytes", "-from", "30m")
	require.Equal(t, 0, code)
	assert.Contains(t, stdout, "app-1:rss_bytes  31")

	svg := filepath.Join(dir, "c.svg")
	code, _, stderr = runCLI(t, "chart", "-in", in, "-series", "app-1:rss_bytes", "-title", "RSS", "-unit", "KiB", "-scale", "0.0009765625", "-out", svg)
	require.Equal(t, 0, code, stderr)
	data, err := os.ReadFile(svg)
	require.NoError(t, err)
	assert.Contains(t, string(data), "<polyline")

	code, _, _ = runCLI(t, "analyze", "-in", in, "-series", "bad")
	assert.Equal(t, 2, code)
}
