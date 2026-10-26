package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/internal/conformance"
	"github.com/bailalayoub/rebound/store/memory"
)

func target(t *testing.T) string {
	t.Helper()
	hub, err := rebound.New(memory.New(), rebound.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	streams := httpx.NewHandler(hub)
	mux := http.NewServeMux()
	mux.Handle("POST /conformance/streams", conformance.StartHandler(hub))
	mux.Handle("GET /streams/{id}", streams.Stream(httpx.PathValue("id")))
	mux.Handle("POST /streams/{id}/cancel", streams.Cancel(httpx.PathValue("id")))
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hub.Close(ctx) // ends producers of abandoned streams
	})
	return srv.URL
}

func quick(url string, extra ...string) []string {
	return append([]string{
		"--base-url", url, "--runs", "4", "--other-runs", "2", "--parallel", "2", "--text-size", "60",
		"--delay-ms", "0", "--max-reconnect-delay", "1ms", "--seed", "7",
	}, extra...)
}

func TestRun_TablePass(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run(quick(target(t), "--scenarios", "S1,s2,S9"), &stdout, &stderr)
	require.Equal(t, 0, code, "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	assert.Contains(t, stderr.String(), "seed 7")
	assert.Contains(t, stdout.String(), "PASS: 3/3 scenarios passed (reproduce with --seed 7)")
}

func TestRun_JSONReport(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run(quick(target(t), "--scenarios", "S8", "--json", "--trace"), &stdout, &stderr)
	require.Equal(t, 0, code, stderr.String())
	var report conformance.Report
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	assert.True(t, report.Passed)
	assert.Equal(t, uint64(7), report.Seed)
	require.Len(t, report.Scenarios, 1)
	assert.Equal(t, "S8", report.Scenarios[0].ID)
}

func TestRun_FailingServerExitsOne(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 1, run(quick(srv.URL, "--scenarios", "S1"), &stdout, &stderr))
	assert.Contains(t, stdout.String(), "FAIL: 0/1")
}

func TestRun_UsageErrorsExitTwo(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"--no-such-flag"},
		{"--runs", "zero"},
		{"--scenarios", "S42"},
		{"--drop-rate", "1.5"},
		{"stray-argument"},
	} {
		var stdout, stderr bytes.Buffer
		assert.Equal(t, 2, run(args, &stdout, &stderr), "args %v", args)
		assert.NotEmpty(t, stderr.String(), "args %v", args)
	}
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 2, run([]string{"-h"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "-drop-rate")
}

func TestRun_PicksAndPrintsASeed(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	args := quick(target(t), "--scenarios", "S8")
	args[len(args)-3] = "0" // --seed 0
	require.Equal(t, 0, run(args, &stdout, &stderr), stderr.String())
	assert.Regexp(t, `seed [1-9][0-9]*,`, stderr.String())
	assert.NotContains(t, stdout.String(), "--seed 0)")
}
