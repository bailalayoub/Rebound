package main

// Regression tests for defects found in the pre-release review of v0.1.0. Each one failed before
// its fix; the comments point to the ADR that records the decision when there is one.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/examples/chat/llm"
	"github.com/bailalayoub/rebound/store/memory"
)

func postConformance(t *testing.T, cfg config, body string) (status int, respBody string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = hub.Close(ctx) // ends the long producers started by the tests
	})
	handler, err := newRouter(hub, llm.Fake{}, cfg, logger)
	require.NoError(t, err)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/conformance/streams", strings.NewReader(body))
	require.NoError(t, err)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(b)
}

// The test-only POST /conformance/streams, which lets anyone start producers of up to 1 Mi
// events, is not served by the default configuration; CONFORMANCE=1 mounts it.
func TestRegression_ConformanceEndpointIsOptIn(t *testing.T) {
	big := `{"text":"` + strings.Repeat("x", 1<<20) + `","chunk":1,"delay_ms":10000}`
	t.Setenv("CONFORMANCE", "")
	code, _ := postConformance(t, loadConfig(), big)
	assert.Equal(t, http.StatusNotFound, code, "off by default")

	t.Setenv("CONFORMANCE", "1")
	cfg := loadConfig()
	require.True(t, cfg.conformance)
	code, body := postConformance(t, cfg, `{"text":"Bonjour"}`)
	assert.Equal(t, http.StatusCreated, code, body)
}

// The conformance endpoint answers its own validation messages but does not echo the JSON
// decoder's internals.
func TestRegression_ConformanceErrorsDoNotEchoDecoderInternals(t *testing.T) {
	t.Parallel()
	cfg := config{storeKind: "memory", instance: "t", conformance: true}
	code, body := postConformance(t, cfg, `{"text":"x","unknown_field_name":1}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.JSONEq(t, `{"error":"conformance: invalid JSON body"}`, body)
	code, body = postConformance(t, cfg, `{"text":"x","chunk":0}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.JSONEq(t, `{"error":"conformance: chunk must be at least 1"}`, body)
}
