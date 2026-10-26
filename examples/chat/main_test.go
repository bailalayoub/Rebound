package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/examples/chat/llm"
	"github.com/bailalayoub/rebound/store/memory"
)

func TestRouter(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })
	handler, err := newRouter(hub, llm.Fake{}, config{storeKind: "memory", instance: "app-test", provider: "fake", conformance: true}, logger)
	require.NoError(t, err)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	do := func(method, path, body string) (int, string) {
		req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		resp, err := srv.Client().Do(req)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}

	code, body := do(http.MethodGet, "/healthz", "")
	require.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"store":"memory","instance":"app-test","provider":"fake","model":"built-in fake model"}`, body)

	code, body = do(http.MethodPost, "/conformance/streams", `{"text":"Bonjour","chunk":3,"delay_ms":0}`)
	require.Equal(t, http.StatusCreated, code, body)
	var created struct {
		StreamID string `json:"stream_id"`
		URL      string `json:"url"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &created))
	require.Equal(t, "/streams/"+created.StreamID, created.URL)

	code, body = do(http.MethodGet, "/streams/"+created.StreamID, "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "data: Bon\n")
	assert.Contains(t, body, "data: jou\n")
	assert.Contains(t, body, "event: done\n")

	code, _ = do(http.MethodPost, "/streams/"+created.StreamID+"/cancel", "")
	assert.Equal(t, http.StatusAccepted, code)
	code, _ = do(http.MethodPost, "/conformance/streams", `{"text":""}`)
	assert.Equal(t, http.StatusBadRequest, code)

	// The chat: meta (instance and model) first, text chunks, usage, done.
	code, body = do(http.MethodPost, "/chat", `{"message":"hello"}`)
	require.Equal(t, http.StatusCreated, code, body)
	require.NoError(t, json.Unmarshal([]byte(body), &created))
	code, body = do(http.MethodGet, "/streams/"+created.StreamID, "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `id: 1`+"\n"+`event: meta`+"\n"+`data: {"instance":"app-test","model":"built-in fake model","provider":"fake"}`+"\n")
	assert.Contains(t, body, `data: {"text":"Resumable "}`)
	assert.Contains(t, body, `"usage":{"prompt_tokens":1`)
	assert.Contains(t, body, "event: done\n")

	for _, bad := range []string{``, `{}`, `{"message":"   "}`, `{"message":"x","extra":1}`} {
		code, _ = do(http.MethodPost, "/chat", bad)
		assert.Equal(t, http.StatusBadRequest, code, bad)
	}

	// The UI and the vendored client are served from the embedded files.
	code, body = do(http.MethodGet, "/", "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "<title>Rebound chat</title>")
	code, body = do(http.MethodGet, "/vendor/rebound/index.js", "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "subscribe")
	code, _ = do(http.MethodPost, "/nope", "")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestChatProducerFailures(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })
	st, err := hub.Start(context.Background(), chatProducer(llm.Fake{}, "please !fail", "i1", modelInfo{Provider: "fake"}))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		s, err := hub.Status(context.Background(), st.ID)
		return err == nil && s.State == rebound.StateError && s.ErrorCode == "producer_failed"
	}, 5*time.Second, 10*time.Millisecond)
}

// refusingModel is a provider that refuses every request, like a provider with a bad key.
type refusingModel struct{ err error }

func (m refusingModel) Stream(context.Context, string) (<-chan llm.Chunk, error) { return nil, m.err }

// A refused key or model reaches the UI as an "llm_error" event with the provider's message and a
// hint, before the terminal event: not a silent stop. The key is never in it.
func TestChatProducer_ReportsProviderErrors(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })
	refused := &llm.ProviderError{Status: http.StatusUnauthorized, Message: "Incorrect API key provided: [redacted]."}
	for _, tc := range []struct {
		provider llm.Provider
		want     llmError
	}{
		{refusingModel{refused}, llmError{Status: 401, Message: refused.Message, Hint: refused.Hint()}},
		{
			refusingModel{errors.New("llm provider unreachable: dial tcp: connection refused")},
			llmError{Message: "llm provider unreachable: dial tcp: connection refused", Hint: "could not reach the model: check LLM_BASE_URL and the network"},
		},
	} {
		st, err := hub.Start(context.Background(), chatProducer(tc.provider, "hi", "i1", modelInfo{Provider: "api.example.com", Model: "m"}))
		require.NoError(t, err)
		sub, err := hub.Subscribe(context.Background(), st.ID, 0)
		require.NoError(t, err)
		var kinds []string
		var got llmError
		for {
			ev, err := sub.Next(context.Background())
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			kinds = append(kinds, ev.Kind)
			if ev.Kind == "llm_error" {
				require.NoError(t, json.Unmarshal(ev.Data, &got))
			}
		}
		require.NoError(t, sub.Close())
		assert.Equal(t, []string{"meta", "llm_error", "error"}, kinds)
		assert.Equal(t, tc.want, got)
	}
}

func TestNewProvider(t *testing.T) {
	t.Parallel()
	p, err := newProvider(config{provider: "fake", fakeDelay: time.Millisecond, fakeRepeat: 1})
	require.NoError(t, err)
	assert.Equal(t, llm.Fake{Delay: time.Millisecond, Repeat: 1}, p)
	// Fail fast, with a readable message, when the real provider is half configured.
	_, err = newProvider(config{provider: "openai", llmURL: "https://api.openai.com/v1"})
	require.EqualError(t, err, "chat: LLM_PROVIDER=openai needs LLM_API_KEY (any value for a local Ollama) and LLM_MODEL; see playground/.env.example")
	_, err = newProvider(config{provider: "openai", llmKey: "k", llmURL: "https://api.openai.com/v1"})
	require.EqualError(t, err, "chat: LLM_PROVIDER=openai needs LLM_MODEL; see playground/.env.example")
	_, err = newProvider(config{provider: "openai", llmModel: "m", llmURL: "https://api.openai.com/v1"})
	require.ErrorContains(t, err, "needs LLM_API_KEY")
	_, err = newProvider(config{provider: "openai", llmKey: "k", llmModel: "m", llmURL: "api.groq.com/openai/v1"})
	require.ErrorContains(t, err, "LLM_BASE_URL must be an http(s) URL")
	// The value is never echoed. A key pasted on the LLM_BASE_URL line of .env
	// would otherwise be printed at startup, into the logs.
	const misplacedKey = "gsk_misplacedKey0123456789abcdef"
	_, err = newProvider(config{provider: "openai", llmKey: "k", llmModel: "m", llmURL: misplacedKey})
	require.ErrorContains(t, err, "LLM_BASE_URL must be an http(s) URL")
	assert.NotContains(t, err.Error(), "misplacedKey")
	p, err = newProvider(config{provider: "openai", llmKey: "k", llmURL: "https://h/v1", llmModel: "m", llmUsage: true})
	require.NoError(t, err)
	assert.Equal(t, llm.OpenAI{BaseURL: "https://h/v1", APIKey: "k", Model: "m"}, p)
	p, err = newProvider(config{provider: "openai", llmKey: "k", llmURL: "https://h/v1", llmModel: "m"})
	require.NoError(t, err)
	assert.Equal(t, llm.OpenAI{BaseURL: "https://h/v1", APIKey: "k", Model: "m", NoStreamUsage: true}, p)
	_, err = newProvider(config{provider: "other"})
	require.Error(t, err)
}

// /healthz and the UI name the provider host and the model, never the key.
func TestConfigAbout(t *testing.T) {
	t.Setenv("LLM_PROVIDER", "openai")
	t.Setenv("LLM_BASE_URL", "https://api.groq.com/openai/v1")
	t.Setenv("LLM_API_KEY", " gsk-secret ")
	t.Setenv("LLM_MODEL", "llama-3.1-8b-instant")
	t.Setenv("LLM_STREAM_USAGE", "0")
	cfg := loadConfig()
	assert.Equal(t, "gsk-secret", cfg.llmKey, "surrounding spaces from .env files are trimmed")
	assert.False(t, cfg.llmUsage)
	assert.Equal(t, modelInfo{Provider: "api.groq.com", Model: "llama-3.1-8b-instant"}, cfg.about())
	assert.Equal(t, modelInfo{Provider: "fake", Model: "built-in fake model"}, config{provider: "fake"}.about())
	t.Setenv("LLM_BASE_URL", "")
	assert.Equal(t, "https://api.openai.com/v1", loadConfig().llmURL)
}

func TestOpenStore(t *testing.T) {
	t.Parallel()
	s, release, err := openStore(config{storeKind: "memory"})
	require.NoError(t, err)
	require.NotNil(t, s)
	release()
	_, _, err = openStore(config{storeKind: "postgres"})
	require.ErrorContains(t, err, "memory or redis")
	_, _, err = openStore(config{storeKind: "redis", redisAddr: "127.0.0.1:1"})
	require.Error(t, err)
}
