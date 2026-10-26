package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collect(t *testing.T, ch <-chan Chunk) (text string, usage *Usage, err error) {
	t.Helper()
	var b strings.Builder
	timeout := time.After(5 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return b.String(), usage, err
			}
			b.WriteString(c.Text)
			if c.Usage != nil {
				usage = c.Usage
			}
			if c.Err != nil {
				err = c.Err
			}
		case <-timeout:
			t.Fatal("stream did not end")
		}
	}
}

func TestFake(t *testing.T) {
	t.Parallel()
	ch, err := Fake{}.Stream(context.Background(), "hello world")
	require.NoError(t, err)
	text, usage, err := collect(t, ch)
	require.NoError(t, err)
	assert.Equal(t, cannedAnswer+" ", text)
	require.NotNil(t, usage)
	assert.Equal(t, 2, usage.PromptTokens)

	ch, err = Fake{Repeat: 2}.Stream(context.Background(), "x")
	require.NoError(t, err)
	text, _, err = collect(t, ch)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat(cannedAnswer+" ", 2), text)
}

func TestFake_FailAndHang(t *testing.T) {
	t.Parallel()
	ch, err := Fake{}.Stream(context.Background(), "please !fail")
	require.NoError(t, err)
	text, _, err := collect(t, ch)
	require.ErrorIs(t, err, ErrPlannedFailure)
	assert.NotEmpty(t, text)
	assert.Less(t, len(text), len(cannedAnswer))

	ctx, cancel := context.WithCancel(context.Background())
	ch, err = Fake{}.Stream(ctx, "!hang")
	require.NoError(t, err)
	cancel()
	text, _, err = collect(t, ch)
	require.NoError(t, err)
	assert.Empty(t, text)
}

func TestFake_StopsOnCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := Fake{Delay: time.Hour}.Stream(ctx, "x")
	require.NoError(t, err)
	first := <-ch
	assert.NotEmpty(t, first.Text)
	cancel()
	_, _, err = collect(t, ch)
	require.NoError(t, err)
}

func TestOpenAI_StreamsContentAndUsage(t *testing.T) {
	t.Parallel()
	var gotBody map[string]any
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/chat/completions", r.URL.Path, "BaseURL includes the version")
		gotAuth = r.Header.Get("Authorization")
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "text/event-stream")
		for _, piece := range []string{"Bon", "jour", " !"} {
			_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", piece)
		}
		_, _ = fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":3,\"total_tokens\":6}}\n\n")
		_, _ = fmt.Fprint(w, ": keep-alive\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)

	p := OpenAI{BaseURL: srv.URL + "/v1/", APIKey: "sk-test", Model: "m1"}
	ch, err := p.Stream(context.Background(), "say hi")
	require.NoError(t, err)
	text, usage, err := collect(t, ch)
	require.NoError(t, err)
	assert.Equal(t, "Bonjour !", text)
	assert.Equal(t, &Usage{PromptTokens: 3, CompletionTokens: 3, TotalTokens: 6}, usage)
	assert.Equal(t, "Bearer sk-test", gotAuth)
	assert.Equal(t, "m1", gotBody["model"])
	assert.Equal(t, true, gotBody["stream"])
	assert.Equal(t, map[string]any{"include_usage": true}, gotBody["stream_options"])
}

func TestOpenAI_NoStreamUsage(t *testing.T) {
	t.Parallel()
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1beta/openai/chat/completions", r.URL.Path)
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	ch, err := OpenAI{BaseURL: srv.URL + "/v1beta/openai/", APIKey: "k", Model: "m", NoStreamUsage: true}.Stream(context.Background(), "x")
	require.NoError(t, err)
	_, _, err = collect(t, ch)
	require.NoError(t, err)
	assert.NotContains(t, gotBody, "stream_options")
}

// The provider's refusal reaches the UI as a ProviderError: status, the provider's message (in
// any of the shapes providers use) and a hint, never the API key.
// withheld replaces the provider's message on 401/403: those messages are about the key, and some
// providers quote it, whole or partly masked.
const withheld = "authentication failed (the provider's message is not shown: it may quote the key)"

func TestOpenAI_ProviderErrors(t *testing.T) {
	t.Parallel()
	const key = "sk-secret-123"
	cases := []struct {
		name, body, want, hint string
		status                 int
	}{
		{"forbidden", `{"error":{"message":"key sk-secret-123 is disabled"}}`, withheld, "API key", 403},
		{
			"openai object", `{"error":{"message":"Incorrect API key provided: sk-secret-123.","type":"invalid_request_error"}}`,
			withheld, "API key", 401,
		},
		{
			"masked key", `{"error":{"message":"Incorrect API key provided: sk-secr*****-123. You can find your API key at https://platform.openai.com/account/api-keys."}}`,
			withheld, "API key", 401,
		},
		{
			"gemini array", `[{"error":{"code":400,"message":"Please pass a valid API key","status":"INVALID_ARGUMENT"}}]`,
			"Please pass a valid API key", "LLM_MODEL", 400,
		},
		{"flat string", `{"error":"model 'nope' not found"}`, "model 'nope' not found", "LLM_MODEL", 404},
		{"plain text", "upstream overloaded", "upstream overloaded", "try again", 503},
		{"empty", "", "(no message)", "quota", 429},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(srv.Close)
			_, err := OpenAI{BaseURL: srv.URL, APIKey: key, Model: "m"}.Stream(context.Background(), "x")
			var pe *ProviderError
			require.ErrorAs(t, err, &pe)
			assert.Equal(t, tc.status, pe.Status)
			assert.Equal(t, tc.want, pe.Message)
			assert.Contains(t, pe.Hint(), tc.hint)
			assert.NotContains(t, err.Error(), key)
			assert.NotContains(t, err.Error(), "sk-secr", "no fragment of the key either")
		})
	}
}

// Outside 401/403 the provider's message is shown, without any word that shares
// 8 or more characters with the key, whatever the case or the masking.
func TestOpenAI_RedactsKeyFragments(t *testing.T) {
	t.Parallel()
	const key = "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"
	cases := []struct{ echo, want string }{
		{"key sk-proj-AbCd...6789 has no quota", "key [redacted] has no quota"},
		{"key sk-proj-AbCd…6789 has no quota", "key [redacted] has no quota"},
		{"key sk-proj-AbCd*6789 has no quota", "key [redacted] has no quota"},
		{"key sk-proj-AbCd****6789 has no quota", "key [redacted] has no quota"},
		{"key SK-PROJ-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 has no quota", "key [redacted] has no quota"},
		{"key (" + key + "), has no quota", "key ([redacted]), has no quota"},
		{"key (sk-proj-AbCd...6789). Retry", "key ([redacted]). Retry"},
		{"model gpt-oss-20b has no quota", "model gpt-oss-20b has no quota"},
		{"you exceeded your quota, see https://platform.openai.com/account/billing.", "you exceeded your quota, see https://platform.openai.com/account/billing."},
	}
	for _, tc := range cases {
		t.Run(tc.echo, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":{"message":"`+tc.echo+`"}}`)
			}))
			t.Cleanup(srv.Close)
			_, err := OpenAI{BaseURL: srv.URL, APIKey: key, Model: "m"}.Stream(context.Background(), "x")
			var pe *ProviderError
			require.ErrorAs(t, err, &pe)
			assert.Equal(t, tc.want, pe.Message)
			assert.NotContains(t, strings.ToLower(err.Error()), "abcd")
		})
	}

	// The same inside a stream that already started.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `data: {"error":{"message":"key sk-proj-AbCd...6789 revoked"}}`+"\n\n")
	}))
	t.Cleanup(srv.Close)
	ch, err := OpenAI{BaseURL: srv.URL, APIKey: key, Model: "m"}.Stream(context.Background(), "x")
	require.NoError(t, err)
	_, _, err = collect(t, ch)
	require.EqualError(t, err, "llm provider error: key [redacted] revoked")
}

func TestOpenAI_ErrorInsideTheStream(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `data: {"choices":[{"delta":{"content":"Hi"}}]}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"error":{"message":"provider overloaded"}}`+"\n\n")
	}))
	t.Cleanup(srv.Close)
	ch, err := OpenAI{BaseURL: srv.URL, APIKey: "k", Model: "m"}.Stream(context.Background(), "x")
	require.NoError(t, err)
	text, _, err := collect(t, ch)
	assert.Equal(t, "Hi", text)
	var pe *ProviderError
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, "provider overloaded", pe.Message)
	assert.Equal(t, "the provider stopped the answer", pe.Hint())
}

func TestOpenAI_Errors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			http.Error(w, `{"error":"no key"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {not json\n\n")
	}))
	t.Cleanup(srv.Close)

	_, err := OpenAI{BaseURL: srv.URL, Model: "m"}.Stream(context.Background(), "x")
	require.EqualError(t, err, "llm provider error: HTTP 401: "+withheld)

	ch, err := OpenAI{BaseURL: srv.URL, APIKey: "k", Model: "m"}.Stream(context.Background(), "x")
	require.NoError(t, err)
	_, _, err = collect(t, ch)
	require.ErrorContains(t, err, "decode chunk")

	_, err = OpenAI{BaseURL: "http://127.0.0.1:1", Model: "m"}.Stream(context.Background(), "x")
	require.Error(t, err)
}
