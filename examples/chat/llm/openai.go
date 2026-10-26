package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/bailalayoub/rebound/internal/sseclient"
)

// maxErrorBody bounds how much of a refused request's body is read for its message.
const maxErrorBody = 4 << 10

// maxErrorMessage bounds the provider message kept in a ProviderError.
const maxErrorMessage = 300

// authWithheld replaces the provider's message on 401 and 403: the hint says what to check.
const authWithheld = "authentication failed (the provider's message is not shown: it may quote the key)"

// OpenAI streams from any OpenAI-compatible chat completions endpoint (stream=true), with plain
// net/http and no SDK: OpenAI, Groq, OpenRouter, Gemini's OpenAI-compatible endpoint, Ollama…
type OpenAI struct {
	// BaseURL is the API base including its version, as in the providers' docs and SDKs, for
	// example https://api.openai.com/v1; "/chat/completions" is appended.
	BaseURL string
	APIKey  string
	Model   string
	// NoStreamUsage omits stream_options.include_usage, for providers that reject it.
	NoStreamUsage bool
	Client        *http.Client // default http.DefaultClient
}

// ProviderError is a request the provider refused, or an error it reported mid-stream: a bad key,
// an unknown model, a quota. Its message never contains the API key.
type ProviderError struct {
	Status  int    // HTTP status, 0 for an error reported inside the stream
	Message string // the provider's own message, shortened
}

func (e *ProviderError) Error() string {
	if e.Status == 0 {
		return "llm provider error: " + e.Message
	}
	return fmt.Sprintf("llm provider error: HTTP %d: %s", e.Status, e.Message)
}

// Hint says, for a person, what the error most likely means and what to check.
func (e *ProviderError) Hint() string {
	switch {
	case e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden:
		return "the provider rejected the API key: check LLM_API_KEY"
	case e.Status == http.StatusNotFound:
		return "model or endpoint not found: check LLM_MODEL and LLM_BASE_URL"
	case e.Status == http.StatusTooManyRequests:
		return "rate limited or out of quota at the provider"
	case e.Status == http.StatusBadRequest:
		return "the provider rejected the request: check LLM_MODEL (and LLM_API_KEY: Gemini answers 400 to a bad key)"
	case e.Status >= http.StatusInternalServerError:
		return "the provider failed: try again later"
	default:
		return "the provider stopped the answer"
	}
}

// Stream implements Provider.
func (o OpenAI) Stream(ctx context.Context, prompt string) (<-chan Chunk, error) {
	req := map[string]any{
		"model":    o.Model,
		"stream":   true,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	}
	if !o.NoStreamUsage {
		req["stream_options"] = map[string]bool{"include_usage": true}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if o.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+o.APIKey)
	}
	client := o.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("llm provider unreachable: %s", o.redact(err.Error()))
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody)) // best effort, for the message
		_ = resp.Body.Close()                                         // the request failed anyway
		msg := o.redact(errorMessage(raw))
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			msg = authWithheld // about the key, and some providers quote it: never shown
		}
		return nil, &ProviderError{Status: resp.StatusCode, Message: msg}
	}
	out := make(chan Chunk)
	go o.streamCompletion(ctx, resp, out)
	return out, nil
}

// endpoint is BaseURL + "/chat/completions".
func (o OpenAI) endpoint() string {
	return strings.TrimRight(o.BaseURL, "/") + "/chat/completions"
}

// redact removes the API key from a message, whatever the provider echoes back: the key itself,
// and the partly masked renditions some providers print ("sk-pr*****xyz", "sk-proj-AbCd...6789",
// the key in another case), which still reveal parts of it. A word goes when it contains "**" or
// shares keyFragment or more characters with the key, ignoring case; the punctuation around it
// stays.
func (o OpenAI) redact(s string) string {
	if o.APIKey != "" {
		s = strings.ReplaceAll(s, o.APIKey, "[redacted]")
	}
	key := strings.ToLower(o.APIKey)
	words := strings.Split(s, " ")
	for i, w := range words {
		core := strings.TrimLeft(w, `(["'`)
		lead := w[:len(w)-len(core)]
		core = strings.TrimRight(core, `.,;:!?)]"'`)
		trail := w[len(lead)+len(core):]
		if strings.Contains(core, "**") || sharesFragment(strings.ToLower(core), key) {
			words[i] = lead + "[redacted]" + trail
		}
	}
	return strings.Join(words, " ")
}

// keyFragment is the shortest run of key characters that makes a word count as revealing the key.
const keyFragment = 8

// sharesFragment reports whether word contains keyFragment consecutive bytes of key.
func sharesFragment(word, key string) bool {
	for i := 0; i+keyFragment <= len(key); i++ {
		if strings.Contains(word, key[i:i+keyFragment]) {
			return true
		}
	}
	return false
}

// errorMessage extracts the message of an error body: {"error":{"message":…}} (OpenAI, Groq,
// OpenRouter, Ollama), [{"error":{…}}] (Gemini) or {"error":"…"}; otherwise the text itself.
func errorMessage(raw []byte) string {
	type detail struct {
		Error json.RawMessage `json:"error"`
	}
	var one detail
	var many []detail
	switch {
	case json.Unmarshal(raw, &one) == nil && len(one.Error) > 0:
	case json.Unmarshal(raw, &many) == nil && len(many) > 0 && len(many[0].Error) > 0:
		one = many[0]
	default:
		return shorten(strings.TrimSpace(string(raw)))
	}
	var nested struct {
		Message string `json:"message"`
	}
	var flat string
	switch {
	case json.Unmarshal(one.Error, &nested) == nil && nested.Message != "":
		return shorten(nested.Message)
	case json.Unmarshal(one.Error, &flat) == nil && flat != "":
		return shorten(flat)
	default:
		return shorten(string(one.Error))
	}
}

func shorten(s string) string {
	if s == "" {
		return "(no message)"
	}
	if r := []rune(s); len(r) > maxErrorMessage {
		return string(r[:maxErrorMessage]) + "…"
	}
	return s
}

// streamCompletion owns resp: it forwards the completion to out, then closes both.
func (o OpenAI) streamCompletion(ctx context.Context, resp *http.Response, out chan<- Chunk) {
	defer close(out)
	defer func() { _ = resp.Body.Close() }() // read-only body, nothing to report
	o.readCompletion(ctx, resp.Body, out)
}

// completionChunk is the part of a streamed chat.completion.chunk the example uses.
type completionChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *Usage `json:"usage"`
	// Error is how some providers (OpenRouter) report a failure after the stream started.
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (o OpenAI) readCompletion(ctx context.Context, body io.Reader, out chan<- Chunk) {
	rd := sseclient.NewReader(body)
	for {
		f, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			send(ctx, out, Chunk{Err: fmt.Errorf("llm provider: read stream: %w", err)})
			return
		}
		if !f.IsEvent() {
			continue
		}
		if f.Data == "[DONE]" {
			return
		}
		var c completionChunk
		if err := json.Unmarshal([]byte(f.Data), &c); err != nil {
			send(ctx, out, Chunk{Err: fmt.Errorf("llm provider: decode chunk: %w", err)})
			return
		}
		if c.Error != nil {
			send(ctx, out, Chunk{Err: &ProviderError{Message: o.redact(shorten(c.Error.Message))}})
			return
		}
		for _, choice := range c.Choices {
			if choice.Delta.Content != "" && !send(ctx, out, Chunk{Text: choice.Delta.Content}) {
				return
			}
		}
		if c.Usage != nil && !send(ctx, out, Chunk{Usage: c.Usage}) {
			return
		}
	}
}
