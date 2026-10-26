package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/examples/chat/llm"
)

// maxMessageRunes bounds a chat message.
const maxMessageRunes = 4000

type chatRequest struct {
	Message string `json:"message"`
}

// chatHandler serves POST /chat: it starts a generation and answers 201 {"stream_id","url"}. The
// browser then follows the stream with @bailalayoub/rebound-client; the generation does not depend
// on that connection.
func chatHandler(hub *rebound.Hub, provider llm.Provider, instance string, about modelInfo) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req chatRequest
		dec := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
			return
		}
		message := strings.TrimSpace(req.Message)
		if message == "" || utf8.RuneCountInString(message) > maxMessageRunes {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_message"})
			return
		}
		st, err := hub.Start(c.Request.Context(), chatProducer(provider, message, instance, about),
			rebound.WithMeta(map[string]string{"kind": "chat"}))
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "unavailable"})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"stream_id": st.ID, "url": "/streams/" + st.ID})
	}
}

// modelInfo names the model that answers; it is shown in the UI and never includes the key.
type modelInfo struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// llmError is the payload of the "llm_error" event: why the model did not answer, for the UI.
// The terminal event that follows stays error "producer_failed" without details (spec §3.1).
type llmError struct {
	Status  int    `json:"status,omitempty"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
}

// chatProducer writes, in order: a meta event naming the instance that generates and the model,
// one data event {"text":…} per chunk, and a final data event {"usage":…} when the provider reports
// it. If the model fails, an "llm_error" event explains why before the stream ends. The Hub adds
// the terminal event.
func chatProducer(provider llm.Provider, prompt, instance string, about modelInfo) rebound.ProducerFunc {
	return func(ctx context.Context, w rebound.Writer) error {
		if err := writeJSONEvent(w, "meta", map[string]string{
			"instance": instance, "provider": about.Provider, "model": about.Model,
		}); err != nil {
			return err
		}
		chunks, err := provider.Stream(ctx, prompt)
		if err != nil {
			return reportLLMError(w, err)
		}
		for chunk := range chunks {
			switch {
			case chunk.Err != nil:
				return reportLLMError(w, chunk.Err)
			case chunk.Usage != nil:
				err = w.WriteJSON(map[string]any{"usage": chunk.Usage})
			default:
				err = w.WriteJSON(map[string]string{"text": chunk.Text})
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// reportLLMError writes an "llm_error" event describing err, then returns err so that the stream
// ends with error "producer_failed".
func reportLLMError(w rebound.Writer, err error) error {
	report := llmError{Message: err.Error(), Hint: "could not reach the model: check LLM_BASE_URL and the network"}
	var pe *llm.ProviderError
	if errors.As(err, &pe) {
		report = llmError{Status: pe.Status, Message: pe.Message, Hint: pe.Hint()}
	}
	if werr := writeJSONEvent(w, "llm_error", report); werr != nil {
		return errors.Join(err, werr)
	}
	return err
}

// writeJSONEvent appends v, JSON-encoded, as an event of the given custom kind.
func writeJSONEvent(w rebound.Writer, kind string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.WriteEvent(kind, data)
}
