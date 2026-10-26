// Package llm streams model answers for the example chat: a deterministic fake for demos and
// tests, and a client for any OpenAI-compatible /v1/chat/completions endpoint.
package llm

import "context"

// Usage is the token accounting a provider may report at the end of an answer.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Chunk is one piece of a streamed answer: text, final usage, or the error that ended it.
type Chunk struct {
	Text  string
	Usage *Usage
	Err   error
}

// Provider streams the answer to a prompt. The channel is closed when the answer is complete, after
// a Chunk with Err, or when ctx is done.
type Provider interface {
	Stream(ctx context.Context, prompt string) (<-chan Chunk, error)
}
