package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/bailalayoub/rebound"
)

// Limits on a start request, so the conformance endpoint cannot be used to exhaust a server.
const (
	maxTextRunes = 1 << 20
	maxDelay     = 10 * time.Second
	metaEvery    = 10 // with Kinds, every event whose seq is a multiple of 10 is a meta event
)

// StartRequest is the body of POST /conformance/streams (docs/06 §1).
type StartRequest struct {
	// Text is the full expected payload; required.
	Text string `json:"text"`
	// Chunk is the number of characters (runes) per data event; default 4.
	Chunk int `json:"chunk"`
	// DelayMS is the pause between events in milliseconds; default 10.
	DelayMS int `json:"delay_ms"`
	// FailAfter, when >= 0, makes the producer return an error after that many data events.
	FailAfter int `json:"fail_after"`
	// Kinds interleaves meta events: the event with seq n is meta {"i":n} when n % 10 == 0.
	Kinds bool `json:"kinds"`
}

// DecodeStartRequest reads and validates a start request, applying the defaults of docs/06.
func DecodeStartRequest(r io.Reader) (StartRequest, error) {
	req := StartRequest{Chunk: 4, DelayMS: 10, FailAfter: -1}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return StartRequest{}, fmt.Errorf("conformance: decode start request: %w", err)
	}
	if err := req.Validate(); err != nil {
		return StartRequest{}, &invalidRequest{err}
	}
	return req, nil
}

// invalidRequest marks a request that decoded but broke the contract; its message is safe to
// send back.
type invalidRequest struct{ err error }

func (e *invalidRequest) Error() string { return e.err.Error() }

func (e *invalidRequest) Unwrap() error { return e.err }

// Validate checks the request against the contract and the server limits.
func (r StartRequest) Validate() error {
	switch {
	case r.Text == "":
		return errors.New("conformance: text is required")
	case len([]rune(r.Text)) > maxTextRunes:
		return fmt.Errorf("conformance: text longer than %d characters", maxTextRunes)
	case r.Chunk < 1:
		return errors.New("conformance: chunk must be at least 1")
	case r.DelayMS < 0 || time.Duration(r.DelayMS)*time.Millisecond > maxDelay:
		return fmt.Errorf("conformance: delay_ms must be within 0-%d", maxDelay.Milliseconds())
	case r.FailAfter < -1:
		return errors.New("conformance: fail_after must be -1 or more")
	}
	return nil
}

// Chunks splits text into pieces of chunk runes, the payloads of the data events.
func Chunks(text string, chunk int) []string {
	runes := []rune(text)
	out := make([]string, 0, len(runes)/chunk+1)
	for start := 0; start < len(runes); start += chunk {
		out = append(out, string(runes[start:min(start+chunk, len(runes))]))
	}
	return out
}

// Expected is the exact sequence a correct server produces for a request: the non-terminal
// events in seq order, and the terminal outcome.
type Expected struct {
	Events []rebound.Event
	// Terminal is "done", or "producer_failed" when FailAfter cut the stream.
	Terminal string
}

// Text returns the concatenated payloads of the data events.
func (e Expected) Text() string {
	var b []byte
	for _, ev := range e.Events {
		if ev.Kind == "data" {
			b = append(b, ev.Data...)
		}
	}
	return string(b)
}

// ExpectedEvents computes what the producer of r writes, event by event.
func ExpectedEvents(r StartRequest) Expected {
	chunks := Chunks(r.Text, r.Chunk)
	exp := Expected{Terminal: "done"}
	data := 0
	for seq := uint64(1); data < len(chunks); seq++ {
		if r.FailAfter >= 0 && data == r.FailAfter {
			exp.Terminal = "producer_failed"
			return exp
		}
		if r.Kinds && seq%metaEvery == 0 {
			exp.Events = append(exp.Events, rebound.Event{Seq: seq, Kind: "meta", Data: metaPayload(seq)})
			continue
		}
		exp.Events = append(exp.Events, rebound.Event{Seq: seq, Kind: "data", Data: []byte(chunks[data])})
		data++
	}
	if r.FailAfter >= 0 && data == r.FailAfter {
		exp.Terminal = "producer_failed"
	}
	return exp
}

func metaPayload(seq uint64) []byte {
	return []byte(`{"i":` + strconv.FormatUint(seq, 10) + `}`)
}

// errPlannedFailure is what the producer returns when FailAfter is reached.
var errPlannedFailure = errors.New("conformance: planned failure (fail_after)")

// Producer returns the deterministic producer of r: it writes ExpectedEvents(r) with DelayMS
// between events, then returns nil or, with FailAfter, an error.
func (r StartRequest) Producer() rebound.ProducerFunc {
	exp := ExpectedEvents(r)
	delay := time.Duration(r.DelayMS) * time.Millisecond
	return func(ctx context.Context, w rebound.Writer) error {
		for i, ev := range exp.Events {
			if i > 0 && delay > 0 {
				if err := pause(ctx, delay); err != nil {
					return err
				}
			}
			if err := w.WriteEvent(ev.Kind, ev.Data); err != nil {
				return err
			}
		}
		if exp.Terminal == "producer_failed" {
			return errPlannedFailure
		}
		return nil
	}
}

// pause waits for d or until ctx is done.
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// maxBodyBytes bounds a start request: maxTextRunes runes of up to 4 bytes, plus the JSON around.
const maxBodyBytes = 4*maxTextRunes + 4096

// StartHandler serves POST /conformance/streams on hub: it validates the request, starts the
// deterministic producer and answers 201 {"stream_id","url"}.
func StartHandler(hub *rebound.Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := DecodeStartRequest(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			// Validation messages are this package's own; decoder internals are not echoed.
			msg := "conformance: invalid JSON body"
			var invalid *invalidRequest
			if errors.As(err, &invalid) {
				msg = invalid.Error()
			}
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
			return
		}
		st, err := hub.Start(r.Context(), req.Producer())
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "could not start"})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"stream_id": st.ID, "url": "/streams/" + st.ID})
	})
}

func writeJSON(w http.ResponseWriter, status int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The client is gone; there is nobody left to report the error to.
		return
	}
}
