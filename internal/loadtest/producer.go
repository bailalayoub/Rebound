package loadtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bailalayoub/rebound"
)

// Limits on a start request, so the endpoint cannot be used to exhaust a server.
const (
	// MaxEvents is the Hub's default MaxEvents: a longer stream would end with max_events_exceeded.
	MaxEvents = 10_000
	// MaxSize bounds the padded payload, half of the Hub's default MaxEventBytes.
	MaxSize  = 32 << 10
	maxDelay = 10 * time.Second
	// padOverhead is the length of `,"x":""`, which padding adds around its characters.
	padOverhead = 7
)

// StartRequest is the body of POST /loadtest/streams.
type StartRequest struct {
	// Events is the number of data events the producer writes; default 400.
	Events int `json:"events"`
	// DelayMS is the pause between events in milliseconds; default 50.
	DelayMS int `json:"delay_ms"`
	// Size pads each payload to at least this many bytes; default 0 (no padding).
	Size int `json:"size"`
}

// Validate checks the request against the server limits.
func (r StartRequest) Validate() error {
	switch {
	case r.Events < 1 || r.Events > MaxEvents:
		return fmt.Errorf("loadtest: events must be within 1-%d", MaxEvents)
	case r.DelayMS < 0 || time.Duration(r.DelayMS)*time.Millisecond > maxDelay:
		return fmt.Errorf("loadtest: delay_ms must be within 0-%d", maxDelay.Milliseconds())
	case r.Size < 0 || r.Size > MaxSize:
		return fmt.Errorf("loadtest: size must be within 0-%d", MaxSize)
	}
	return nil
}

// Payload is the data of every event the load producer writes. The seq it was written under, the
// write time, the instance and the producer run let a reader check each event on its own: I must
// equal the event's seq, and H and N must never change within a stream (the producer neither moved
// nor restarted).
type Payload struct {
	I uint64 `json:"i"`           // the seq the producer writes this event as (1, 2, 3 …)
	T int64  `json:"t"`           // Unix time in nanoseconds, just before the write
	H string `json:"h"`           // instance running the producer
	N string `json:"n"`           // random nonce of this producer run
	X string `json:"x,omitempty"` // padding up to StartRequest.Size
}

// Encode returns p as JSON, padded with X to at least size bytes.
func (p Payload) Encode(size int) ([]byte, error) {
	p.X = ""
	data, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("loadtest: encode payload: %w", err)
	}
	if pad := size - len(data) - padOverhead; pad > 0 {
		p.X = strings.Repeat("x", pad)
		if data, err = json.Marshal(p); err != nil {
			return nil, fmt.Errorf("loadtest: encode payload: %w", err)
		}
	}
	return data, nil
}

// Producer returns the producer of r on instance: Events data events, DelayMS apart, each one a
// Payload stamped with the time it is written.
func (r StartRequest) Producer(instance string) rebound.ProducerFunc {
	delay := time.Duration(r.DelayMS) * time.Millisecond
	return func(ctx context.Context, w rebound.Writer) error {
		nonce, err := newNonce()
		if err != nil {
			return err
		}
		for i := 1; i <= r.Events; i++ {
			if i > 1 && delay > 0 {
				if err := pause(ctx, delay); err != nil {
					return err
				}
			}
			data, err := Payload{I: uint64(i), T: time.Now().UnixNano(), H: instance, N: nonce}.Encode(r.Size) //nolint:gosec // i is in 1..MaxEvents
			if err != nil {
				return err
			}
			if err := w.Write(data); err != nil {
				return err
			}
		}
		return nil
	}
}

func newNonce() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("loadtest: nonce: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
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

// DecodeStartRequest reads and validates a start request, applying the defaults.
func DecodeStartRequest(r io.Reader) (StartRequest, error) {
	req := StartRequest{Events: 400, DelayMS: 50}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return StartRequest{}, errInvalidJSON
	}
	if err := req.Validate(); err != nil {
		return StartRequest{}, err
	}
	return req, nil
}

var errInvalidJSON = errors.New("loadtest: invalid JSON body")

// maxBodyBytes bounds a start request: three small integers.
const maxBodyBytes = 4096

// StartHandler serves POST /loadtest/streams on hub: it validates the request, starts the
// timestamped producer on this instance and answers 201 {"stream_id","url"}.
func StartHandler(hub *rebound.Hub, instance string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := DecodeStartRequest(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		st, err := hub.Start(r.Context(), req.Producer(instance))
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
