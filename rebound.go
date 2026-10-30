// Package rebound makes Server-Sent Events streams resumable.
//
// A Hub starts a producer (for example an LLM call) in a goroutine that is detached from any HTTP
// request. The producer appends events to a Store, which numbers them densely from 1. Readers
// subscribe after a cursor — the SSE Last-Event-ID — and receive exactly the events they missed,
// then keep following live, because replay and live delivery read the same log. A client can
// disconnect, reload, or reconnect to another server instance (with the Redis store) and still
// end up with the complete, ordered sequence of events.
//
// Guarantees (docs/02-spec.md §2): ordering, no gap and no duplicate, producer independence from
// connections, instance independence, guaranteed termination, bounded resources and an idempotent
// terminal event.
//
// HTTP handlers live in package httpx (net/http) and ginx (Gin); storage backends in store/memory
// and store/redis. This package is the public API; the implementation is in internal/core
// (ADR 0009).
package rebound

import (
	"context"

	"github.com/bailalayoub/rebound/internal/core"
	"github.com/bailalayoub/rebound/store"
)

// ProducerFunc generates a stream's events through w. It runs in its own goroutine, detached from
// the request that started it: ctx is cancelled only by Cancel, MaxDuration or Close. Returning nil
// ends the stream with "done"; returning an error ends it with error "producer_failed".
type ProducerFunc func(ctx context.Context, w Writer) error

// Writer appends events to a stream. Each event is stored exactly once: transient store errors are
// retried for at most StaleAfter/2 (ADR 0005). Payloads are stored as UTF-8 with LF line endings
// (invalid sequences become U+FFFD, CR and CRLF become LF; ADR 0007). Its methods return ErrClosed
// once the stream is terminal or cancelled (errors.Is also matches ErrCancelled after a cancel,
// and ErrTooManyEvents beyond MaxEvents); the producer should then return. It is safe for
// concurrent use; concurrent writes are stored in the order they acquire the writer.
type Writer interface {
	// Write appends an event of kind "data".
	Write(data []byte) error
	// WriteEvent appends an event of a custom kind matching ^[a-z][a-z0-9_]{0,31}$; the kinds done,
	// error, open and ping are reserved (ErrInvalidKind).
	WriteEvent(kind string, data []byte) error
	// WriteJSON appends v, JSON-encoded, as an event of kind "data".
	WriteJSON(v any) error
	// StreamID returns the id of the stream being written.
	StreamID() string
}

// Stream identifies a started stream.
type Stream struct{ ID string }

// Hub starts producers and serves subscriptions over a Store. It is safe for concurrent use.
// Several Hubs may share one Store (for example Redis), on one or several instances.
type Hub struct{ core *core.Hub }

// New returns a Hub over s, configured by opts.
func New(s store.Store, opts ...Option) (*Hub, error) {
	h, err := core.New(s, opts...)
	if err != nil {
		return nil, err
	}
	return &Hub{core: h}, nil
}

// Start creates a stream and runs produce in a new goroutine. It returns as soon as the stream
// exists in the store. The producer's context derives from ctx without its cancellation: ending
// the request that called Start never stops the producer.
func (h *Hub) Start(ctx context.Context, produce ProducerFunc, opts ...StartOption) (*Stream, error) {
	var p core.ProducerFunc
	if produce != nil { // a nil producer is rejected by core
		p = func(ctx context.Context, w core.Writer) error { return produce(ctx, w) }
	}
	st, err := h.core.Start(ctx, p, opts...)
	if err != nil {
		return nil, err
	}
	return &Stream{ID: st.ID}, nil
}

// Cancel asks the stream's producer to stop. It is idempotent and succeeds on finished streams.
// The producer notices within HeartbeatInterval (immediately when it runs on this Hub), then the
// stream ends with error "cancelled".
func (h *Hub) Cancel(ctx context.Context, id string) error { return h.core.Cancel(ctx, id) }

// Status returns a snapshot of the stream.
func (h *Hub) Status(ctx context.Context, id string) (Status, error) { return h.core.Status(ctx, id) }

// Close stops accepting Start (ErrHubClosed) and waits for the producers running on this Hub to
// finish. If ctx is done first, the remaining producers are cancelled, their streams end with
// error "server_shutdown" (written concurrently, within about one second), and Close returns an
// error wrapping ctx.Err(). A producer that ignores its context is abandoned one
// HeartbeatInterval later and stops heartbeating, but its goroutine runs until it returns; close
// the store only after that if it may still write (its writes then fail with ErrClosed).
func (h *Hub) Close(ctx context.Context) error { return h.core.Close(ctx) }

// Subscribe returns a Subscription to the events of stream id with Seq > afterSeq; 0 means from
// the beginning. It fails with ErrNotFound for unknown, expired or malformed ids, ErrInvalidCursor
// if afterSeq is beyond the last event, and ErrGap if a store trimmed the events right after
// afterSeq (only possible on a terminal stream, ADR 0004).
func (h *Hub) Subscribe(ctx context.Context, id string, afterSeq uint64) (*Subscription, error) {
	s, err := h.core.Subscribe(ctx, id, afterSeq)
	if err != nil {
		return nil, err
	}
	return &Subscription{core: s}, nil
}

// Subscription delivers a stream's events strictly after a cursor, replayed or live alike, up to
// and including the terminal event. One goroutine calls Next; Close may be called from any
// goroutine to stop it.
type Subscription struct{ core *core.Subscription }

// Next blocks until the next event is available or ctx is done. After it has delivered the
// terminal event ("done" or "error") it returns io.EOF, and only then: no other error matches
// io.EOF. While waiting, it ends a stream whose producer showed no activity for StaleAfter with
// error "producer_lost".
func (s *Subscription) Next(ctx context.Context) (Event, error) { return s.core.Next(ctx) }

// Close ends the subscription and interrupts a Next in progress. It is idempotent. The stream
// itself is unaffected.
func (s *Subscription) Close() error { return s.core.Close() }

// Event is one entry of a stream: a dense sequence number, a kind and an opaque payload.
type Event = store.Event

// Status is a snapshot of a stream: state, error code, retained range, cancellation, timing and
// metadata.
type Status = store.Status

// State is the lifecycle state of a stream: running, done or error.
type State = store.State

// Stream states.
const (
	StateRunning = store.StateRunning
	StateDone    = store.StateDone
	StateError   = store.StateError
)

// IsTerminal reports whether kind ends a stream ("done" or "error").
func IsTerminal(kind string) bool { return store.IsTerminal(kind) }

// Clock is the Hub's time source, injected with WithClock for deterministic tests.
type Clock = store.Clock

// Timer is the timer returned by Clock.NewTimer.
type Timer = store.Timer

// Ticker is the ticker returned by Clock.NewTicker.
type Ticker = store.Ticker

// Sentinel errors, matched with errors.Is. The first seven are declared by package store, because
// every Store returns them (docs/adr/0002); they are the same values.
var (
	// ErrNotFound reports an unknown or expired stream id.
	ErrNotFound = store.ErrNotFound
	// ErrExists reports that a stream with the same id already exists.
	ErrExists = store.ErrExists
	// ErrClosed reports a write to a stream that already holds its terminal event.
	ErrClosed = store.ErrClosed
	// ErrCancelled reports a write to a cancelled stream.
	ErrCancelled = store.ErrCancelled
	// ErrGap reports a cursor older than the oldest retained event; only possible after a store
	// trimmed a terminal stream (ADR 0004), and final for that reader.
	ErrGap = store.ErrGap
	// ErrTooManyEvents reports a write beyond MaxEvents; the stream ends with error
	// "max_events_exceeded" (ADR 0004).
	ErrTooManyEvents = store.ErrTooManyEvents
	// ErrConflict reports an append whose expected sequence number does not match the log
	// (ADR 0005).
	ErrConflict = store.ErrConflict

	// ErrInvalidCursor reports a subscription cursor beyond the last event of the stream.
	ErrInvalidCursor = core.ErrInvalidCursor
	// ErrInvalidKind reports an event kind that is reserved or does not match
	// ^[a-z][a-z0-9_]{0,31}$.
	ErrInvalidKind = core.ErrInvalidKind
	// ErrTooLarge reports event data larger than the maximum event size (64 KiB by default).
	ErrTooLarge = core.ErrTooLarge
	// ErrHubClosed reports a call on a Hub after Close.
	ErrHubClosed = core.ErrHubClosed
)
