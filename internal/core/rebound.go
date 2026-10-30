package core

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/bailalayoub/rebound/store"
)

const (
	// maxIDAttempts bounds retries when a generated id collides (spec: max 3).
	maxIDAttempts = 3
	// maxStreamIDLen bounds stream ids, which travel in URLs and Redis keys.
	maxStreamIDLen = 128
	// readBatch is how many events a Subscription fetches per store read.
	readBatch = 64
	// maxReadBlock caps one blocking store read, so liveness is re-checked regularly.
	maxReadBlock = 5 * time.Second
	// shutdownWriteTimeout bounds the server_shutdown writes Close makes at its deadline (ADR 0006).
	shutdownWriteTimeout = time.Second
	// maxShutdownWriters bounds how many of those writes run at once.
	maxShutdownWriters = 32
)

// Status is a snapshot of a stream: state, error code, retained range, cancellation, timing and
// metadata.
type Status = store.Status

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
type Hub struct {
	store     store.Store
	cfg       config
	readBlock time.Duration

	mu        sync.Mutex
	closed    bool
	producers map[string]*producer // producers running on this Hub
	wg        sync.WaitGroup       // one per producer; Add only under mu while !closed
}

// New returns a Hub over s, configured by opts.
func New(s store.Store, opts ...Option) (*Hub, error) {
	if s == nil {
		return nil, errors.New("rebound: New: nil store")
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Hub{
		store:     s,
		cfg:       cfg,
		readBlock: min(cfg.staleAfter/3, maxReadBlock),
		producers: make(map[string]*producer),
	}, nil
}

// Start creates a stream and runs produce in a new goroutine. It returns as soon as the stream
// exists in the store. The producer's context derives from ctx without its cancellation: ending
// the request that called Start never stops the producer.
func (h *Hub) Start(ctx context.Context, produce ProducerFunc, opts ...StartOption) (*Stream, error) {
	if produce == nil {
		return nil, errors.New("rebound: start: nil producer")
	}
	var sc startConfig
	for _, opt := range opts {
		if err := opt(&sc); err != nil {
			return nil, err
		}
	}
	if h.isClosed() {
		return nil, ErrHubClosed
	}
	maxDuration := h.cfg.maxDuration
	if sc.maxDuration > 0 {
		maxDuration = sc.maxDuration
	}
	meta := store.Meta{
		CreatedAt: h.cfg.clock.Now(),
		IdleTTL:   h.cfg.idleTTL, // heartbeats refresh it, so it need not cover MaxDuration
		Retention: h.cfg.retention,
		MaxEvents: h.cfg.maxEvents,
		Attrs:     sc.meta,
	}
	id, err := h.create(ctx, sc.id, meta)
	if err != nil {
		return nil, err
	}

	base := context.WithoutCancel(ctx)
	pctx, cancel := context.WithCancelCause(base)
	p := &producer{id: id, cancel: cancel}
	if !h.register(p) {
		// Close won the race after the stream was created: end it rather than leave it running.
		cancel(errShutdown)
		h.writeTerminal(base, id, errorEvent(codeServerShutdown, ""))
		return nil, ErrHubClosed
	}
	h.hookStreamStart(id)
	h.cfg.logger.Info("rebound: stream started", "stream_id", id)
	go h.run(base, pctx, p, produce, maxDuration)
	return &Stream{ID: id}, nil
}

// create registers the stream, retrying generated ids that collide.
func (h *Hub) create(ctx context.Context, customID string, meta store.Meta) (string, error) {
	if customID != "" {
		if err := h.store.Create(ctx, customID, meta); err != nil {
			return "", fmt.Errorf("rebound: start %s: %w", customID, err)
		}
		return customID, nil
	}
	var err error
	for range maxIDAttempts {
		var id string
		if id, err = h.cfg.idGen(); err != nil {
			return "", fmt.Errorf("rebound: start: generate id: %w", err)
		}
		if err = validateStreamID(id); err != nil {
			return "", fmt.Errorf("rebound: start: generated id: %w", err)
		}
		if err = h.store.Create(ctx, id, meta); err == nil {
			return id, nil
		}
		if !errors.Is(err, ErrExists) {
			return "", fmt.Errorf("rebound: start %s: %w", id, err)
		}
	}
	return "", fmt.Errorf("rebound: start: %d generated ids collided: %w", maxIDAttempts, err)
}

// Cancel asks the stream's producer to stop. It is idempotent and succeeds on finished streams.
// The producer notices within HeartbeatInterval (immediately when it runs on this Hub), then the
// stream ends with error "cancelled".
func (h *Hub) Cancel(ctx context.Context, id string) error {
	if err := checkID(id); err != nil {
		return fmt.Errorf("rebound: cancel: %w", err)
	}
	if err := h.store.Cancel(ctx, id); err != nil {
		return fmt.Errorf("rebound: cancel %s: %w", id, err)
	}
	h.mu.Lock()
	p := h.producers[id]
	h.mu.Unlock()
	if p != nil {
		p.cancel(ErrCancelled)
	}
	h.cfg.logger.Info("rebound: stream cancel requested", "stream_id", id, "local", p != nil)
	return nil
}

// Status returns a snapshot of the stream.
func (h *Hub) Status(ctx context.Context, id string) (Status, error) {
	if err := checkID(id); err != nil {
		return Status{}, fmt.Errorf("rebound: status: %w", err)
	}
	st, err := h.store.Status(ctx, id)
	if err != nil {
		return Status{}, fmt.Errorf("rebound: status %s: %w", id, err)
	}
	return st, nil
}

// Close stops accepting Start (ErrHubClosed) and waits for the producers running on this Hub to
// finish. If ctx is done first, the remaining producers are cancelled, their streams end with
// error "server_shutdown" (written concurrently, within about one second), and Close returns an
// error wrapping ctx.Err(). A producer that ignores its context is abandoned one
// HeartbeatInterval later and stops heartbeating, but its goroutine runs until it returns; close
// the store only after that if it may still write (its writes then fail with ErrClosed).
func (h *Hub) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		h.cfg.logger.Info("rebound: hub closed")
		return nil
	case <-ctx.Done():
	}

	h.mu.Lock()
	remaining := slices.Collect(maps.Values(h.producers))
	h.mu.Unlock()
	for _, p := range remaining {
		p.cancel(errShutdown)
	}
	h.endAtShutdown(ctx, remaining)
	h.cfg.logger.Warn("rebound: hub closed at deadline", "reason", codeServerShutdown, "streams", len(remaining))
	return fmt.Errorf("rebound: close: %d producers still running were ended with %s: %w",
		len(remaining), codeServerShutdown, ctx.Err())
}

// endAtShutdown writes the server_shutdown terminals of ps concurrently, all within
// shutdownWriteTimeout: Close must not outlive its deadline by more, whatever the store does.
// A stream whose write does not finish in time is still ended by its own producer goroutine.
func (h *Hub) endAtShutdown(ctx context.Context, ps []*producer) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownWriteTimeout)
	defer cancel()
	slots := make(chan struct{}, maxShutdownWriters)
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Go(func() {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
				h.writeTerminal(wctx, p.id, errorEvent(codeServerShutdown, ""))
			case <-wctx.Done():
			}
		})
	}
	wg.Wait()
}

func (h *Hub) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// register records a local producer unless the Hub is closed.
func (h *Hub) register(p *producer) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.producers[p.id] = p
	h.wg.Add(1)
	return true
}

func (h *Hub) unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.producers, id)
}

// randomID returns 16 bytes from crypto/rand, base64url-encoded (22 characters).
func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// checkID answers ErrNotFound for ids that no stream can have, without calling the store
// (ADR 0008): they come from URLs and would otherwise become store keys.
func checkID(id string) error {
	if validateStreamID(id) != nil {
		return fmt.Errorf("malformed id %.40q: %w", id, ErrNotFound)
	}
	return nil
}

// validateStreamID accepts 1–128 URL-safe characters (RFC 3986 unreserved), which are also safe
// inside Redis hash tags.
func validateStreamID(id string) error {
	if id == "" || len(id) > maxStreamIDLen {
		return fmt.Errorf("rebound: invalid stream id %q: length must be 1-%d", id, maxStreamIDLen)
	}
	for i := range len(id) {
		c := id[i]
		if !isLower(c) && !isDigit(c) && (c < 'A' || c > 'Z') && c != '-' && c != '_' && c != '.' && c != '~' {
			return fmt.Errorf("rebound: invalid stream id %q: only [A-Za-z0-9._~-] allowed", id)
		}
	}
	return nil
}
