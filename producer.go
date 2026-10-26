package rebound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bailalayoub/rebound/store"
)

// Error codes of terminal "error" events (spec §5.1).
const (
	codeCancelled      = "cancelled"
	codeProducerFailed = "producer_failed"
	codeProducerPanic  = "producer_panic"
	codeTimeout        = "timeout"
	codeProducerLost   = "producer_lost"
	codeServerShutdown = "server_shutdown"
	codeMaxEvents      = "max_events_exceeded"
)

// reasonClosedElsewhere is the OnStreamEnd reason when the stream ended without this producer and
// its terminal event can no longer be read (the stream expired or the store failed).
const reasonClosedElsewhere = "closed_elsewhere"

// Backoff of the store retries made by the Writer and by terminal writes (ADR 0005).
const (
	retryFirstDelay = 50 * time.Millisecond
	retryMaxDelay   = time.Second
)

// Cancellation causes of a producer context, besides ErrCancelled (cancel requested),
// ErrTooManyEvents (MaxEvents reached) and ErrClosed / ErrNotFound (the stream ended or vanished
// without this producer).
var (
	errTimeout  = errors.New("rebound: max duration exceeded")
	errShutdown = errors.New("rebound: hub closed before the producer finished")
)

// defaultMessage is the client-facing text of an error code.
func defaultMessage(code string) string {
	switch code {
	case codeCancelled:
		return "cancelled by client"
	case codeProducerFailed:
		return "producer failed"
	case codeProducerPanic:
		return "producer panicked"
	case codeTimeout:
		return "max duration exceeded"
	case codeProducerLost:
		return "producer stopped sending heartbeats"
	case codeMaxEvents:
		return "too many events"
	default:
		return "server shutting down"
	}
}

// producer is a stream whose producer runs on this Hub.
type producer struct {
	id     string
	cancel context.CancelCauseFunc
}

// panicError carries a recovered producer panic.
type panicError struct{ value any }

func (e *panicError) Error() string { return fmt.Sprintf("rebound: producer panicked: %v", e.value) }

// terminal is the event that ends a stream.
type terminal struct {
	kind   string
	data   []byte
	reason string // "done" or the error code
}

func errorEvent(code, message string) terminal {
	if message == "" {
		message = defaultMessage(code)
	}
	data, err := json.Marshal(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message})
	if err != nil {
		// Unreachable for two strings; keep a valid payload anyway.
		data = []byte(`{"code":"` + code + `"}`)
	}
	return terminal{kind: store.KindError, data: data, reason: code}
}

// run owns the producer's lifecycle: it supervises liveness and MaxDuration, runs produce, then
// writes the terminal event and reports the end.
func (h *Hub) run(base, pctx context.Context, p *producer, produce ProducerFunc, maxDuration time.Duration) {
	defer h.wg.Done()
	defer h.unregister(p.id)
	defer p.cancel(nil) // releases the context's resources; no effect if already cancelled
	started := h.cfg.clock.Now()

	stopSupervisor := h.supervise(base, p, maxDuration)
	abandoned, err := h.await(pctx, produce, &writer{hub: h, ctx: pctx, id: p.id, cancel: p.cancel, next: 1})
	stopSupervisor()
	if abandoned {
		h.cfg.logger.Warn("rebound: producer did not return after its context was cancelled; abandoned",
			"stream_id", p.id, "reason", context.Cause(pctx).Error())
	}

	term, ok := h.outcome(pctx, err)
	reason := term.reason
	if !ok || h.writeTerminal(base, p.id, term) {
		reason = h.storedReason(base, p.id) // another writer ended the stream: report its terminal
	}
	elapsed := h.cfg.clock.Now().Sub(started)
	h.logEnd(p.id, reason, elapsed, err)
	h.hookStreamEnd(p.id, reason, elapsed)
}

// await runs produce in its own goroutine and returns its error. Once pctx is cancelled it waits
// one HeartbeatInterval for produce to return, then abandons it (ADR 0006): the stream is ended
// without it, and its goroutine runs until produce returns, every write failing with ErrClosed.
func (h *Hub) await(pctx context.Context, produce ProducerFunc, w Writer) (abandoned bool, err error) {
	done := make(chan error, 1) // buffered: an abandoned producer can still return
	go func() { done <- callProducer(pctx, produce, w) }()
	select {
	case err := <-done:
		return false, err
	case <-pctx.Done():
	}
	grace := h.cfg.clock.NewTimer(h.cfg.heartbeat)
	defer grace.Stop()
	select {
	case err := <-done:
		return false, err
	case <-grace.C():
		return true, nil
	}
}

// callProducer runs produce, turning a panic into a *panicError.
func callProducer(ctx context.Context, produce ProducerFunc, w Writer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{value: r}
		}
	}()
	return produce(ctx, w)
}

// outcome maps how the producer ended to its terminal event. ok is false when the stream already
// ended without this producer (another writer's terminal, or expiry), so nothing is written.
func (h *Hub) outcome(pctx context.Context, err error) (term terminal, ok bool) {
	var pe *panicError
	cause := context.Cause(pctx)
	switch {
	case errors.As(err, &pe):
		return errorEvent(codeProducerPanic, ""), true
	case errors.Is(cause, errTimeout):
		return errorEvent(codeTimeout, ""), true
	case errors.Is(cause, ErrCancelled):
		return errorEvent(codeCancelled, ""), true
	case errors.Is(cause, errShutdown):
		return errorEvent(codeServerShutdown, ""), true
	case errors.Is(cause, ErrTooManyEvents):
		return errorEvent(codeMaxEvents, ""), true
	case errors.Is(cause, ErrClosed), errors.Is(cause, ErrNotFound):
		return terminal{}, false
	case err != nil:
		msg := ""
		if h.cfg.exposeErrors {
			msg = err.Error()
		}
		return errorEvent(codeProducerFailed, msg), true
	default:
		return terminal{kind: store.KindDone, data: []byte("{}"), reason: store.KindDone}, true
	}
}

// writeTerminal appends the terminal event with Force, retrying transient store errors (ADR 0005).
// It reports whether another writer had already stored the terminal event: losing that race is
// expected (G7). Other failures are logged.
func (h *Hub) writeTerminal(ctx context.Context, id string, term terminal) (lost bool) {
	_, err := h.appendWithRetry(ctx, id, store.AppendRequest{Kind: term.kind, Data: term.data, Force: true})
	switch {
	case err == nil:
	case errors.Is(err, ErrClosed):
		// Another writer stored the terminal event first (G7): exactly one exists, nothing to do.
		h.cfg.logger.Debug("rebound: terminal already written", "stream_id", id, "reason", term.reason)
		return true
	default:
		h.cfg.logger.Error("rebound: write terminal event", "stream_id", id, "reason", term.reason, "error", err)
	}
	return false
}

// storedReason reads back how a stream ended when another writer stored its terminal event, so
// that hooks and logs report what clients received (ADR 0006).
func (h *Hub) storedReason(ctx context.Context, id string) string {
	st, err := h.store.Status(ctx, id)
	switch {
	case err != nil, !st.State.IsTerminal():
		return reasonClosedElsewhere
	case st.State == StateDone:
		return store.KindDone
	default:
		return st.ErrorCode
	}
}

// appendWithRetry appends req stamped with the Hub's clock. Transient store errors are retried
// with backoff (50 ms doubling to 1 s) for at most StaleAfter/2; errors meaning that the stream
// ended, and ctx being done, are final. Retrying is safe: data appends carry their expected Seq
// and a second terminal append is ErrClosed (ADR 0005).
func (h *Hub) appendWithRetry(ctx context.Context, id string, req store.AppendRequest) (uint64, error) {
	giveUp := h.cfg.clock.Now().Add(h.cfg.staleAfter / 2)
	delay := retryFirstDelay
	for {
		req.At = h.cfg.clock.Now()
		seq, err := h.store.Append(ctx, id, req)
		if err == nil || streamEnded(err) || ctx.Err() != nil || !h.cfg.clock.Now().Add(delay).Before(giveUp) {
			return seq, err
		}
		h.cfg.logger.Warn("rebound: store append failed; retrying", "stream_id", id, "error", err,
			"retry_in_ms", delay.Milliseconds())
		if !h.pause(ctx, delay) {
			return 0, err
		}
		delay = min(2*delay, retryMaxDelay)
	}
}

// streamEnded reports store errors that no retry can change.
func streamEnded(err error) bool {
	for _, final := range []error{ErrClosed, ErrCancelled, ErrNotFound, ErrTooManyEvents, ErrConflict} {
		if errors.Is(err, final) {
			return true
		}
	}
	return false
}

// pause waits d on the Hub's clock; it returns false if ctx is done first.
func (h *Hub) pause(ctx context.Context, d time.Duration) bool {
	t := h.cfg.clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return true
	case <-ctx.Done():
		return false
	}
}

func (h *Hub) logEnd(id, reason string, elapsed time.Duration, err error) {
	attrs := []any{"stream_id", id, "reason", reason, "duration_ms", elapsed.Milliseconds()}
	if err != nil {
		attrs = append(attrs, "error", err) // producer errors are always logged (spec §3.1)
	}
	h.cfg.logger.Info("rebound: stream ended", attrs...)
}

// supervise runs the heartbeat and the MaxDuration timer until the returned stop is called.
func (h *Hub) supervise(ctx context.Context, p *producer, maxDuration time.Duration) (stop func()) {
	quit := make(chan struct{})
	exited := make(chan struct{})
	heartbeat := h.cfg.clock.NewTicker(h.cfg.heartbeat)
	deadline := h.cfg.clock.NewTimer(maxDuration)
	go func() {
		defer close(exited)
		defer heartbeat.Stop()
		defer deadline.Stop()
		for {
			select {
			case <-quit:
				return
			case <-deadline.C():
				p.cancel(errTimeout)
			case <-heartbeat.C():
				h.beat(ctx, p)
			}
		}
	}()
	return func() {
		close(quit)
		<-exited
	}
}

// beat records liveness and propagates a cancel requested on any instance, or the end of the
// stream by another writer, to the producer.
func (h *Hub) beat(ctx context.Context, p *producer) {
	if err := h.store.Touch(ctx, p.id, h.cfg.clock.Now()); err != nil {
		h.stopOnStoreError(p, "touch", err)
		return
	}
	st, err := h.store.Status(ctx, p.id)
	if err != nil {
		h.stopOnStoreError(p, "status", err)
		return
	}
	switch {
	case st.Cancelled:
		p.cancel(ErrCancelled)
	case st.State.IsTerminal():
		p.cancel(ErrClosed)
	}
}

func (h *Hub) stopOnStoreError(p *producer, op string, err error) {
	if errors.Is(err, ErrNotFound) {
		h.cfg.logger.Warn("rebound: stream vanished while running", "stream_id", p.id, "reason", "expired")
		p.cancel(ErrNotFound)
		return
	}
	// A transient store failure must not kill a healthy producer; the next beat retries.
	h.cfg.logger.Error("rebound: heartbeat "+op, "stream_id", p.id, "error", err)
}

// writer is the Writer given to a producer.
type writer struct {
	hub *Hub
	// ctx is the producer's context. Writer methods take no context (spec §3), so the writer keeps
	// the one it was created for.
	ctx    context.Context
	id     string
	cancel context.CancelCauseFunc

	// mu serializes appends: each one carries the expected seq next (ADR 0005), so concurrent
	// writes stay dense and ordered. It is held across the store call on purpose; it guards only
	// this producer's sequence.
	mu   sync.Mutex
	next uint64
}

// StreamID returns the id of the stream being written.
func (w *writer) StreamID() string { return w.id }

// Write appends an event of kind "data".
func (w *writer) Write(data []byte) error { return w.WriteEvent(store.KindData, data) }

// WriteJSON appends v, JSON-encoded, as an event of kind "data".
func (w *writer) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("rebound: write %s: encode json: %w", w.id, err)
	}
	return w.WriteEvent(store.KindData, data)
}

// WriteEvent validates kind, normalizes data (ADR 0007) and checks its size, then appends the
// event exactly once (ADR 0005) unless the stream is over.
func (w *writer) WriteEvent(kind string, data []byte) error {
	if err := validateKind(kind); err != nil {
		return err
	}
	data = normalizePayload(data)
	if len(data) > w.hub.cfg.maxEventBytes {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, len(data), w.hub.cfg.maxEventBytes)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if context.Cause(w.ctx) != nil {
		return w.closedErr()
	}
	seq, err := w.hub.appendWithRetry(w.ctx, w.id, store.AppendRequest{Kind: kind, Data: data, Seq: w.next})
	switch {
	case err == nil:
		w.next = seq + 1
		w.hub.hookEvent(w.id, seq, kind)
		return nil
	case errors.Is(err, ErrCancelled):
		w.cancel(ErrCancelled)
	case errors.Is(err, ErrTooManyEvents):
		w.cancel(ErrTooManyEvents)
	case errors.Is(err, ErrClosed), errors.Is(err, ErrNotFound):
		w.cancel(ErrClosed)
	case context.Cause(w.ctx) == nil:
		return fmt.Errorf("rebound: write %s: %w", w.id, err)
	}
	return w.closedErr()
}

// closedErr is returned once the producer's context is done: always ErrClosed, plus ErrCancelled
// or ErrTooManyEvents when that is why the stream ended.
func (w *writer) closedErr() error {
	cause := context.Cause(w.ctx)
	for _, why := range []error{ErrCancelled, ErrTooManyEvents} {
		if errors.Is(cause, why) {
			return fmt.Errorf("rebound: write %s: %w (%w)", w.id, ErrClosed, why)
		}
	}
	return fmt.Errorf("rebound: write %s: %w", w.id, ErrClosed)
}

// normalizePayload returns data as valid UTF-8 with LF line endings (ADR 0007): invalid sequences
// become U+FFFD and CRLF / CR become LF, which is what SSE clients would decode anyway. Data that
// is already normalized is returned as is.
func normalizePayload(data []byte) []byte {
	if utf8.Valid(data) && bytes.IndexByte(data, '\r') < 0 {
		return data
	}
	s := strings.ToValidUTF8(string(data), "�")
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return []byte(strings.ReplaceAll(s, "\r", "\n"))
}
