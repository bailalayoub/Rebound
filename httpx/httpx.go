package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/sse"
	"github.com/bailalayoub/rebound/store"
)

// Errors an Authorize hook returns to refuse a request: ErrUnauthorized maps to 401, ErrForbidden
// (and any other error) to 403.
var (
	ErrUnauthorized = errors.New("httpx: unauthorized")
	ErrForbidden    = errors.New("httpx: forbidden")
)

// Defaults of spec §4.4.
const (
	defaultPingInterval   = 15 * time.Second
	defaultReconnectDelay = time.Second
	defaultWriteTimeout   = 10 * time.Second
)

// Handler serves the streams of a Hub over HTTP. Create it with NewHandler.
type Handler struct {
	hub          *rebound.Hub
	authorize    func(ctx context.Context, r *http.Request, st rebound.Status) error
	ping         time.Duration
	retry        time.Duration
	writeTimeout time.Duration
	logger       *slog.Logger
	clock        rebound.Clock // drives the ping ticker; tests replace it
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// NewHandler returns a Handler serving the streams of h.
func NewHandler(h *rebound.Hub, opts ...HandlerOption) *Handler {
	x := &Handler{
		hub:          h,
		ping:         defaultPingInterval,
		retry:        defaultReconnectDelay,
		writeTimeout: defaultWriteTimeout,
		logger:       slog.Default(),
		clock:        store.RealClock{},
	}
	for _, opt := range opts {
		opt(x)
	}
	return x
}

// WithAuthorize installs a hook called before serving or cancelling a stream, with the stream's
// Status (including the Meta given to Start). Returning ErrUnauthorized answers 401; any other
// error, typically ErrForbidden, answers 403.
func WithAuthorize(f func(ctx context.Context, r *http.Request, st rebound.Status) error) HandlerOption {
	return func(x *Handler) { x.authorize = f }
}

// WithPingInterval sets how often a keep-alive comment ": ping" is sent (default 15 s). Non-positive
// values keep the default.
func WithPingInterval(d time.Duration) HandlerOption {
	return func(x *Handler) {
		if d > 0 {
			x.ping = d
		}
	}
}

// WithReconnectDelay sets the "retry:" delay sent once at the start of each response (default 1 s).
// Negative values keep the default.
func WithReconnectDelay(d time.Duration) HandlerOption {
	return func(x *Handler) {
		if d >= 0 {
			x.retry = d
		}
	}
}

// WithWriteTimeout bounds each write and flush to the client (default 10 s), so a client that stops
// reading cannot hold the handler forever. Non-positive values keep the default.
func WithWriteTimeout(d time.Duration) HandlerOption {
	return func(x *Handler) {
		if d > 0 {
			x.writeTimeout = d
		}
	}
}

// WithLogger sets the logger for store failures (answered 503 or closing a stream) and write
// timeouts (default slog.Default()). Nil keeps the default. Event payloads are never logged.
func WithLogger(l *slog.Logger) HandlerOption {
	return func(x *Handler) {
		if l != nil {
			x.logger = l
		}
	}
}

// PathValue returns an id extractor reading the named wildcard of the Go 1.22 ServeMux pattern,
// for example PathValue("id") with "GET /streams/{id}".
func PathValue(name string) func(*http.Request) string {
	return func(r *http.Request) string { return r.PathValue(name) }
}

// Stream returns an http.Handler serving GET /streams/{id}, with the id read by idFrom.
func (x *Handler) Stream(idFrom func(*http.Request) string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.ServeStream(w, r, idFrom(r))
	})
}

// Cancel returns an http.Handler serving POST /streams/{id}/cancel, with the id read by idFrom.
func (x *Handler) Cancel(idFrom func(*http.Request) string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.ServeCancel(w, r, idFrom(r))
	})
}

// ServeStream resumes stream id after the client's Last-Event-ID (header, else the last_event_id
// query parameter, else from the beginning) and streams events as SSE until the terminal event or
// until the client goes away. The producer is never affected by the connection.
func (x *Handler) ServeStream(w http.ResponseWriter, r *http.Request, id string) {
	ctx := r.Context()
	cursor, err := sse.ParseLastEventID(r.Header.Get("Last-Event-ID"), r.URL.Query().Get("last_event_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_last_event_id")
		return
	}
	st, ok := x.authorized(w, r, id)
	if !ok {
		return
	}
	switch {
	case cursor > st.LastSeq:
		writeError(w, http.StatusBadRequest, "invalid_last_event_id")
		return
	case st.State.IsTerminal() && cursor == st.LastSeq:
		w.WriteHeader(http.StatusNoContent) // the client already has the terminal event
		return
	case st.FirstSeq > 0 && cursor+1 < st.FirstSeq:
		writeGap(w, st.FirstSeq)
		return
	}
	sub, err := x.hub.Subscribe(ctx, id, cursor)
	if err != nil {
		x.subscribeError(w, r, id, err)
		return
	}
	defer func() {
		// Close only releases the subscription and always succeeds.
		_ = sub.Close()
	}()
	x.stream(w, r, sub, id)
}

// ServeCancel cancels stream id and answers 202 {"status":"cancelling"}, also when the stream has
// already ended.
func (x *Handler) ServeCancel(w http.ResponseWriter, r *http.Request, id string) {
	if _, ok := x.authorized(w, r, id); !ok {
		return
	}
	if err := x.hub.Cancel(r.Context(), id); err != nil {
		if errors.Is(err, rebound.ErrNotFound) {
			writeError(w, http.StatusNotFound, "stream_not_found")
			return
		}
		x.unavailable(w, id, "cancel", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "cancelling"})
}

// authorized loads the stream's status and runs the Authorize hook, answering the request itself
// when either fails.
func (x *Handler) authorized(w http.ResponseWriter, r *http.Request, id string) (rebound.Status, bool) {
	if x.hub == nil {
		writeError(w, http.StatusInternalServerError, "no_hub")
		return rebound.Status{}, false
	}
	st, err := x.hub.Status(r.Context(), id)
	switch {
	case errors.Is(err, rebound.ErrNotFound):
		writeError(w, http.StatusNotFound, "stream_not_found")
		return st, false
	case err != nil:
		x.unavailable(w, id, "status", err)
		return st, false
	}
	if x.authorize == nil {
		return st, true
	}
	if err := x.authorize(r.Context(), r, st); err != nil {
		if errors.Is(err, ErrUnauthorized) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
		} else {
			writeError(w, http.StatusForbidden, "forbidden")
		}
		return st, false
	}
	return st, true
}

// subscribeError maps a Subscribe failure that raced with the checks of ServeStream.
func (x *Handler) subscribeError(w http.ResponseWriter, r *http.Request, id string, err error) {
	switch {
	case errors.Is(err, rebound.ErrNotFound):
		writeError(w, http.StatusNotFound, "stream_not_found")
	case errors.Is(err, rebound.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "invalid_last_event_id")
	case errors.Is(err, rebound.ErrGap):
		st, statusErr := x.hub.Status(r.Context(), id)
		if statusErr != nil {
			x.unavailable(w, id, "status", statusErr)
			return
		}
		writeGap(w, st.FirstSeq)
	default:
		x.unavailable(w, id, "subscribe", err)
	}
}

// unavailable answers 503 for a store failure, and logs it: the client only sees "unavailable".
func (x *Handler) unavailable(w http.ResponseWriter, id, op string, err error) {
	x.logger.Error("httpx: store unavailable", "stream_id", id, "op", op, "error", err)
	writeError(w, http.StatusServiceUnavailable, "unavailable")
}

// next is one result of Subscription.Next, handed from the pump goroutine to the writer loop.
type next struct {
	ev  rebound.Event
	err error
}

// stream writes the SSE response. A pump goroutine owns the subscription's Next calls; this loop
// owns the ResponseWriter. Both stop when the request ends, a write fails, or the terminal event
// has been written.
func (x *Handler) stream(w http.ResponseWriter, r *http.Request, sub *rebound.Subscription, id string) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	h.Set("X-Stream-ID", id)
	w.WriteHeader(http.StatusOK)

	out := &flusher{w: w, rc: http.NewResponseController(w), timeout: x.writeTimeout}
	defer out.clearDeadline()
	if err := out.send(func() error { return sse.WriteRetry(w, x.retry) }); err != nil {
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	pumped := make(chan next)
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		pump(ctx, sub, pumped)
	}()
	defer func() {
		cancel()
		<-pumpDone
	}()

	ping := x.clock.NewTicker(x.ping)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return // client went away; the producer is unaffected
		case n := <-pumped:
			if n.err != nil {
				// Store failure: close the response, the client resumes with Last-Event-ID.
				x.logger.Warn("httpx: stream read failed; closing the response", "stream_id", id, "error", n.err)
				return
			}
			if err := out.send(func() error { return sse.WriteEvent(w, n.ev) }); err != nil {
				x.writeFailed(id, err)
				return
			}
			if rebound.IsTerminal(n.ev.Kind) {
				return
			}
		case <-ping.C():
			if err := out.send(func() error { return sse.WriteComment(w, "ping") }); err != nil {
				x.writeFailed(id, err)
				return
			}
		}
	}
}

// writeFailed logs a failed write: a timeout means a client that stopped reading (Warn); anything
// else is usually a client that went away (Debug).
func (x *Handler) writeFailed(id string, err error) {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		x.logger.Warn("httpx: write timeout; closing the response", "stream_id", id, "error", err)
		return
	}
	x.logger.Debug("httpx: write failed", "stream_id", id, "error", err)
}

// pump forwards events from sub until ctx is done, an error occurs or the terminal event is sent.
func pump(ctx context.Context, sub *rebound.Subscription, out chan<- next) {
	for {
		ev, err := sub.Next(ctx)
		select {
		case out <- next{ev, err}:
		case <-ctx.Done():
			return
		}
		if err != nil || rebound.IsTerminal(ev.Kind) {
			return
		}
	}
}

// flusher writes with a per-write deadline and flushes after each write.
type flusher struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	timeout time.Duration
}

func (f *flusher) send(write func() error) error {
	// Network write deadlines are wall-clock times, so they use time.Now, not the injected clock.
	if err := f.rc.SetWriteDeadline(time.Now().Add(f.timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("httpx: set write deadline: %w", err)
	}
	if err := write(); err != nil {
		return err
	}
	if err := f.rc.Flush(); err != nil {
		return fmt.Errorf("httpx: flush: %w", err)
	}
	return nil
}

// clearDeadline removes the write deadline so it cannot affect a later request on a reused
// connection.
func (f *flusher) clearDeadline() {
	if err := f.rc.SetWriteDeadline(time.Time{}); err != nil {
		// Unsupported writers have no deadline to clear, and a closed connection has no later use.
		return
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": code})
}

func writeGap(w http.ResponseWriter, firstSeq uint64) {
	writeJSON(w, http.StatusGone, map[string]any{"error": "gap", "first_seq": firstSeq})
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	data, err := json.Marshal(body)
	if err != nil {
		// Unreachable for these literal bodies; answer with the status alone.
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if _, err := w.Write(append(data, '\n')); err != nil {
		// The client is gone; there is nobody left to report the error to.
		return
	}
}
