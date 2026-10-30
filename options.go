package rebound

import (
	"log/slog"
	"time"

	"github.com/bailalayoub/rebound/internal/core"
)

// Option configures a Hub. Options are validated by New.
type Option = core.Option

// StartOption configures one stream.
type StartOption = core.StartOption

// WithLogger sets the structured logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return core.WithLogger(l) }

// WithClock sets the time source, for deterministic tests (default: the wall clock). Give the
// store the same clock.
func WithClock(clk Clock) Option { return core.WithClock(clk) }

// WithIDGenerator replaces the stream id generator (default: 16 random bytes from crypto/rand,
// base64url, 22 characters). Generated ids must be 1–128 characters from [A-Za-z0-9._~-].
func WithIDGenerator(gen func() string) Option { return core.WithIDGenerator(gen) }

// WithMaxDuration caps how long a producer may run (default 10 minutes); past it the stream ends
// with error "timeout".
func WithMaxDuration(d time.Duration) Option { return core.WithMaxDuration(d) }

// WithHeartbeatInterval sets how often a running producer records liveness and checks for
// cancellation from other instances (default 2 s).
func WithHeartbeatInterval(d time.Duration) Option { return core.WithHeartbeatInterval(d) }

// WithStaleAfter sets how long readers must observe a running stream without append or heartbeat
// before they end it with error "producer_lost" (default 15 s). It must exceed the heartbeat
// interval. A Writer retries transient store errors for at most StaleAfter/2 (ADR 0005).
func WithStaleAfter(d time.Duration) Option { return core.WithStaleAfter(d) }

// WithRetention sets how long a finished stream stays readable (default 10 minutes).
func WithRetention(d time.Duration) Option { return core.WithRetention(d) }

// WithIdleTTL sets the TTL refreshed on every append and heartbeat while a stream runs (default
// MaxDuration + StaleAfter + 1 minute). It must be at least StaleAfter + HeartbeatInterval.
func WithIdleTTL(d time.Duration) Option { return core.WithIdleTTL(d) }

// WithMaxEvents sets the hard cap on events per stream (default 10 000). A producer that writes
// more gets ErrTooManyEvents (with ErrClosed) and the stream ends with error
// "max_events_exceeded"; running streams are never trimmed (ADR 0004).
func WithMaxEvents(n int) Option { return core.WithMaxEvents(n) }

// WithExposeProducerErrors includes the producer's error text in the terminal "error" payload
// sent to clients (default false: only the code "producer_failed"). The text is always logged.
func WithExposeProducerErrors(expose bool) Option { return core.WithExposeProducerErrors(expose) }

// Hooks are optional callbacks for metrics. They run synchronously on the Hub's goroutines and
// must return quickly; a panicking hook is recovered and logged. Nil fields are skipped.
type Hooks struct {
	// OnStreamStart runs when Start has created a stream.
	OnStreamStart func(streamID string)
	// OnStreamEnd runs when a local producer's stream ended. reason is the stored terminal: "done"
	// or its error code (spec §5.1), also when another writer stored it, or "closed_elsewhere"
	// when it cannot be read back (the stream expired).
	OnStreamEnd func(streamID, reason string, duration time.Duration)
	// OnSubscribe runs when a Subscription is created.
	OnSubscribe func(streamID string, afterSeq uint64)
	// OnUnsubscribe runs when a Subscription is closed.
	OnUnsubscribe func(streamID string)
	// OnEvent runs after a producer event is stored.
	OnEvent func(streamID string, seq uint64, kind string)
}

// WithHooks installs callbacks for metrics.
func WithHooks(h Hooks) Option { return core.WithHooks(core.Hooks(h)) }

// WithMeta attaches application metadata to the stream. It is stored, returned in Status and
// passed to the HTTP Authorize hook (for example a user id). Do not put secrets in it.
func WithMeta(meta map[string]string) StartOption { return core.WithMeta(meta) }

// WithStreamID uses id instead of a generated one; Start returns ErrExists if it is taken. The id
// must be 1–128 characters from [A-Za-z0-9._~-].
func WithStreamID(id string) StartOption { return core.WithStreamID(id) }

// WithStreamMaxDuration overrides the Hub's MaxDuration for this stream.
func WithStreamMaxDuration(d time.Duration) StartOption { return core.WithStreamMaxDuration(d) }
