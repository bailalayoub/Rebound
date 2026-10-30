package core

import (
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/bailalayoub/rebound/store"
)

// Defaults of spec §3.2.
const (
	defaultMaxDuration   = 10 * time.Minute
	defaultHeartbeat     = 2 * time.Second
	defaultStaleAfter    = 15 * time.Second
	defaultRetention     = 10 * time.Minute
	defaultMaxEvents     = 10_000
	defaultMaxEventBytes = 64 << 10
	// idleTTLMargin is added to MaxDuration + StaleAfter for the default IdleTTL.
	idleTTLMargin = time.Minute
)

// Option configures a Hub. Options are validated by New.
type Option func(*config) error

type config struct {
	logger        *slog.Logger
	clock         Clock
	idGen         func() (string, error)
	maxDuration   time.Duration
	heartbeat     time.Duration
	staleAfter    time.Duration
	retention     time.Duration
	idleTTL       time.Duration // 0: derived from maxDuration and staleAfter
	maxEvents     int
	maxEventBytes int
	exposeErrors  bool
	hooks         Hooks
}

func defaultConfig() config {
	return config{
		logger:        slog.Default(),
		clock:         store.RealClock{},
		idGen:         randomID,
		maxDuration:   defaultMaxDuration,
		heartbeat:     defaultHeartbeat,
		staleAfter:    defaultStaleAfter,
		retention:     defaultRetention,
		maxEvents:     defaultMaxEvents,
		maxEventBytes: defaultMaxEventBytes,
	}
}

// validate checks relations between options once all of them are applied.
func (c *config) validate() error {
	if c.staleAfter <= c.heartbeat {
		return fmt.Errorf("rebound: StaleAfter (%v) must be longer than HeartbeatInterval (%v), "+
			"or live producers would be declared lost", c.staleAfter, c.heartbeat)
	}
	if c.idleTTL == 0 {
		c.idleTTL = c.maxDuration + c.staleAfter + idleTTLMargin
	}
	if minTTL := c.staleAfter + c.heartbeat; c.idleTTL < minTTL {
		return fmt.Errorf("rebound: IdleTTL (%v) must be at least StaleAfter + HeartbeatInterval (%v), "+
			"or running streams could expire between heartbeats or before readers end them (ADR 0008)", c.idleTTL, minTTL)
	}
	return nil
}

func positive(name string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("rebound: %s must be positive, got %v", name, d)
	}
	return nil
}

// WithLogger sets the structured logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		if l == nil {
			return fmt.Errorf("rebound: WithLogger: nil logger")
		}
		c.logger = l
		return nil
	}
}

// WithClock sets the time source, for deterministic tests (default: the wall clock). Give the
// store the same clock.
func WithClock(clk Clock) Option {
	return func(c *config) error {
		if clk == nil {
			return fmt.Errorf("rebound: WithClock: nil clock")
		}
		c.clock = clk
		return nil
	}
}

// WithIDGenerator replaces the stream id generator (default: 16 random bytes from crypto/rand,
// base64url, 22 characters). Generated ids must be 1–128 characters from [A-Za-z0-9._~-].
func WithIDGenerator(gen func() string) Option {
	return func(c *config) error {
		if gen == nil {
			return fmt.Errorf("rebound: WithIDGenerator: nil generator")
		}
		c.idGen = func() (string, error) { return gen(), nil }
		return nil
	}
}

// WithMaxDuration caps how long a producer may run (default 10 minutes); past it the stream ends
// with error "timeout".
func WithMaxDuration(d time.Duration) Option {
	return func(c *config) error {
		c.maxDuration = d
		return positive("WithMaxDuration", d)
	}
}

// WithHeartbeatInterval sets how often a running producer records liveness and checks for
// cancellation from other instances (default 2 s).
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *config) error {
		c.heartbeat = d
		return positive("WithHeartbeatInterval", d)
	}
}

// WithStaleAfter sets how long readers must observe a running stream without append or heartbeat
// before they end it with error "producer_lost" (default 15 s). It must exceed the heartbeat
// interval. A Writer retries transient store errors for at most StaleAfter/2 (ADR 0005).
func WithStaleAfter(d time.Duration) Option {
	return func(c *config) error {
		c.staleAfter = d
		return positive("WithStaleAfter", d)
	}
}

// WithRetention sets how long a finished stream stays readable (default 10 minutes).
func WithRetention(d time.Duration) Option {
	return func(c *config) error {
		c.retention = d
		return positive("WithRetention", d)
	}
}

// WithIdleTTL sets the TTL refreshed on every append and heartbeat while a stream runs (default
// MaxDuration + StaleAfter + 1 minute). It must be at least StaleAfter + HeartbeatInterval.
func WithIdleTTL(d time.Duration) Option {
	return func(c *config) error {
		c.idleTTL = d
		return positive("WithIdleTTL", d)
	}
}

// WithMaxEvents sets the hard cap on events per stream (default 10 000). A producer that writes
// more gets ErrTooManyEvents (with ErrClosed) and the stream ends with error
// "max_events_exceeded"; running streams are never trimmed (ADR 0004).
func WithMaxEvents(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("rebound: WithMaxEvents must be positive, got %d", n)
		}
		c.maxEvents = n
		return nil
	}
}

// WithExposeProducerErrors includes the producer's error text in the terminal "error" payload
// sent to clients (default false: only the code "producer_failed"). The text is always logged.
func WithExposeProducerErrors(expose bool) Option {
	return func(c *config) error {
		c.exposeErrors = expose
		return nil
	}
}

// WithHooks installs callbacks for metrics.
func WithHooks(h Hooks) Option {
	return func(c *config) error {
		c.hooks = h
		return nil
	}
}

// StartOption configures one stream.
type StartOption func(*startConfig) error

type startConfig struct {
	meta        map[string]string
	id          string
	maxDuration time.Duration
}

// WithMeta attaches application metadata to the stream. It is stored, returned in Status and
// passed to the HTTP Authorize hook (for example a user id). Do not put secrets in it.
func WithMeta(meta map[string]string) StartOption {
	return func(sc *startConfig) error {
		sc.meta = maps.Clone(meta)
		return nil
	}
}

// WithStreamID uses id instead of a generated one; Start returns ErrExists if it is taken. The id
// must be 1–128 characters from [A-Za-z0-9._~-].
func WithStreamID(id string) StartOption {
	return func(sc *startConfig) error {
		if err := validateStreamID(id); err != nil {
			return err
		}
		sc.id = id
		return nil
	}
}

// WithStreamMaxDuration overrides the Hub's MaxDuration for this stream.
func WithStreamMaxDuration(d time.Duration) StartOption {
	return func(sc *startConfig) error {
		sc.maxDuration = d
		return positive("WithStreamMaxDuration", d)
	}
}
