package store

import (
	"context"
	"time"
)

// Store is the ordered log and metadata backend of the Hub (docs/02-spec.md §6). Every method is
// safe for concurrent use, from any number of Hubs sharing the backend.
type Store interface {
	// Create registers a new, running, empty stream. It returns ErrExists if id is taken.
	Create(ctx context.Context, id string, m Meta) error

	// Append atomically assigns seq = LastSeq+1 to the event and stores it, updates LastActivity
	// to ev.At and refreshes the idle TTL. A terminal kind ("done", "error") flips the state and
	// switches the TTL to Retention.
	//
	// It returns ErrNotFound for unknown ids, ErrClosed once the stream holds its terminal event
	// (so exactly one terminal append succeeds), ErrCancelled for a cancelled stream unless
	// ev.Force is set, ErrTooManyEvents for a non-terminal event beyond MaxEvents (a hard cap: a
	// running stream is never trimmed), and ErrConflict when ev.Seq does not match the log. No
	// method of a Store returns an error matching io.EOF.
	Append(ctx context.Context, id string, ev AppendRequest) (seq uint64, err error)

	// Read returns, in order, at most limit events with Seq > afterSeq (limit <= 0: no limit),
	// read from the log itself. When none is available it blocks up to block, then returns
	// (nil, nil). It returns ErrGap if afterSeq+1 < FirstSeq, ErrNotFound for unknown ids, and
	// returns promptly with the context's error when ctx is done.
	Read(ctx context.Context, id string, afterSeq uint64, limit int, block time.Duration) ([]Event, error)

	// Status returns a snapshot consistent with the last completed Append.
	Status(ctx context.Context, id string) (Status, error)

	// Cancel sets the cancelled flag. It is idempotent and succeeds on terminal streams.
	Cancel(ctx context.Context, id string) error

	// Touch records producer liveness: LastActivity = at, and the idle TTL is refreshed while the
	// stream is running.
	Touch(ctx context.Context, id string, at time.Time) error

	// Close releases the store's resources. Blocked reads return promptly.
	Close() error
}

// Meta is the per-stream configuration fixed at Create.
type Meta struct {
	CreatedAt time.Time
	// IdleTTL is how long a running stream survives without an append or touch.
	IdleTTL time.Duration
	// Retention is how long a terminal stream stays readable.
	Retention time.Duration
	// MaxEvents caps the number of non-terminal events (ADR 0004): appending more fails with
	// ErrTooManyEvents. 0 means no cap.
	MaxEvents int
	// Attrs is application metadata returned in Status.Meta.
	Attrs map[string]string
}

// AppendRequest is one event to append.
type AppendRequest struct {
	Kind string
	Data []byte
	// At is the producer's time of the write; it becomes LastActivity.
	At time.Time
	// Force allows the append on a cancelled stream. The Hub uses it for terminal events only.
	Force bool
	// Seq is the sequence number the writer expects the event to get; 0 lets the store assign
	// the next one. With Seq > 0 the append is idempotent (ADR 0005): if event Seq is already
	// stored with the same kind and data, Append returns Seq and changes nothing; if Seq does not
	// match the log otherwise, Append returns ErrClosed on a terminal stream, else ErrConflict.
	Seq uint64
}
