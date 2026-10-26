package store

import "errors"

// Sentinel errors returned by every Store implementation and re-exported by package rebound.
// Match them with errors.Is; implementations may wrap them with context.
var (
	// ErrNotFound reports an unknown or expired stream id.
	ErrNotFound = errors.New("rebound: stream not found")
	// ErrExists reports that a stream with the same id already exists.
	ErrExists = errors.New("rebound: stream already exists")
	// ErrClosed reports a write to a stream that already holds its terminal event.
	ErrClosed = errors.New("rebound: stream closed")
	// ErrCancelled reports a non-forced append to a cancelled stream.
	ErrCancelled = errors.New("rebound: stream cancelled")
	// ErrGap reports a cursor older than the oldest retained event: a store trimmed the events
	// right after it (only allowed on terminal streams, ADR 0004). It is final for that reader.
	ErrGap = errors.New("rebound: events after cursor were trimmed")
	// ErrTooManyEvents reports a non-terminal append on a stream that already holds MaxEvents
	// events (ADR 0004).
	ErrTooManyEvents = errors.New("rebound: stream reached MaxEvents")
	// ErrConflict reports an append whose expected Seq does not match the log (ADR 0005).
	ErrConflict = errors.New("rebound: append conflicts with the stored sequence")
)
