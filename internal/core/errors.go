package core

import (
	"errors"

	"github.com/bailalayoub/rebound/store"
)

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
	ErrInvalidCursor = errors.New("rebound: invalid cursor")
	// ErrInvalidKind reports an event kind that is reserved or does not match
	// ^[a-z][a-z0-9_]{0,31}$.
	ErrInvalidKind = errors.New("rebound: invalid event kind")
	// ErrTooLarge reports event data larger than the maximum event size (64 KiB by default).
	ErrTooLarge = errors.New("rebound: event too large")
	// ErrHubClosed reports a call on a Hub after Close.
	ErrHubClosed = errors.New("rebound: hub closed")
)
