package core

import (
	"fmt"

	"github.com/bailalayoub/rebound/store"
)

// Event is one entry of a stream: a dense sequence number, a kind and an opaque payload.
type Event = store.Event

// State is the lifecycle state of a stream: running, done or error.
type State = store.State

// Stream states.
const (
	StateRunning = store.StateRunning
	StateDone    = store.StateDone
	StateError   = store.StateError
)

// Kinds that producers may not use: the terminal kinds are written by the Hub only, and "open"
// and "ping" are kept free for transport-level signals.
const (
	kindOpen = "open"
	kindPing = "ping"
)

// maxKindLen is the longest custom kind accepted (spec §1: ^[a-z][a-z0-9_]{0,31}$).
const maxKindLen = 32

// IsTerminal reports whether kind ends a stream ("done" or "error").
func IsTerminal(kind string) bool {
	return store.IsTerminal(kind)
}

// validateKind accepts "data" and custom kinds matching ^[a-z][a-z0-9_]{0,31}$, and rejects the
// reserved kinds done, error, open and ping.
func validateKind(kind string) error {
	switch kind {
	case store.KindData:
		return nil
	case store.KindDone, store.KindError, kindOpen, kindPing:
		return fmt.Errorf("%w: %q is reserved", ErrInvalidKind, kind)
	}
	if !isKindSyntax(kind) {
		return fmt.Errorf("%w: %q does not match ^[a-z][a-z0-9_]{0,31}$", ErrInvalidKind, kind)
	}
	return nil
}

// isKindSyntax implements ^[a-z][a-z0-9_]{0,31}$ without a package-level regexp.
func isKindSyntax(kind string) bool {
	if kind == "" || len(kind) > maxKindLen || !isLower(kind[0]) {
		return false
	}
	for i := 1; i < len(kind); i++ {
		c := kind[i]
		if !isLower(c) && !isDigit(c) && c != '_' {
			return false
		}
	}
	return true
}

func isLower(c byte) bool { return c >= 'a' && c <= 'z' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
