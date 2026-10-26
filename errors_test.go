package rebound

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bailalayoub/rebound/store"
)

func allSentinels() map[string]error {
	return map[string]error{
		"ErrNotFound":      ErrNotFound,
		"ErrExists":        ErrExists,
		"ErrClosed":        ErrClosed,
		"ErrCancelled":     ErrCancelled,
		"ErrGap":           ErrGap,
		"ErrTooManyEvents": ErrTooManyEvents,
		"ErrConflict":      ErrConflict,
		"ErrInvalidCursor": ErrInvalidCursor,
		"ErrInvalidKind":   ErrInvalidKind,
		"ErrTooLarge":      ErrTooLarge,
		"ErrHubClosed":     ErrHubClosed,
	}
}

func TestSentinels_DistinctAndPrefixed(t *testing.T) {
	t.Parallel()
	all := allSentinels()
	for name, err := range all {
		assert.True(t, strings.HasPrefix(err.Error(), "rebound: "), "%s message %q", name, err)
		for other, otherErr := range all {
			if name != other {
				assert.NotErrorIs(t, err, otherErr, "%s must not match %s", name, other)
			}
		}
	}
}

func TestSentinels_StoreErrorsAreTheSameValues(t *testing.T) {
	t.Parallel()
	// A store wraps its own sentinel; callers match it with the rebound name (ADR 0002).
	pairs := map[error]error{
		store.ErrNotFound:      ErrNotFound,
		store.ErrExists:        ErrExists,
		store.ErrClosed:        ErrClosed,
		store.ErrCancelled:     ErrCancelled,
		store.ErrGap:           ErrGap,
		store.ErrTooManyEvents: ErrTooManyEvents,
		store.ErrConflict:      ErrConflict,
	}
	for storeErr, reboundErr := range pairs {
		wrapped := fmt.Errorf("memory store: append abc: %w", storeErr)
		assert.ErrorIs(t, wrapped, reboundErr)
		assert.True(t, errors.Is(reboundErr, storeErr))
	}
}
