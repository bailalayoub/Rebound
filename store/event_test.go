package store_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bailalayoub/rebound/store"
)

func TestIsTerminal(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"done":  true,
		"error": true,
		"data":  false,
		"meta":  false,
		"":      false,
		"Done":  false,
		"ping":  false,
	}
	for kind, want := range cases {
		assert.Equal(t, want, store.IsTerminal(kind), "IsTerminal(%q)", kind)
	}
}

func TestStateIsTerminal(t *testing.T) {
	t.Parallel()
	assert.False(t, store.StateRunning.IsTerminal())
	assert.True(t, store.StateDone.IsTerminal())
	assert.True(t, store.StateError.IsTerminal())
	assert.False(t, store.State("").IsTerminal())
}
