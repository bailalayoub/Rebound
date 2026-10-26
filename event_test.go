package rebound

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateKind_Accepts(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"default kind":          "data",
		"length 1":              "a",
		"length 32":             "a" + strings.Repeat("b", 31),
		"custom word":           "meta",
		"underscore":            "tool_call",
		"digits after first":    "v2",
		"trailing underscore":   "x_",
		"all allowed classes":   "a0_9z",
		"reserved as substring": "done_soon",
	}
	for name, kind := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, validateKind(kind), "kind %q", kind)
		})
	}
}

func TestValidateKind_Rejects(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"reserved done":         "done",
		"reserved error":        "error",
		"reserved open":         "open",
		"reserved ping":         "ping",
		"empty":                 "",
		"length 33":             "a" + strings.Repeat("b", 32),
		"uppercase first":       "Meta",
		"uppercase single":      "A",
		"uppercase later":       "aB",
		"uppercase reserved":    "DONE",
		"digit first":           "1abc",
		"underscore first":      "_abc",
		"hyphen":                "tool-call",
		"space":                 "tool call",
		"non-ascii":             "é",
		"trailing newline":      "data\n",
		"char before a (`)":     "`",
		"char after z ({)":      "a{",
		"char before 0 (/)":     "a/",
		"char after 9 (:)":      "a:",
		"uppercase like data":   "Data",
		"reserved with spacing": " done",
	}
	for name, kind := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := validateKind(kind)
			require.ErrorIs(t, err, ErrInvalidKind, "kind %q", kind)
			assert.True(t, strings.HasPrefix(err.Error(), "rebound: "), "error message %q", err)
		})
	}
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()
	assert.True(t, IsTerminal("done"))
	assert.True(t, IsTerminal("error"))
	assert.False(t, IsTerminal("data"))
	assert.False(t, IsTerminal("meta"))
	assert.False(t, IsTerminal(""))
}

func TestStateAliases(t *testing.T) {
	t.Parallel()
	assert.Equal(t, State("running"), StateRunning)
	assert.Equal(t, State("done"), StateDone)
	assert.Equal(t, State("error"), StateError)
	assert.True(t, StateDone.IsTerminal())
	assert.False(t, StateRunning.IsTerminal())
}
