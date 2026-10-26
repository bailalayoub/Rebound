package sse_test

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/sse"
	"github.com/bailalayoub/rebound/store"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata/")

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		require.NoError(t, os.WriteFile(path, got, 0o600))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "missing golden file; run go test ./sse -update")
	assert.Equal(t, string(want), string(got))
}

func TestWriteEvent_Golden(t *testing.T) {
	t.Parallel()
	cases := map[string]store.Event{
		"event_data":      {Seq: 42, Kind: "data", Data: []byte(`{"text":"Bon"}`)},
		"event_custom":    {Seq: 1, Kind: "meta", Data: []byte(`{"instance":"app-2"}`)},
		"event_multiline": {Seq: 7, Kind: "data", Data: []byte("first\nsecond\r\nthird\rfourth")},
		"event_blanks":    {Seq: 8, Kind: "data", Data: []byte("\nmiddle\n\n")},
		"event_empty":     {Seq: 9, Kind: "data", Data: nil},
		"terminal_done":   {Seq: 57, Kind: "done", Data: []byte("{}")},
		"terminal_error":  {Seq: 57, Kind: "error", Data: []byte(`{"code":"cancelled","message":"cancelled by client"}`)},
		"event_max_seq":   {Seq: 18446744073709551615, Kind: "data", Data: []byte("x")},
	}
	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			require.NoError(t, sse.WriteEvent(&b, ev))
			golden(t, name, b.Bytes())
		})
	}
}

func TestWriteCommentAndRetry_Golden(t *testing.T) {
	t.Parallel()
	var b bytes.Buffer
	require.NoError(t, sse.WriteRetry(&b, time.Second))
	require.NoError(t, sse.WriteComment(&b, "ping"))
	require.NoError(t, sse.WriteRetry(&b, -time.Second))
	require.NoError(t, sse.WriteRetry(&b, 1500*time.Microsecond))
	golden(t, "comment_retry", b.Bytes())
}

func TestWrite_RejectsLineBreaksInFields(t *testing.T) {
	t.Parallel()
	var b bytes.Buffer
	for _, kind := range []string{"da\nta", "data\r", "\r\n"} {
		err := sse.WriteEvent(&b, store.Event{Seq: 1, Kind: kind})
		require.ErrorIs(t, err, sse.ErrInvalidField, "kind %q", kind)
	}
	require.ErrorIs(t, sse.WriteComment(&b, "ping\n\nid: 999"), sse.ErrInvalidField)
	assert.Zero(t, b.Len(), "nothing is written for a rejected field")
}

type failingWriter struct{}

var errBrokenPipe = errors.New("broken pipe")

func (failingWriter) Write([]byte) (int, error) { return 0, errBrokenPipe }

func TestWrite_PropagatesWriterErrors(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, sse.WriteEvent(failingWriter{}, store.Event{Seq: 1, Kind: "data"}), errBrokenPipe)
	require.ErrorIs(t, sse.WriteComment(failingWriter{}, "ping"), errBrokenPipe)
	require.ErrorIs(t, sse.WriteRetry(failingWriter{}, time.Second), errBrokenPipe)
}

// countingWriter records how many Write calls an event needs.
type countingWriter struct {
	bytes.Buffer
	calls int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.calls++
	return c.Buffer.Write(p)
}

func TestWriteEvent_SingleWrite(t *testing.T) {
	t.Parallel()
	var w countingWriter
	require.NoError(t, sse.WriteEvent(&w, store.Event{Seq: 3, Kind: "data", Data: []byte("a\nb\nc")}))
	assert.Equal(t, 1, w.calls)
}

func TestParseLastEventID(t *testing.T) {
	t.Parallel()
	valid := []struct {
		header, query string
		want          uint64
	}{
		{"", "", 0},
		{"0", "", 0},
		{"42", "", 42},
		{"", "17", 17},
		{"5", "99", 5}, // header wins
		{"007", "", 7},
		{"18446744073709551615", "", 18446744073709551615},
		{"00000000000000000001", "", 1}, // 20 characters
	}
	for _, c := range valid {
		got, err := sse.ParseLastEventID(c.header, c.query)
		require.NoError(t, err, "header %q query %q", c.header, c.query)
		assert.Equal(t, c.want, got, "header %q query %q", c.header, c.query)
	}

	invalid := []struct{ header, query string }{
		{"abc", ""},
		{"-1", ""},
		{"+1", ""},
		{" 1", ""},
		{"1 ", ""},
		{"1.0", ""},
		{"1e3", ""},
		{"0x10", ""},
		{"١", ""},                     // ARABIC-INDIC DIGIT ONE, not ASCII
		{"18446744073709551616", ""},  // uint64 overflow
		{"000000000000000000001", ""}, // 21 characters
		{strings.Repeat("9", 4096), ""},
		{"", "abc"},
		{"bad", "5"}, // header wins, even when invalid
	}
	for _, c := range invalid {
		_, err := sse.ParseLastEventID(c.header, c.query)
		require.ErrorIs(t, err, sse.ErrInvalidLastEventID, "header %q query %q", c.header, c.query)
	}
}
