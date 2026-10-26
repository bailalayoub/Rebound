package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/store/memory"
)

func TestDecodeStartRequest(t *testing.T) {
	t.Parallel()
	req, err := DecodeStartRequest(strings.NewReader(`{"text":"hello"}`))
	require.NoError(t, err)
	assert.Equal(t, StartRequest{Text: "hello", Chunk: 4, DelayMS: 10, FailAfter: -1}, req, "defaults of docs/06")

	req, err = DecodeStartRequest(strings.NewReader(`{"text":"x","chunk":1,"delay_ms":0,"fail_after":0,"kinds":true}`))
	require.NoError(t, err)
	assert.Equal(t, StartRequest{Text: "x", Chunk: 1, DelayMS: 0, FailAfter: 0, Kinds: true}, req)

	for _, body := range []string{
		``, `not json`, `{"text":"x","extra":1}`, `{}`, `{"text":"x","chunk":0}`,
		`{"text":"x","delay_ms":-1}`, `{"text":"x","delay_ms":10001}`, `{"text":"x","fail_after":-2}`,
	} {
		_, err := DecodeStartRequest(strings.NewReader(body))
		require.Error(t, err, "body %q", body)
	}
	_, err = DecodeStartRequest(strings.NewReader(`{"text":"` + strings.Repeat("a", maxTextRunes+1) + `"}`))
	require.Error(t, err)
}

func TestChunks_RuneAware(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"abc", "de"}, Chunks("abcde", 3))
	assert.Equal(t, []string{"é€", "😀\n", "x"}, Chunks("é€😀\nx", 2))
	assert.Equal(t, []string{"a"}, Chunks("a", 10))
}

func TestExpectedEvents(t *testing.T) {
	t.Parallel()
	plain := ExpectedEvents(StartRequest{Text: "abcdefg", Chunk: 2, FailAfter: -1})
	assert.Equal(t, "done", plain.Terminal)
	assert.Equal(t, "abcdefg", plain.Text())
	require.Len(t, plain.Events, 4)
	assert.Equal(t, uint64(4), plain.Events[3].Seq)

	kinds := ExpectedEvents(StartRequest{Text: strings.Repeat("x", 25), Chunk: 1, FailAfter: -1, Kinds: true})
	assert.Equal(t, strings.Repeat("x", 25), kinds.Text())
	for i, ev := range kinds.Events {
		seq := uint64(i + 1)
		require.Equal(t, seq, ev.Seq)
		if seq%10 == 0 {
			assert.Equal(t, "meta", ev.Kind)
			assert.JSONEq(t, fmt.Sprintf(`{"i":%d}`, seq), string(ev.Data))
		} else {
			assert.Equal(t, "data", ev.Kind)
		}
	}
	assert.Len(t, kinds.Events, 27, "25 data + metas at 10 and 20")

	failing := ExpectedEvents(StartRequest{Text: "abcdef", Chunk: 1, FailAfter: 3})
	assert.Equal(t, "producer_failed", failing.Terminal)
	assert.Equal(t, "abc", failing.Text())

	atEnd := ExpectedEvents(StartRequest{Text: "ab", Chunk: 1, FailAfter: 2})
	assert.Equal(t, "producer_failed", atEnd.Terminal)
	assert.Equal(t, "ab", atEnd.Text())

	beyond := ExpectedEvents(StartRequest{Text: "ab", Chunk: 1, FailAfter: 5})
	assert.Equal(t, "done", beyond.Terminal)

	immediate := ExpectedEvents(StartRequest{Text: "ab", Chunk: 1, FailAfter: 0})
	assert.Empty(t, immediate.Events)
	assert.Equal(t, "producer_failed", immediate.Terminal)
}

// The producer the server runs writes exactly ExpectedEvents, then the expected terminal.
func TestProducerMatchesExpected(t *testing.T) {
	t.Parallel()
	hub, err := rebound.New(memory.New(), rebound.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })

	for name, req := range map[string]StartRequest{
		"plain":   {Text: "Lorem ipsum dolor sit amet, é€😀\nconsectetur", Chunk: 3, DelayMS: 1, FailAfter: -1},
		"kinds":   {Text: strings.Repeat("ab", 30), Chunk: 2, FailAfter: -1, Kinds: true},
		"failing": {Text: "abcdefghij", Chunk: 1, FailAfter: 4, Kinds: true},
	} {
		st, err := hub.Start(context.Background(), req.Producer())
		require.NoError(t, err, name)
		sub, err := hub.Subscribe(context.Background(), st.ID, 0)
		require.NoError(t, err, name)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var got []rebound.Event
		for {
			ev, err := sub.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, name)
			got = append(got, ev)
		}
		cancel()
		require.NoError(t, sub.Close())

		exp := ExpectedEvents(req)
		require.Len(t, got, len(exp.Events)+1, name)
		assert.Equal(t, exp.Events, got[:len(exp.Events)], name)
		terminal := got[len(got)-1]
		if exp.Terminal == "done" {
			assert.Equal(t, "done", terminal.Kind, name)
		} else {
			assert.Equal(t, "error", terminal.Kind, name)
			assert.Contains(t, string(terminal.Data), `"code":"producer_failed"`, name)
		}
	}
}

func TestPauseHonoursContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, pause(ctx, time.Hour), context.Canceled)
	require.NoError(t, pause(context.Background(), time.Millisecond))
}
