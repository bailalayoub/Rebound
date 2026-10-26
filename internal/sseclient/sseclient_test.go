package sseclient_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/sseclient"
	"github.com/bailalayoub/rebound/sse"
	"github.com/bailalayoub/rebound/store"
)

func readAll(t *testing.T, input string) []sseclient.Frame {
	t.Helper()
	rd := sseclient.NewReader(strings.NewReader(input))
	var frames []sseclient.Frame
	for {
		f, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return frames
		}
		require.NoError(t, err)
		frames = append(frames, f)
	}
}

func TestReader_Fields(t *testing.T) {
	t.Parallel()
	frames := readAll(t, "retry: 1000\n\n"+
		": ping\n\n"+
		"id: 42\nevent: data\ndata: {\"text\":\"Bon\"}\n\n"+
		"id: 43\ndata: no event field\n\n")
	require.Len(t, frames, 4)

	assert.True(t, frames[0].HasRetry)
	assert.Equal(t, int64(1000), frames[0].Retry)
	assert.False(t, frames[0].IsEvent())

	assert.Equal(t, []string{"ping"}, frames[1].Comments)
	assert.False(t, frames[1].IsEvent())
	assert.False(t, frames[1].HasID)

	assert.Equal(t, sseclient.Frame{ID: "42", HasID: true, Event: "data", Data: `{"text":"Bon"}`, HasData: true}, frames[2])
	assert.Equal(t, "", frames[3].Event)
	assert.Equal(t, "no event field", frames[3].Data)
}

func TestReader_LineEndingsAndMultilineData(t *testing.T) {
	t.Parallel()
	for name, input := range map[string]string{
		"LF":    "data: a\ndata:\ndata: b\n\n",
		"CRLF":  "data: a\r\ndata:\r\ndata: b\r\n\r\n",
		"CR":    "data: a\rdata:\rdata: b\r\r",
		"mixed": "data: a\r\ndata:\rdata: b\n\r\n",
	} {
		frames := readAll(t, input)
		require.Len(t, frames, 1, name)
		assert.Equal(t, "a\n\nb", frames[0].Data, name)
	}
}

func TestReader_EdgeCases(t *testing.T) {
	t.Parallel()
	frames := readAll(t, "\uFEFFdata:no space\n\n"+
		"data:  two spaces\n\n"+
		"data\n\n"+ // field without colon: empty value
		"id: a\x00b\ndata: x\n\n"+ // id with NUL is ignored
		"retry: 1x\nretry: -5\ndata: y\n\n"+ // invalid retry values are ignored
		"unknown: field\ndata: z\n\n"+
		"\n\n\n"+ // extra blank lines dispatch nothing
		"data: cut off at EOF")
	require.Len(t, frames, 6)
	assert.Equal(t, "no space", frames[0].Data)
	assert.Equal(t, " two spaces", frames[1].Data)
	assert.True(t, frames[2].HasData)
	assert.Equal(t, "", frames[2].Data)
	assert.False(t, frames[3].HasID)
	assert.False(t, frames[4].HasRetry)
	assert.Equal(t, "z", frames[5].Data)
}

type failingReader struct{}

var errReset = errors.New("connection reset")

func (failingReader) Read([]byte) (int, error) { return 0, errReset }

func TestReader_PropagatesReadErrors(t *testing.T) {
	t.Parallel()
	_, err := sseclient.NewReader(failingReader{}).Next()
	require.ErrorIs(t, err, errReset)

	_, err = sseclient.NewReader(io.MultiReader(strings.NewReader("data: x\r"), failingReader{})).Next()
	require.ErrorIs(t, err, errReset)
}

// Round trip with the server encoder: what sse.WriteEvent writes, the parser gives back.
func TestReader_RoundTripWithEncoder(t *testing.T) {
	t.Parallel()
	events := []store.Event{
		{Seq: 1, Kind: "data", Data: []byte(`{"text":"x"}`)},
		{Seq: 2, Kind: "meta", Data: []byte("line1\nline2\n")},
		{Seq: 3, Kind: "data", Data: nil},
		{Seq: 4, Kind: "done", Data: []byte("{}")},
	}
	var b bytes.Buffer
	for _, ev := range events {
		require.NoError(t, sse.WriteEvent(&b, ev))
		require.NoError(t, sse.WriteComment(&b, "ping"))
	}
	frames := readAll(t, b.String())
	var got []sseclient.Frame
	for _, f := range frames {
		if f.IsEvent() {
			got = append(got, f)
		}
	}
	require.Len(t, got, len(events))
	for i, ev := range events {
		assert.Equal(t, ev.Kind, got[i].Event)
		assert.Equal(t, string(ev.Data), got[i].Data)
		assert.True(t, got[i].HasID)
	}
}
