package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeServer speaks the load contract (POST /loadtest/streams, GET /streams/{id}) and breaks
// one guarantee on purpose, so that the tests can show the checker reports it. A checker that
// cannot fail proves nothing.
type fakeServer struct {
	mode   string
	events int
	base   int64 // event seq is stamped base + seq ms, the same on every read

	mu      sync.Mutex
	streams int
	gets    map[string]int // GETs per stream
}

func newFakeServer(t *testing.T, mode string, events int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(&fakeServer{mode: mode, events: events, base: time.Now().UnixNano(), gets: map[string]int{}})
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/loadtest/streams":
		f.mu.Lock()
		f.streams++
		id := "s" + strconv.Itoa(f.streams)
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"stream_id":%q,"url":"/streams/%s"}`, id, id)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/streams/"):
		f.serveStream(w, r, strings.TrimPrefix(r.URL.Path, "/streams/"))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeServer) serveStream(w http.ResponseWriter, r *http.Request, id string) {
	cursor, _ := strconv.ParseUint(r.Header.Get("Last-Event-ID"), 10, 64)
	f.mu.Lock()
	f.gets[id]++
	nth := f.gets[id]
	f.mu.Unlock()
	last := uint64(f.events) //nolint:gosec // small test value
	if f.mode == "missing" && cursor == last {
		w.WriteHeader(http.StatusNoContent) // a server that claims the reader already has the terminal
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Upstream", "fake:"+strconv.Itoa(nth%2))
	from := cursor + 1
	if f.mode == "dup" && cursor > 0 {
		from = cursor // replays the last event the reader already has
	}
	for seq := from; seq <= last; seq++ {
		if f.mode == "gap" && seq == 3 {
			continue
		}
		p := Payload{I: seq, T: f.base + int64(seq)*int64(time.Millisecond), H: "app-1", N: "n1"} //nolint:gosec // small seq
		switch {
		case f.mode == "moved" && seq > last/2:
			p.H = "app-2"
		case f.mode == "restarted" && seq > last/2:
			p.N = "n2"
		case f.mode == "badpayload" && seq == 2:
			p.I = 7
		case f.mode == "notprefix" && nth == 2 && seq == 2:
			p.T = 1 // the audit read sees other bytes than the reader got
		}
		data, _ := json.Marshal(p)
		_, _ = fmt.Fprintf(w, "id: %d\nevent: data\ndata: %s\n\n", seq, data)
	}
	switch {
	case f.mode == "missing":
		return // ends the body without the terminal event
	case f.mode == "differs" && nth%2 == 0:
		_, _ = fmt.Fprintf(w, "id: %d\nevent: error\ndata: {\"code\":\"cancelled\"}\n\n", last+1)
	default:
		_, _ = fmt.Fprintf(w, "id: %d\nevent: done\ndata: {}\n\n", last+1)
	}
}

// runAgainst runs one short phase against url with readers readers per stream.
func runAgainst(t *testing.T, url string, readers int, drop DropPolicy) *Report {
	t.Helper()
	cfg := DefaultConfig()
	cfg.BaseURL = url
	cfg.Phases = []Phase{{Spec: "test", Streams: 1, ReadersMin: readers, ReadersMax: readers, Hold: 20 * time.Millisecond}}
	cfg.Stream = StartRequest{Events: 6, DelayMS: 0}
	cfg.Drop = drop
	cfg.AuditRate = 1
	cfg.Window = 10 * time.Millisecond
	cfg.Slack = 5 * time.Second
	cfg.Seed = 7
	rep, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	require.Len(t, rep.Phases, 1)
	require.Positive(t, rep.Phases[0].StreamsStarted)
	return rep
}

func TestChecker_ReportsEachInjectedViolation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		mode    string
		readers int
		drop    DropPolicy
		want    []string // integrity classes that must be reported
	}{
		{mode: "gap", readers: 1, want: []string{ClassGap, ClassIncomplete}},
		{mode: "dup", readers: 1, drop: DropPolicy{Kind: "every", Min: 2, Max: 2}, want: []string{ClassDuplicate}},
		{mode: "missing", readers: 1, want: []string{ClassMissing}},
		{mode: "differs", readers: 2, want: []string{ClassTerminalDiffer}},
		{mode: "moved", readers: 1, want: []string{ClassMoved}},
		{mode: "restarted", readers: 1, want: []string{ClassRestarted}},
		{mode: "badpayload", readers: 1, want: []string{ClassBadPayload}},
		{mode: "notprefix", readers: 1, want: []string{ClassNotPrefix}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			if tc.drop.Kind == "" {
				tc.drop.Kind = "none"
			}
			rep := runAgainst(t, newFakeServer(t, tc.mode, 6).URL, tc.readers, tc.drop)
			p := rep.Phases[0]
			for _, class := range tc.want {
				assert.Positive(t, p.Integrity[class], "%s not reported: %v", class, p.Integrity)
			}
			assert.False(t, p.Passed)
			assert.False(t, rep.Passed)
			require.NotEmpty(t, p.Failures)
			assert.NotEmpty(t, p.Failures[0].Problems)
		})
	}
}

// The same checker on a server that keeps every guarantee reports nothing.
func TestChecker_CleanServerPasses(t *testing.T) {
	t.Parallel()
	rep := runAgainst(t, newFakeServer(t, "good", 6).URL, 3, DropPolicy{Kind: "every", Min: 1, Max: 3})
	p := rep.Phases[0]
	assert.Empty(t, p.Integrity)
	assert.Equal(t, p.StreamsStarted, p.Terminals["done"])
	assert.Equal(t, p.StreamsStarted, p.Audited)
	assert.True(t, p.Passed)
	assert.True(t, rep.Passed)
	assert.Positive(t, p.Ended[EndDropped], "the drop policy was applied")
	assert.NotEmpty(t, p.Served["app-1"], "the X-Upstream of every connection is counted")
}
