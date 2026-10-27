package loadtest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/store/memory"
)

func TestHistogram_ExactBelow64AndBoundedAbove(t *testing.T) {
	t.Parallel()
	h := NewHistogram()
	for v := range 64 {
		h.Record(time.Duration(v))
	}
	assert.Equal(t, time.Duration(31), h.Quantile(0.5))
	assert.Equal(t, time.Duration(63), h.Quantile(1))

	rng := mrand.New(mrand.NewPCG(1, 2))
	h = NewHistogram()
	values := make([]int64, 20_000)
	for i := range values {
		values[i] = rng.Int64N(int64(5 * time.Second))
		h.Record(time.Duration(values[i]))
	}
	slices.Sort(values)
	for _, q := range []float64{0.5, 0.95, 0.99, 0.999} {
		exact := values[int(math.Ceil(q*float64(len(values))))-1]
		got := int64(h.Quantile(q))
		assert.GreaterOrEqual(t, got, exact, "q=%v", q)
		assert.LessOrEqual(t, float64(got-exact), float64(exact)/64+1, "q=%v: within 1/64", q)
	}
	assert.Equal(t, time.Duration(values[len(values)-1]), h.Max())
}

func TestHistogram_BucketsCoverEveryValue(t *testing.T) {
	t.Parallel()
	for _, v := range []int64{0, 1, 63, 64, 127, 128, 129, 1 << 20, 1<<40 + 12345, math.MaxInt64} {
		i := bucketOf(v)
		require.Less(t, i, numBuckets)
		assert.GreaterOrEqual(t, bucketUpper(i), v, "v=%d", v)
		if i > 0 {
			assert.Less(t, bucketUpper(i-1), v, "v=%d is above the previous bucket", v)
		}
	}
}

func TestHistogram_MergeAndConcurrentRecord(t *testing.T) {
	t.Parallel()
	a, b := NewHistogram(), NewHistogram()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				a.Record(time.Duration(g*1000 + i))
			}
		})
	}
	wg.Wait()
	b.Record(-5) // clamped to zero
	b.Record(time.Hour)
	a.Merge(b)
	assert.Equal(t, uint64(8002), a.Count())
	assert.Equal(t, time.Hour, a.Max())
	assert.Equal(t, time.Duration(0), a.Quantile(0.0001))
	assert.Equal(t, Summary{}, NewHistogram().Summarize())
}

func ev(seq uint64, kind, data string) rebound.Event {
	return rebound.Event{Seq: seq, Kind: kind, Data: []byte(data)}
}

func payload(seq uint64, host, nonce string) string {
	b, _ := Payload{I: seq, T: 1, H: host, N: nonce}.Encode(0)
	return string(b)
}

func TestTally(t *testing.T) {
	t.Parallel()
	var clean Tally
	for seq := uint64(1); seq <= 3; seq++ {
		_, ok := clean.Observe(ev(seq, "data", payload(seq, "a", "n")))
		require.True(t, ok)
	}
	clean.Observe(ev(4, "done", "{}"))
	assert.Empty(t, clean.Problems(3))
	assert.Equal(t, "done", clean.TerminalCode)
	assert.Equal(t, []string{"done after 3 events at seq 4, want 4 events"}, clean.Problems(4))

	var bad Tally
	bad.Observe(ev(1, "data", payload(1, "a", "n")))
	bad.Observe(ev(1, "data", payload(1, "a", "n")))        // duplicate
	bad.Observe(ev(3, "data", payload(3, "b", "n")))        // gap of one, and another instance
	bad.Observe(ev(4, "data", payload(4, "a", "m")))        // another producer run
	bad.Observe(ev(5, "data", payload(9, "a", "n")))        // payload of another seq
	bad.Observe(ev(6, "error", `{"code":"producer_lost"}`)) // terminal
	bad.Observe(ev(7, "done", "{}"))                        // after the terminal
	assert.Equal(t, []string{
		"1 gap", "1 duplicate", "1 event after the terminal", "1 bad payload",
		"1 event from another instance", "1 event from another producer run",
	}, bad.Problems(5))
	assert.Equal(t, "producer_lost", bad.TerminalCode)

	var none Tally
	assert.Equal(t, []string{"missing terminal"}, none.Problems(1))
}

func TestTally_IsPrefixOf(t *testing.T) {
	t.Parallel()
	var log, prefix, other Tally
	for seq := uint64(1); seq <= 4; seq++ {
		log.Observe(ev(seq, "data", payload(seq, "a", "n")))
		if seq <= 2 {
			prefix.Observe(ev(seq, "data", payload(seq, "a", "n")))
			other.Observe(ev(seq, "data", payload(seq, "a", "x")))
		}
	}
	assert.True(t, prefix.IsPrefixOf(&log))
	assert.True(t, log.IsPrefixOf(&log))
	assert.False(t, other.IsPrefixOf(&log))
	assert.False(t, log.IsPrefixOf(&prefix), "longer than the log")
	assert.True(t, (&Tally{}).IsPrefixOf(&log))
}

func TestPayload_EncodePadsToSize(t *testing.T) {
	t.Parallel()
	small, err := Payload{I: 1, T: 2, H: "h", N: "n"}.Encode(0)
	require.NoError(t, err)
	assert.JSONEq(t, `{"i":1,"t":2,"h":"h","n":"n"}`, string(small))
	padded, err := Payload{I: 1, T: 2, H: "h", N: "n"}.Encode(200)
	require.NoError(t, err)
	assert.Len(t, padded, 200)
	var p Payload
	require.NoError(t, json.Unmarshal(padded, &p))
	assert.Equal(t, uint64(1), p.I)
}

func TestDecodeStartRequest(t *testing.T) {
	t.Parallel()
	req, err := DecodeStartRequest(strings.NewReader(`{}`))
	require.NoError(t, err)
	assert.Equal(t, StartRequest{Events: 400, DelayMS: 50}, req)
	for _, bad := range []string{``, `{"events":0}`, `{"events":10001}`, `{"delay_ms":-1}`, `{"size":40000}`, `{"x":1}`} {
		_, err := DecodeStartRequest(strings.NewReader(bad))
		assert.Error(t, err, bad)
	}
}

func TestParsePhases(t *testing.T) {
	t.Parallel()
	got, err := ParsePhases("50x1-3@3m, 1250x2~10s@1m,0x1@30s")
	require.NoError(t, err)
	assert.Equal(t, []Phase{
		{Spec: "50x1-3@3m", Streams: 50, ReadersMin: 1, ReadersMax: 3, Hold: 3 * time.Minute},
		{Spec: "1250x2~10s@1m", Streams: 1250, ReadersMin: 2, ReadersMax: 2, Ramp: 10 * time.Second, Hold: time.Minute},
		{Spec: "0x1@30s", Streams: 0, ReadersMin: 1, ReadersMax: 1, Hold: 30 * time.Second},
	}, got)
	for _, bad := range []string{"", "50x1", "50@1m", "x1@1m", "50x0@1m", "50x3-1@1m", "50x1~z@1m", "50x1@0s"} {
		_, err := ParsePhases(bad)
		assert.Error(t, err, bad)
	}
}

func TestParseDropPolicy(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]DropPolicy{
		"none":      {Kind: "none"},
		"":          {Kind: "none"},
		"conn:0.3":  {Kind: "conn", P: 0.3},
		"every:1-5": {Kind: "every", Min: 1, Max: 5},
		"ms:5-50":   {Kind: "ms", Min: 5, Max: 50},
		"every:3":   {Kind: "every", Min: 3, Max: 3},
	} {
		got, err := ParseDropPolicy(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
		again, err := ParseDropPolicy(got.String())
		require.NoError(t, err)
		assert.Equal(t, want, again)
	}
	for _, bad := range []string{"conn:2", "conn:x", "every:0-3", "ms:9-2", "sometimes"} {
		_, err := ParseDropPolicy(bad)
		assert.Error(t, err, bad)
	}
	rng := mrand.New(mrand.NewPCG(3, 4))
	for range 100 {
		n, d := DropPolicy{Kind: "every", Min: 2, Max: 4}.plan(rng, 10)
		assert.True(t, n >= 2 && n <= 4 && d == 0)
		n, d = DropPolicy{Kind: "ms", Min: 5, Max: 50}.plan(rng, 10)
		assert.True(t, n == 0 && d >= 5*time.Millisecond && d <= 50*time.Millisecond)
		n, _ = DropPolicy{Kind: "conn", P: 1}.plan(rng, 10)
		assert.True(t, n >= 1 && n <= 10)
		n, _ = DropPolicy{Kind: "conn", P: 1}.plan(rng, 0)
		assert.Zero(t, n, "nothing left to drop before")
	}
}

// newHubServer serves the load contract with a real Hub on the memory store.
func newHubServer(t *testing.T) *httptest.Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })
	mux := http.NewServeMux()
	mux.Handle("POST /loadtest/streams", StartHandler(hub, "solo"))
	mux.Handle("GET /streams/{id}", httpx.NewHandler(hub, httpx.WithLogger(logger)).Stream(httpx.PathValue("id")))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// End to end on the real library: concurrent streams, readers dropping every 1-3 events and
// resuming, every stream audited. Nothing is reported, and every event was measured.
func TestRun_RealHub(t *testing.T) {
	t.Parallel()
	srv := newHubServer(t)
	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.Phases = []Phase{{Spec: "4x1-3", Streams: 4, ReadersMin: 1, ReadersMax: 3, Hold: 200 * time.Millisecond}}
	cfg.Stream = StartRequest{Events: 20, DelayMS: 1, Size: 64}
	cfg.Drop = DropPolicy{Kind: "every", Min: 1, Max: 3}
	cfg.AuditRate = 1
	cfg.Window = 50 * time.Millisecond
	cfg.Slack = 10 * time.Second
	var progress strings.Builder
	cfg.Progress = &progress
	rep, err := Run(context.Background(), cfg)
	require.NoError(t, err)

	p := rep.Phases[0]
	assert.True(t, rep.Passed, "%+v", p.Failures)
	assert.Empty(t, p.Integrity)
	assert.Positive(t, p.StreamsStarted)
	assert.Equal(t, p.StreamsStarted, p.Terminals["done"])
	assert.Equal(t, p.StreamsStarted, p.Audited)
	assert.Zero(t, p.AuditUnavailable)
	assert.Equal(t, map[string]int{"solo": p.StreamsStarted}, p.Producers)
	assert.Equal(t, uint64(p.Readers), p.TTFE.Count)                         //nolint:gosec // a count
	assert.Equal(t, uint64(p.StreamsStarted), p.StartLat.Count)              //nolint:gosec // a count
	assert.Equal(t, uint64(p.Conns), p.ConnectLat.Count)                     //nolint:gosec // a count
	assert.Equal(t, p.EventsReceived, int64(p.Latency.Count+p.Replay.Count)) //nolint:gosec // a count
	assert.Positive(t, p.Ended[EndDropped])
	assert.Empty(t, p.HTTPErrors)
	assert.Positive(t, rep.TimerBefore)
	assert.Contains(t, progress.String(), "ev/s=")

	var table, js strings.Builder
	require.NoError(t, rep.WriteTable(&table))
	assert.Contains(t, table.String(), "4x1-3")
	require.NoError(t, rep.WriteJSON(&js))
	var back Report
	require.NoError(t, json.Unmarshal([]byte(js.String()), &back))
	assert.Equal(t, rep.Seed, back.Seed)
}

// A slow reader stops reading while its connection stays open. With an httpx write timeout
// shorter than the pause and events large enough to fill the socket buffers, the server closes
// the response; the reader then resumes with Last-Event-ID and still gets every event once.
func TestRun_SlowReaderIsReleasedAndResumes(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := rebound.New(memory.New(), rebound.WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hub.Close(context.Background())) })
	mux := http.NewServeMux()
	mux.Handle("POST /loadtest/streams", StartHandler(hub, "solo"))
	mux.Handle("GET /streams/{id}", httpx.NewHandler(hub, httpx.WithLogger(logger),
		httpx.WithWriteTimeout(200*time.Millisecond)).Stream(httpx.PathValue("id")))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.SlowStreams = 1
	cfg.SlowStream = StartRequest{Events: 800, DelayMS: 0, Size: MaxSize} // 25 MiB: more than the socket buffers
	cfg.Slow = &SlowPolicy{AfterEvents: 1, Pause: 2 * time.Second}
	cfg.Window = 100 * time.Millisecond
	cfg.Slack = 30 * time.Second
	rep, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, rep.Slow)
	assert.Equal(t, 1, rep.Slow.SlowReleased, "the write timeout closed the paused connection")
	assert.Empty(t, rep.Slow.Integrity)
	assert.Equal(t, 1, rep.Slow.Terminals["done"])
	assert.True(t, rep.Passed)
	for i := range rep.Windows {
		assert.Zero(t, rep.Windows[i].Events, "a slow reader's events stay out of the windows")
	}
}
