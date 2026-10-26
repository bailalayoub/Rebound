package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

// newTarget serves the conformance contract with a Hub over s.
func newTarget(t *testing.T, s store.Store) *httptest.Server {
	t.Helper()
	hub, err := rebound.New(s, rebound.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	streams := httpx.NewHandler(hub)
	mux := http.NewServeMux()
	mux.Handle("POST /conformance/streams", StartHandler(hub))
	mux.Handle("GET /streams/{id}", streams.Stream(httpx.PathValue("id")))
	mux.Handle("POST /streams/{id}/cancel", streams.Cancel(httpx.PathValue("id")))
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hub.Close(ctx) // producers of cancelled or abandoned streams end here
	})
	return srv
}

// smallConfig keeps runs short: few runs, short texts, millisecond delays.
func smallConfig(baseURL string) Config {
	cfg := DefaultConfig()
	cfg.BaseURL = baseURL
	cfg.Runs, cfg.OtherRuns, cfg.Parallel, cfg.Concurrency = 20, 3, 4, 3
	cfg.TextSize, cfg.DelayMS, cfg.MaxReconnectDelay = 200, 1, 2*time.Millisecond
	cfg.Timeout = 20 * time.Second
	cfg.Seed = 42
	return cfg
}

func TestRun_AllScenariosPassOnACorrectServer(t *testing.T) {
	t.Parallel()
	srv := newTarget(t, memory.New())
	report, err := Run(context.Background(), smallConfig(srv.URL))
	require.NoError(t, err)

	var table bytes.Buffer
	require.NoError(t, report.WriteTable(&table))
	t.Log("\n" + table.String())
	require.True(t, report.Passed)
	require.Len(t, report.Scenarios, 12)
	for _, s := range report.Scenarios {
		assert.Zero(t, s.Failures, "%s %s: %+v", s.ID, s.Name, s.FailureDetails)
	}
	byID := map[string]ScenarioReport{}
	for _, s := range report.Scenarios {
		byID[s.ID] = s
	}
	assert.Equal(t, 20, byID["S2"].Runs)
	assert.Equal(t, 3, byID["S1"].Runs)
	assert.Positive(t, byID["S2"].Reconnections, "drops happened and were resumed")
	assert.Zero(t, byID["S1"].Reconnections)
	assert.Positive(t, byID["S10"].TTFEP95ms)
	assert.Contains(t, table.String(), "PASS: 12/12 scenarios passed")
}

// skipEvent hides one event from every read, like a server that loses an event at the
// replay/live seam.
type skipEvent struct {
	store.Store
	seq uint64
}

func (s skipEvent) Read(ctx context.Context, id string, after uint64, limit int, block time.Duration) ([]store.Event, error) {
	evs, err := s.Store.Read(ctx, id, after, limit, block)
	out := evs[:0]
	for _, ev := range evs {
		if ev.Seq != s.seq {
			out = append(out, ev)
		}
	}
	return out, err
}

func TestRun_DetectsAGap(t *testing.T) {
	t.Parallel()
	srv := newTarget(t, skipEvent{Store: memory.New(), seq: 5})
	cfg := smallConfig(srv.URL)
	cfg.Scenarios = []string{"s1", "S2"}
	report, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	require.False(t, report.Passed)
	for _, s := range report.Scenarios {
		assert.Equal(t, s.Runs, s.Failures, s.ID)
		require.NotEmpty(t, s.FailureDetails, s.ID)
		assert.Contains(t, s.FailureDetails[0].Error, "got seq 6, want 5", s.ID)
		assert.NotEmpty(t, s.FailureDetails[0].StreamID)
		assert.NotEmpty(t, s.FailureDetails[0].Connections)
	}
	var table bytes.Buffer
	require.NoError(t, report.WriteTable(&table))
	assert.Contains(t, table.String(), "FAIL S1 run")
	assert.Contains(t, table.String(), "FAIL: 0/2 scenarios passed (reproduce with --seed 42)")
}

func TestRun_SameSeedReplaysTheSameDrops(t *testing.T) {
	t.Parallel()
	srv := newTarget(t, memory.New())
	cfg := smallConfig(srv.URL)
	cfg.Scenarios, cfg.Trace, cfg.Runs = []string{"S2", "S3", "S6"}, true, 10

	first, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	second, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	require.True(t, first.Passed)
	require.True(t, second.Passed)
	for i := range first.Scenarios {
		assert.Equal(t, first.Scenarios[i].ConnectionTrace, second.Scenarios[i].ConnectionTrace, first.Scenarios[i].ID)
		assert.Equal(t, first.Scenarios[i].Reconnections, second.Scenarios[i].Reconnections)
	}
	assert.Positive(t, first.Scenarios[0].Reconnections)

	cfg.Seed = 43
	other, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotEqual(t, first.Scenarios[0].ConnectionTrace, other.Scenarios[0].ConnectionTrace, "another seed drops elsewhere")
}

func TestRun_DoesNotLeakGoroutines(t *testing.T) {
	srv := newTarget(t, memory.New())
	cfg := smallConfig(srv.URL)
	cfg.Scenarios = []string{"S2", "S4", "S6", "S10"}
	before := runtime.NumGoroutine()
	report, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	require.True(t, report.Passed)
	// Server-side handlers and producers wind down asynchronously; the tester's own goroutines must
	// be gone. A generous bound absorbs the server's idle connections.
	testutil.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+10 }, 10*time.Second)
}

func TestRun_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	_, err := Run(context.Background(), DefaultConfig())
	require.ErrorContains(t, err, "base URL")

	cfg := smallConfig("http://127.0.0.1:1")
	cfg.Scenarios = []string{"S99"}
	_, err = Run(context.Background(), cfg)
	require.ErrorContains(t, err, "unknown scenario")

	for _, mutate := range []func(*Config){
		func(c *Config) { c.Runs = 0 },
		func(c *Config) { c.DropRate = 1 },
		func(c *Config) { c.DelayMS = -1 },
		func(c *Config) { c.Timeout = 0 },
	} {
		cfg := smallConfig("http://127.0.0.1:1")
		mutate(&cfg)
		_, err := Run(context.Background(), cfg)
		require.Error(t, err)
	}
}

func TestRun_UnreachableServerFailsEveryScenario(t *testing.T) {
	t.Parallel()
	cfg := smallConfig("http://127.0.0.1:1")
	cfg.Runs, cfg.OtherRuns = 1, 1
	report, err := Run(context.Background(), cfg)
	require.NoError(t, err)
	assert.False(t, report.Passed)
	for _, s := range report.Scenarios {
		assert.Equal(t, 1, s.Failures, s.ID)
	}
	var out bytes.Buffer
	require.NoError(t, report.WriteJSON(&out))
	var decoded Report
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
	assert.Equal(t, uint64(42), decoded.Seed)
	assert.False(t, decoded.Passed)
}

func TestPercentileAndFormatting(t *testing.T) {
	t.Parallel()
	ds := []time.Duration{5 * time.Millisecond, time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}
	assert.Equal(t, 3*time.Millisecond, percentile(ds, 50))
	assert.Equal(t, 5*time.Millisecond, percentile(ds, 95))
	assert.Zero(t, percentile(nil, 50))
	assert.Equal(t, "-", fmtMS(0))
	assert.Equal(t, "12.5 ms", fmtMS(12.5))
	assert.Equal(t, "2.50 s", fmtMS(2500))
	assert.True(t, strings.HasSuffix(abbreviate(bytes.Repeat([]byte("x"), 100)), "…"))
}
