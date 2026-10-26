package conformance

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/internal/sseclient"
)

// Config of a tester run (docs/06 §3). Start from DefaultConfig.
type Config struct {
	BaseURL           string
	Runs              int           // runs of S2, the core scenario
	OtherRuns         int           // runs of every other scenario
	Parallel          int           // runs in flight at once, and streams at once in S10
	Concurrency       int           // readers per stream in S6
	DropRate          float64       // probability of dropping the connection after each event
	TextSize          int           // characters of the generated text
	Chunk             int           // characters per event
	DelayMS           int           // pause between events, in ms
	MaxReconnectDelay time.Duration // reconnect after a random 0..this delay
	CancelWithin      time.Duration // S4: the terminal event must follow the cancel within this
	Seed              uint64        // 0: random; the effective seed is in the report
	Scenarios         []string      // IDs to run; empty: all
	Timeout           time.Duration // per run
	Trace             bool          // record every run's connections in the report
	Client            *http.Client  // nil: a dedicated client sized for Parallel
}

// DefaultConfig returns the defaults of docs/06 §3.
func DefaultConfig() Config {
	return Config{
		Runs:              200,
		OtherRuns:         20,
		Parallel:          20,
		Concurrency:       3,
		DropRate:          0.3,
		TextSize:          2000,
		Chunk:             4,
		DelayMS:           5,
		MaxReconnectDelay: 200 * time.Millisecond,
		CancelWithin:      2 * time.Second,
		Timeout:           60 * time.Second,
	}
}

func (c Config) validate() error {
	switch {
	case c.BaseURL == "":
		return errors.New("conformance: base URL is required")
	case c.Runs < 1, c.OtherRuns < 1, c.Parallel < 1, c.Concurrency < 1, c.TextSize < 1, c.Chunk < 1:
		return errors.New("conformance: runs, other runs, parallel, concurrency, text size and chunk must be positive")
	case c.DropRate < 0 || c.DropRate >= 1:
		return errors.New("conformance: drop rate must be in [0, 1)")
	case c.DelayMS < 0, c.MaxReconnectDelay < 0, c.CancelWithin <= 0, c.Timeout <= 0:
		return errors.New("conformance: delays must be non-negative and timeouts positive")
	}
	return nil
}

// runner holds what every scenario run needs.
type runner struct {
	cfg  Config
	seed uint64
}

// Run executes the selected scenarios against cfg.BaseURL and returns the report. The error is
// non-nil only when the run could not be set up; scenario failures are in the report.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if len(cfg.Scenarios) == 0 {
		for _, s := range scenarios {
			cfg.Scenarios = append(cfg.Scenarios, s.id)
		}
	}
	selected := make([]scenario, 0, len(cfg.Scenarios))
	for _, id := range cfg.Scenarios {
		i := slices.IndexFunc(scenarios, func(s scenario) bool { return s.id == strings.ToUpper(strings.TrimSpace(id)) })
		if i < 0 {
			return nil, fmt.Errorf("conformance: unknown scenario %q", id)
		}
		selected = append(selected, scenarios[i])
	}
	if cfg.Client == nil {
		conns := cfg.Parallel * (cfg.Concurrency + 2)
		transport := &http.Transport{MaxIdleConns: conns, MaxIdleConnsPerHost: conns, IdleConnTimeout: 30 * time.Second}
		defer transport.CloseIdleConnections()
		cfg.Client = &http.Client{Transport: transport}
	}
	seed := cfg.Seed
	if seed == 0 {
		var err error
		if seed, err = NewSeed(); err != nil {
			return nil, err
		}
	}
	r := &runner{cfg: cfg, seed: seed}
	report := &Report{BaseURL: cfg.BaseURL, Seed: seed, Passed: true}
	for _, sc := range selected {
		sr := r.runScenario(ctx, sc)
		report.Scenarios = append(report.Scenarios, sr)
		if sr.Failures > 0 {
			report.Passed = false
		}
	}
	return report, nil
}

// runScenario runs sc the configured number of times, Parallel at a time.
func (r *runner) runScenario(ctx context.Context, sc scenario) ScenarioReport {
	runs := r.cfg.OtherRuns
	if sc.id == "S2" {
		runs = r.cfg.Runs
	}
	workers := r.cfg.Parallel
	if sc.id == "S10" {
		workers = 1 // each S10 run already starts Parallel streams at once
	}
	results := make([]runResult, runs)
	var wg sync.WaitGroup
	next := make(chan int)
	for range min(workers, runs) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				runCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
				results[i] = sc.run(runCtx, runInfo{scenario: sc.id, run: i, r: r})
				cancel()
			}
		}()
	}
	for i := range runs {
		next <- i
	}
	close(next)
	wg.Wait()
	return summarize(sc, results, r.cfg.Trace)
}

// rng derives the random source of one run (or one reader of a run) from the seed, so that the
// same seed replays the same decisions whatever the scheduling.
func (r *runner) rng(scenarioID string, run, reader int) *mrand.Rand {
	h := uint64(14695981039346656037)
	for _, c := range []byte(scenarioID) {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return mrand.New(mrand.NewPCG(r.seed^h, uint64(run)<<16|uint64(reader))) //nolint:gosec // run and reader are small non-negative indexes
}

// textAlphabet mixes ASCII, multi-byte UTF-8 and line breaks (split into several data lines on
// the wire and joined back by the client).
var textAlphabet = []rune("abcdefghijklmnopqrstuvwxyz ABCDEFGHIJ0123456789 .,;!?'\"éèàçüœ€你好😀\n")

func (r *runner) text(rng *mrand.Rand) string {
	runes := make([]rune, r.cfg.TextSize)
	for i := range runes {
		runes[i] = textAlphabet[rng.IntN(len(textAlphabet))]
	}
	return string(runes)
}

func (r *runner) request(rng *mrand.Rand) StartRequest {
	return StartRequest{Text: r.text(rng), Chunk: r.cfg.Chunk, DelayMS: r.cfg.DelayMS, FailAfter: -1}
}

// start creates a stream and returns its id and the time the server answered.
func (r *runner) start(ctx context.Context, req StartRequest) (string, time.Time, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", time.Time{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.BaseURL+"/conformance/streams", bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := r.cfg.Client.Do(httpReq)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("start: %w", err)
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusCreated {
		return "", time.Time{}, fmt.Errorf("start: HTTP %d", resp.StatusCode)
	}
	var created struct {
		StreamID string `json:"stream_id"`
		URL      string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return "", time.Time{}, fmt.Errorf("start: decode: %w", err)
	}
	if created.StreamID == "" || created.URL != "/streams/"+created.StreamID {
		return "", time.Time{}, fmt.Errorf("start: bad response %+v", created)
	}
	return created.StreamID, time.Now(), nil
}

// get performs GET /streams/{id}, with Last-Event-ID when cursor is set.
func (r *runner) get(ctx context.Context, id string, cursor *uint64) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.BaseURL+"/streams/"+id, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if cursor != nil {
		req.Header.Set("Last-Event-ID", strconv.FormatUint(*cursor, 10))
	}
	return r.cfg.Client.Do(req)
}

// cancel performs POST /streams/{id}/cancel and expects 202.
func (r *runner) cancel(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.BaseURL+"/streams/"+id+"/cancel", http.NoBody)
	if err != nil {
		return err
	}
	resp, err := r.cfg.Client.Do(req)
	if err != nil {
		return fmt.Errorf("cancel: %w", err)
	}
	drainClose(resp)
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("cancel: HTTP %d, want 202", resp.StatusCode)
	}
	return nil
}

// status performs GET /streams/{id} with an optional cursor and returns the status code and
// body, for the scenarios that check error statuses.
func (r *runner) status(ctx context.Context, id string, cursor *uint64) (code int, body []byte, err error) {
	resp, err := r.get(ctx, id, cursor)
	if err != nil {
		return 0, nil, err
	}
	defer drainClose(resp)
	if resp.StatusCode == http.StatusOK {
		return resp.StatusCode, nil, nil // a stream body: not read here
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, body, err
}

// drainClose reads what is left of a response body (up to 64 KiB) and closes it.
func drainClose(resp *http.Response) {
	// Draining lets the connection be reused; errors only mean it will not be.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}

// readMode selects how a reader reconnects after a drop.
type readMode int

const (
	modeResume  readMode = iota // reconnect with Last-Event-ID = last seq received
	modeRestart                 // reconnect without Last-Event-ID: the stream restarts at 1
)

// readOptions configure one reader.
type readOptions struct {
	mode     readMode
	dropRate float64
	// maxDrops, when positive, stops dropping after that many drops (S3: a restarting reader
	// must eventually read the whole stream in one connection).
	maxDrops int
	// afterEvent, when set, runs after each received non-terminal event (S4 uses it to cancel).
	afterEvent func(received int)
}

// readResult is what one reader observed across all its connections.
type readResult struct {
	events      []rebound.Event // resume: every event in order; restart: the last connection's
	terminal    *rebound.Event
	conns       []ConnLog
	firstEvent  time.Time
	terminalAt  time.Time
	reconnected int
}

const maxConnections = 10_000

// read follows stream id to its terminal event, dropping and reconnecting per opts.
func (r *runner) read(ctx context.Context, rng *mrand.Rand, id string, opts readOptions) (readResult, error) {
	var res readResult
	var cursor *uint64
	received := 0
	for len(res.conns) < maxConnections {
		if len(res.conns) > 0 {
			res.reconnected++
			if err := sleep(ctx, randomDelay(rng, r.cfg.MaxReconnectDelay)); err != nil {
				return res, err
			}
		}
		if opts.mode == modeRestart {
			res.events = nil
		}
		log, err := r.connection(ctx, rng, id, cursor, opts, &res, &received)
		res.conns = append(res.conns, log)
		if err != nil {
			return res, err
		}
		if res.terminal != nil {
			return res, nil
		}
		if opts.mode == modeResume && len(res.events) > 0 {
			last := res.events[len(res.events)-1].Seq
			cursor = &last
		}
	}
	return res, fmt.Errorf("no terminal event after %d connections", maxConnections)
}

// connection runs one GET and reads until the terminal event, a drop, or the end of the body.
func (r *runner) connection(ctx context.Context, rng *mrand.Rand, id string, cursor *uint64, opts readOptions,
	res *readResult, received *int,
) (ConnLog, error) {
	log := ConnLog{Cursor: cursorValue(cursor)}
	connCtx, drop := context.WithCancel(ctx)
	defer drop()
	resp, err := r.get(connCtx, id, cursor)
	if err != nil {
		log.Ended = "error"
		return log, fmt.Errorf("connect after %d: %w", log.Cursor, err)
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		log.Ended = "status " + strconv.Itoa(resp.StatusCode)
		return log, fmt.Errorf("connect after %d: HTTP %d before the terminal event", log.Cursor, resp.StatusCode)
	}
	expectSeq := log.Cursor + 1
	if opts.mode == modeRestart {
		expectSeq = 1
	}
	rd := sseclient.NewReader(resp.Body)
	for {
		f, err := rd.Next()
		if err != nil {
			if ctx.Err() != nil {
				log.Ended = "timeout"
				return log, ctx.Err()
			}
			log.Ended = "closed by server"
			return log, nil // reconnect: a body ending without terminal is a normal drop
		}
		if !f.IsEvent() {
			continue
		}
		ev, err := toEvent(f)
		if err != nil {
			log.Ended = "protocol error"
			return log, err
		}
		if ev.Seq != expectSeq {
			log.Ended = "sequence error"
			return log, fmt.Errorf("connection after %d: got seq %d, want %d (gap or duplicate)", log.Cursor, ev.Seq, expectSeq)
		}
		expectSeq++
		log.Events++
		if res.firstEvent.IsZero() {
			res.firstEvent = time.Now()
		}
		if ev.Kind == "done" || ev.Kind == "error" {
			res.terminal, res.terminalAt = &ev, time.Now()
			log.Ended = "terminal " + ev.Kind
			return log, nil
		}
		res.events = append(res.events, ev)
		*received++
		if opts.afterEvent != nil {
			opts.afterEvent(*received)
		}
		if opts.dropRate > 0 && (opts.maxDrops == 0 || res.reconnected < opts.maxDrops) && rng.Float64() < opts.dropRate {
			log.Ended = "dropped"
			return log, nil
		}
	}
}

func cursorValue(c *uint64) uint64 {
	if c == nil {
		return 0
	}
	return *c
}

// toEvent converts a parsed frame to an event; stored events must carry a numeric id.
func toEvent(f sseclient.Frame) (rebound.Event, error) {
	if !f.HasID {
		return rebound.Event{}, fmt.Errorf("event %q without id", f.Event)
	}
	seq, err := strconv.ParseUint(f.ID, 10, 64)
	if err != nil {
		return rebound.Event{}, fmt.Errorf("event id %q is not a decimal seq", f.ID)
	}
	kind := f.Event
	if kind == "" {
		kind = "message"
	}
	return rebound.Event{Seq: seq, Kind: kind, Data: []byte(f.Data)}, nil
}

func randomDelay(rng *mrand.Rand, maxDelay time.Duration) time.Duration {
	if maxDelay <= 0 {
		return 0
	}
	return time.Duration(rng.Int64N(int64(maxDelay) + 1))
}

// sleep waits d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// terminalCode returns the code of a terminal error event, or "done".
func terminalCode(ev *rebound.Event) string {
	if ev.Kind == "done" {
		return "done"
	}
	var p struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(ev.Data, &p); err != nil || p.Code == "" {
		return "error(unparsable)"
	}
	return p.Code
}

// verify compares what a reader assembled with what the producer wrote.
func verify(exp Expected, got []rebound.Event, terminal *rebound.Event) error {
	for i := range min(len(got), len(exp.Events)) {
		want, have := exp.Events[i], got[i]
		if want.Seq != have.Seq || want.Kind != have.Kind || !bytes.Equal(want.Data, have.Data) {
			return fmt.Errorf("first divergence at seq %d: got %s %q, want %s %q",
				want.Seq, have.Kind, abbreviate(have.Data), want.Kind, abbreviate(want.Data))
		}
	}
	if len(got) != len(exp.Events) {
		return fmt.Errorf("received %d events before the terminal, want %d", len(got), len(exp.Events))
	}
	if terminal == nil {
		return errors.New("no terminal event")
	}
	if code := terminalCode(terminal); code != exp.Terminal {
		return fmt.Errorf("terminal %s, want %s", code, exp.Terminal)
	}
	if want := uint64(len(exp.Events)) + 1; terminal.Seq != want {
		return fmt.Errorf("terminal at seq %d, want %d", terminal.Seq, want)
	}
	return nil
}

func abbreviate(b []byte) string {
	const maxLen = 40
	if len(b) <= maxLen {
		return string(b)
	}
	return string(b[:maxLen]) + "…"
}

// NewSeed returns a random non-zero seed, for callers that print it before a run.
func NewSeed() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("conformance: random seed: %w", err)
	}
	return binary.LittleEndian.Uint64(b[:]) | 1, nil
}
