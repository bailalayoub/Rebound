package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Phase holds Streams streams in flight, each read by ReadersMin..ReadersMax readers, for Hold,
// after moving there linearly from the previous phase's level over Ramp.
type Phase struct {
	Spec                   string
	Streams                int
	ReadersMin, ReadersMax int
	Ramp, Hold             time.Duration
}

// ParsePhases parses a comma-separated list of STREAMSxREADERS[~RAMP]@HOLD, where READERS is N
// or MIN-MAX: "50x1-3@3m,1250x2~10s@1m,0x1@30s".
func ParsePhases(s string) ([]Phase, error) {
	var out []Phase
	for spec := range strings.SplitSeq(s, ",") {
		spec = strings.TrimSpace(spec)
		p, err := parsePhase(spec)
		if err != nil {
			return nil, fmt.Errorf("loadtest: phase %q: %w", spec, err)
		}
		out = append(out, p)
	}
	return out, nil
}

func parsePhase(spec string) (Phase, error) {
	p := Phase{Spec: spec}
	left, hold, ok := strings.Cut(spec, "@")
	if !ok {
		return p, errors.New("want STREAMSxREADERS[~RAMP]@HOLD")
	}
	var err error
	if p.Hold, err = time.ParseDuration(hold); err != nil || p.Hold <= 0 {
		return p, errors.New("hold must be a positive duration")
	}
	counts, ramp, hasRamp := strings.Cut(left, "~")
	if hasRamp {
		if p.Ramp, err = time.ParseDuration(ramp); err != nil || p.Ramp < 0 {
			return p, errors.New("ramp must be a duration")
		}
	}
	streams, readers, ok := strings.Cut(counts, "x")
	if !ok {
		return p, errors.New("want STREAMSxREADERS")
	}
	if p.Streams, err = strconv.Atoi(streams); err != nil || p.Streams < 0 {
		return p, errors.New("streams must be a non-negative integer")
	}
	lo, hi, ok := strings.Cut(readers, "-")
	if !ok {
		hi = lo
	}
	p.ReadersMin, err = strconv.Atoi(lo)
	if err != nil || p.ReadersMin < 1 {
		return p, errors.New("readers must be at least 1")
	}
	if p.ReadersMax, err = strconv.Atoi(hi); err != nil || p.ReadersMax < p.ReadersMin {
		return p, errors.New("readers must be N or MIN-MAX")
	}
	return p, nil
}

// Config of a run.
type Config struct {
	BaseURL string
	Phases  []Phase
	// Stream is the request every stream starts with.
	Stream StartRequest
	Drop   DropPolicy
	// SlowStreams streams, started at the beginning with SlowStream and one reader each, follow
	// Slow (nil: none).
	Slow        *SlowPolicy
	SlowStreams int
	SlowStream  StartRequest
	// AuditRate is the share of streams read again from the start once their readers are done, and
	// compared byte for byte with what each reader received.
	AuditRate float64
	Window    time.Duration
	Seed      uint64
	// Slack is added to a stream's nominal duration to give its readers a deadline.
	Slack time.Duration
	// MaxConns bounds the connections of one reader.
	MaxConns int
	// Drain bounds the wait for the streams in flight after the last phase.
	Drain time.Duration
	// KeepFailures is how many failed streams each phase reports in detail.
	KeepFailures int
	Client       *http.Client
	Progress     io.Writer // one line per window; nil: none
}

// DefaultConfig returns a configuration to complete with BaseURL and Phases.
func DefaultConfig() Config {
	return Config{
		Stream:       StartRequest{Events: 400, DelayMS: 50, Size: 0},
		Drop:         DropPolicy{Kind: "none"},
		Window:       10 * time.Second,
		Slack:        2 * time.Minute,
		MaxConns:     100_000,
		Drain:        3 * time.Minute,
		KeepFailures: 10,
	}
}

func (c *Config) validate() error {
	switch {
	case c.BaseURL == "":
		return errors.New("loadtest: base URL is required")
	case len(c.Phases) == 0 && c.SlowStreams == 0:
		return errors.New("loadtest: at least one phase or slow stream is required")
	case c.Window <= 0, c.Slack <= 0, c.Drain <= 0, c.MaxConns < 1:
		return errors.New("loadtest: window, slack, drain and max conns must be positive")
	case c.AuditRate < 0 || c.AuditRate > 1:
		return errors.New("loadtest: audit rate must be in [0, 1]")
	case c.SlowStreams > 0 && c.Slow == nil:
		return errors.New("loadtest: slow streams need a slow policy")
	}
	if err := c.Stream.Validate(); err != nil {
		return err
	}
	if c.SlowStreams > 0 {
		return c.SlowStream.Validate()
	}
	return nil
}

// driver is one run in progress.
type driver struct {
	cfg    Config
	start  time.Time
	rec    *Recorder
	active atomic.Int64
	target atomic.Int64
	phase  atomic.Int64 // index of the current phase

	mu      sync.Mutex
	phases  []*phaseStats
	windows []*Window
	slow    *phaseStats
}

// Run drives cfg.Phases against cfg.BaseURL and returns the report. The error is non-nil only
// when the run could not start; what went wrong during it is in the report.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.Seed == 0 {
		cfg.Seed = mrand.Uint64() | 1 //nolint:gosec // the seed of a load run, printed in the report
	}
	if cfg.Client == nil {
		transport := &http.Transport{
			MaxIdleConns: 10_000, MaxIdleConnsPerHost: 10_000, IdleConnTimeout: 30 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second, DisableCompression: true,
		}
		defer transport.CloseIdleConnections()
		cfg.Client = &http.Client{Transport: transport}
	}
	d := &driver{cfg: cfg, start: time.Now()}
	d.rec = newRecorder(d.start)
	for _, p := range cfg.Phases {
		d.phases = append(d.phases, newPhaseStats(p.Spec))
	}
	d.slow = newPhaseStats("slow")
	timerBefore := MeasureTimer()

	windowsDone := make(chan struct{})
	stopWindows := make(chan struct{})
	go func() {
		defer close(windowsDone)
		d.rotateWindows(stopWindows)
	}()

	var slow sync.WaitGroup
	for i := range uint64(max(cfg.SlowStreams, 0)) {
		slow.Go(func() {
			d.stream(ctx, d.slow, mrand.New(mrand.NewPCG(cfg.Seed, 1<<32+i)), 1, cfg.SlowStream, cfg.Slow)
		})
	}
	boundaries := d.schedule(ctx)
	slow.Wait()
	close(stopWindows)
	<-windowsDone

	rep := d.report(boundaries)
	rep.TimerBefore, rep.TimerAfter = ms(timerBefore), ms(MeasureTimer())
	return rep, nil
}

// MeasureTimer returns the mean duration of ten 10 ms timers: the check of internal/timercheck,
// recorded with each result because a slow VM timer invalidates a run.
func MeasureTimer() time.Duration {
	const n, d = 10, 10 * time.Millisecond
	start := time.Now()
	for range n {
		t := time.NewTimer(d)
		<-t.C
	}
	return time.Since(start) / n
}

// schedule runs the phases, keeping the target number of streams in flight, then waits for the
// streams in flight to finish. It returns when each phase started, and when the drain started.
func (d *driver) schedule(ctx context.Context) []time.Time {
	var (
		slots   sync.WaitGroup
		mu      sync.Mutex
		running = map[int]bool{}
	)
	spawn := func(target int) {
		mu.Lock()
		defer mu.Unlock()
		for i := range target {
			if running[i] {
				continue
			}
			running[i] = true
			slots.Go(func() {
				d.slot(ctx, i)
				mu.Lock()
				delete(running, i)
				mu.Unlock()
			})
		}
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	boundaries := make([]time.Time, 0, len(d.cfg.Phases)+1)
	level := 0
	for i, p := range d.cfg.Phases {
		d.phase.Store(int64(i))
		begin := time.Now()
		boundaries = append(boundaries, begin)
		from := level
		for {
			elapsed := time.Since(begin)
			if elapsed >= p.Ramp+p.Hold || ctx.Err() != nil {
				break
			}
			target := p.Streams
			if elapsed < p.Ramp {
				target = from + int(float64(p.Streams-from)*float64(elapsed)/float64(p.Ramp))
			}
			d.target.Store(int64(target))
			spawn(target)
			select {
			case <-tick.C:
			case <-ctx.Done():
			}
		}
		level = p.Streams
	}
	d.target.Store(0)
	boundaries = append(boundaries, time.Now())
	done := make(chan struct{})
	go func() {
		slots.Wait()
		close(done)
	}()
	drain := time.NewTimer(d.cfg.Drain)
	defer drain.Stop()
	select {
	case <-done:
	case <-drain.C:
		// Streams still in flight past the drain have readers with deadlines; their results
		// are lost to the report, which says how many were active when it was written.
	case <-ctx.Done():
	}
	return boundaries
}

// slot runs streams one after the other while its index is below the target.
func (d *driver) slot(ctx context.Context, i int) {
	for n := 0; ctx.Err() == nil && int64(i) < d.target.Load(); n++ {
		phase := int(d.phase.Load())
		p := d.cfg.Phases[phase]
		rng := mrand.New(mrand.NewPCG(d.cfg.Seed, uint64(i)<<32|uint64(n))) //nolint:gosec // small indexes
		readers := p.ReadersMin + rng.IntN(p.ReadersMax-p.ReadersMin+1)
		if !d.stream(ctx, d.phases[phase], rng, readers, d.cfg.Stream, nil) {
			// The start failed: do not hammer a server that refuses.
			if pause(ctx, 200*time.Millisecond) != nil {
				return
			}
		}
	}
}

// stream starts one stream, follows it with its readers, audits it, and records the outcome.
func (d *driver) stream(ctx context.Context, ps *phaseStats, rng *mrand.Rand, readers int, req StartRequest, slow *SlowPolicy) bool {
	sent := time.Now()
	id, err := d.startStream(ctx, req)
	d.rec.streamStarted(err == nil)
	if err != nil {
		ps.startFailed()
		return false
	}
	d.active.Add(1)
	defer d.active.Add(-1)
	deadline := sent.Add(time.Duration(req.Events)*time.Duration(req.DelayMS)*time.Millisecond + d.cfg.Slack)
	if slow != nil {
		deadline = deadline.Add(slow.Pause)
	}
	rds := make([]*reader, readers)
	var wg sync.WaitGroup
	for k := range rds {
		rds[k] = &reader{
			client: d.cfg.Client, baseURL: d.cfg.BaseURL, id: id, events: req.Events,
			drop: d.cfg.Drop, slow: slow, rng: mrand.New(mrand.NewPCG(rng.Uint64(), uint64(k))), //nolint:gosec // small index
			deadline: deadline, maxConns: d.cfg.MaxConns, stats: d.rec, started: sent, keepLogs: true,
		}
		wg.Go(func() { rds[k].run(ctx) })
	}
	wg.Wait()
	var audit *reader
	if d.cfg.AuditRate > 0 && rng.Float64() < d.cfg.AuditRate && ctx.Err() == nil {
		audit = &reader{
			client: d.cfg.Client, baseURL: d.cfg.BaseURL, id: id, events: req.Events, drop: DropPolicy{Kind: "none"},
			rng: rng, deadline: time.Now().Add(d.cfg.Slack), maxConns: 20, started: time.Now(),
		}
		audit.run(ctx)
	}
	d.rec.streamDone()
	ps.record(id, req.Events, rds, audit, d.cfg.KeepFailures)
	return true
}

// startStream performs POST /loadtest/streams.
func (d *driver) startStream(ctx context.Context, req StartRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, d.cfg.BaseURL+"/loadtest/streams", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	sent := time.Now()
	resp, err := d.cfg.Client.Do(httpReq)
	if err == nil {
		d.rec.started(time.Since(sent))
	}
	if err != nil {
		d.rec.httpError("start", "transport")
		return "", fmt.Errorf("loadtest: start: %w", err)
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusCreated {
		d.rec.httpError("start", strconv.Itoa(resp.StatusCode))
		return "", fmt.Errorf("loadtest: start: HTTP %d", resp.StatusCode)
	}
	var created struct {
		StreamID string `json:"stream_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil || created.StreamID == "" {
		d.rec.httpError("start", "bad_body")
		return "", errors.New("loadtest: start: bad response body")
	}
	return created.StreamID, nil
}

// rotateWindows closes a window every cfg.Window until stop is closed, then closes the last one.
func (d *driver) rotateWindows(stop <-chan struct{}) {
	t := time.NewTicker(d.cfg.Window)
	defer t.Stop()
	for {
		select {
		case now := <-t.C:
			d.closeWindow(now)
		case <-stop:
			d.closeWindow(time.Now())
			return
		}
	}
}

func (d *driver) closeWindow(now time.Time) {
	w := d.rec.rotate(now, d.target.Load(), d.active.Load())
	d.mu.Lock()
	d.windows = append(d.windows, w)
	d.mu.Unlock()
	if d.cfg.Progress != nil {
		r := w.report(d.start)
		_, _ = fmt.Fprintf(d.cfg.Progress, // progress output is best effort
			"t=%6.0fs target=%-5d active=%-5d ev/s=%-8.0f ttfe p99=%-8.1fms lat p50=%-6.1f p99=%-8.1fms conns=%-6d errors=%v\n",
			r.T, r.Target, r.Active, r.EventsPerS, r.TTFE.P99, r.Latency.P50, r.Latency.P99, r.Conns, r.Errors)
	}
}
