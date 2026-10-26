package loadtest

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// phaseStats aggregates the streams started during one phase.
type phaseStats struct {
	name string

	mu               sync.Mutex
	started          int
	startErrors      int
	readers          int
	conns            int
	integrity        map[string]int
	terminals        map[string]int // per stream: the terminal its readers agreed on
	audited          int
	auditUnavailable int
	released         int
	producers        map[string]int            // streams per producing instance
	served           map[string]map[string]int // producing instance -> X-Upstream -> connections
	failures         []StreamFailure
}

func newPhaseStats(name string) *phaseStats {
	return &phaseStats{
		name: name, integrity: map[string]int{}, terminals: map[string]int{},
		producers: map[string]int{}, served: map[string]map[string]int{},
	}
}

func (ps *phaseStats) startFailed() {
	ps.mu.Lock()
	ps.startErrors++
	ps.mu.Unlock()
}

// StreamFailure details one stream whose readers saw a violation.
type StreamFailure struct {
	StreamID string         `json:"stream_id"`
	Problems []string       `json:"problems"`
	Readers  []ReaderDetail `json:"readers"`
}

// ReaderDetail is one reader of a failed stream.
type ReaderDetail struct {
	Tally       Tally     `json:"tally"`
	Connections []ConnLog `json:"connections"` // the first and last few
}

// record checks the readers of one stream against each other and against the audit read.
func (ps *phaseStats) record(id string, want int, rds []*reader, audit *reader, keep int) {
	counts := map[string]int{}
	var problems []string
	for i, rd := range rds {
		rd.tally.Counts(counts, want)
		for _, p := range rd.tally.Problems(want) {
			problems = append(problems, fmt.Sprintf("reader %d: %s", i, p))
		}
		if i > 0 && rd.tally.Terminals > 0 && rds[0].tally.Terminals > 0 &&
			(rd.tally.TerminalCode != rds[0].tally.TerminalCode || rd.tally.TerminalSeq != rds[0].tally.TerminalSeq) {
			counts[ClassTerminalDiffer]++
			problems = append(problems, fmt.Sprintf("reader %d: terminal %s at %d, reader 0 saw %s at %d", i,
				rd.tally.TerminalCode, rd.tally.TerminalSeq, rds[0].tally.TerminalCode, rds[0].tally.TerminalSeq))
		}
	}
	auditOK := audit != nil && audit.tally.Terminals > 0
	if auditOK {
		for i, rd := range rds {
			if !rd.tally.IsPrefixOf(&audit.tally) {
				counts[ClassNotPrefix]++
				problems = append(problems, fmt.Sprintf("reader %d: received events are not a prefix of the log", i))
			}
		}
		for _, p := range audit.tally.Integrity() {
			problems = append(problems, "audit: "+p)
		}
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.started++
	ps.readers += len(rds)
	for k, v := range counts {
		ps.integrity[k] += v
	}
	terminal := "none"
	if rds[0].tally.Terminals > 0 {
		terminal = rds[0].tally.TerminalCode
	}
	ps.terminals[terminal]++
	if audit != nil {
		ps.audited++
		if !auditOK {
			ps.auditUnavailable++
		}
	}
	host := rds[0].tally.Host
	if host == "" {
		host = "?"
	}
	ps.producers[host]++
	if ps.served[host] == nil {
		ps.served[host] = map[string]int{}
	}
	for _, rd := range rds {
		ps.conns += rd.conns
		if rd.released {
			ps.released++
		}
		for up, n := range rd.upstreams {
			ps.served[host][up] += n
		}
	}
	if len(problems) > 0 && len(ps.failures) < keep {
		f := StreamFailure{StreamID: id, Problems: problems}
		for _, rd := range rds {
			f.Readers = append(f.Readers, ReaderDetail{Tally: rd.tally, Connections: trimLogs(rd.logs)})
		}
		ps.failures = append(ps.failures, f)
	}
}

// trimLogs keeps the first 5 and the last 5 connections.
func trimLogs(logs []ConnLog) []ConnLog {
	if len(logs) <= 10 {
		return logs
	}
	return append(slices.Clip(logs[:5]), logs[len(logs)-5:]...)
}

// Report is the outcome of a run.
type Report struct {
	BaseURL     string         `json:"base_url"`
	Seed        uint64         `json:"seed"`
	Started     time.Time      `json:"started"`
	Stream      StartRequest   `json:"stream"`
	Drop        string         `json:"drop"`
	AuditRate   float64        `json:"audit_rate"`
	TimerBefore float64        `json:"timer_10ms_before_ms"`
	TimerAfter  float64        `json:"timer_10ms_after_ms"`
	Phases      []PhaseReport  `json:"phases"`
	Slow        *PhaseReport   `json:"slow,omitempty"`
	Windows     []WindowReport `json:"windows"`
	// Unfinished is the number of streams still in flight when the drain timed out.
	Unfinished int64 `json:"unfinished"`
	Passed     bool  `json:"passed"`
}

// PhaseReport is one phase: the windows it spans, and the streams started in it.
type PhaseReport struct {
	Name             string                    `json:"name"`
	From             float64                   `json:"from_s"`
	To               float64                   `json:"to_s"`
	StreamsStarted   int                       `json:"streams_started"`
	StartErrors      int                       `json:"start_errors"`
	Readers          int                       `json:"readers"`
	Conns            int                       `json:"conns"`
	EventsReceived   int64                     `json:"events_received"`
	EventsPerS       float64                   `json:"events_per_s"`
	TTFE             Summary                   `json:"ttfe"`
	StartLat         Summary                   `json:"start_latency"`
	ConnectLat       Summary                   `json:"connect_latency"`
	Latency          Summary                   `json:"latency"`
	Replay           Summary                   `json:"replay_latency"`
	HTTPErrors       map[string]int            `json:"http_errors"`
	Ended            map[string]int            `json:"conn_endings"`
	Integrity        map[string]int            `json:"integrity"`
	Terminals        map[string]int            `json:"terminals"`
	Audited          int                       `json:"audited"`
	AuditUnavailable int                       `json:"audit_unavailable"`
	SlowReleased     int                       `json:"slow_released,omitempty"`
	Producers        map[string]int            `json:"producers"`
	Served           map[string]map[string]int `json:"served_by_upstream"`
	Failures         []StreamFailure           `json:"failures,omitempty"`
	Passed           bool                      `json:"passed"`
}

func (d *driver) report(boundaries []time.Time) *Report {
	d.mu.Lock()
	windows := slices.Clone(d.windows)
	d.mu.Unlock()
	rep := &Report{
		BaseURL: d.cfg.BaseURL, Seed: d.cfg.Seed, Started: d.start, Stream: d.cfg.Stream,
		Drop: d.cfg.Drop.String(), AuditRate: d.cfg.AuditRate, Unfinished: d.active.Load(), Passed: true,
	}
	for _, w := range windows {
		rep.Windows = append(rep.Windows, w.report(d.start))
	}
	for i, ps := range d.phases {
		from, to := boundaries[i], boundaries[i+1]
		if i == len(d.phases)-1 {
			to = time.Now() // the last phase owns the drain
		}
		pr := ps.report(d.start, from, to, windows)
		rep.Passed = rep.Passed && pr.Passed
		rep.Phases = append(rep.Phases, pr)
	}
	if d.cfg.SlowStreams > 0 {
		end := time.Now()
		pr := d.slow.report(d.start, d.start, end, nil)
		rep.Slow = &pr
		rep.Passed = rep.Passed && pr.Passed
	}
	rep.Passed = rep.Passed && rep.Unfinished == 0
	return rep
}

// report summarizes ps, with the windows that ended within [from, to] (none when windows is nil).
func (ps *phaseStats) report(runStart, from, to time.Time, windows []*Window) PhaseReport {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	pr := PhaseReport{
		Name: ps.name, From: round3(from.Sub(runStart).Seconds()), To: round3(to.Sub(runStart).Seconds()),
		StreamsStarted: ps.started, StartErrors: ps.startErrors, Readers: ps.readers, Conns: ps.conns,
		HTTPErrors: map[string]int{}, Ended: map[string]int{},
		Integrity: maps.Clone(ps.integrity), Terminals: maps.Clone(ps.terminals),
		Audited: ps.audited, AuditUnavailable: ps.auditUnavailable, SlowReleased: ps.released,
		Producers: maps.Clone(ps.producers), Served: map[string]map[string]int{}, Failures: ps.failures,
	}
	for h, m := range ps.served {
		pr.Served[h] = maps.Clone(m)
	}
	ttfe, lat, replay, start, connect := NewHistogram(), NewHistogram(), NewHistogram(), NewHistogram(), NewHistogram()
	var secs float64
	for _, w := range windows {
		if w.End.Before(from) || w.End.After(to.Add(time.Millisecond)) || !w.End.After(from) {
			continue
		}
		ttfe.Merge(w.TTFE)
		start.Merge(w.StartLat)
		connect.Merge(w.ConnectLat)
		lat.Merge(w.Latency)
		replay.Merge(w.Replay)
		pr.EventsReceived += w.events.Load()
		secs += w.End.Sub(w.Start).Seconds()
		w.mu.Lock()
		for k, v := range w.errors {
			pr.HTTPErrors[k] += v
		}
		for k, v := range w.ended {
			pr.Ended[k] += v
		}
		w.mu.Unlock()
	}
	pr.TTFE, pr.Latency, pr.Replay = ttfe.Summarize(), lat.Summarize(), replay.Summarize()
	pr.StartLat, pr.ConnectLat = start.Summarize(), connect.Summarize()
	if secs > 0 {
		pr.EventsPerS = round3(float64(pr.EventsReceived) / secs)
	}
	pr.Passed = pr.Terminals["none"] == 0
	for _, v := range pr.Integrity {
		if v > 0 {
			pr.Passed = false
		}
	}
	return pr
}

// WriteJSON writes the report as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteTable writes one line per phase.
func (r *Report) WriteTable(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	// The tabwriter buffers every line: Flush returns the first write error.
	_, _ = fmt.Fprintf(tw, "seed %d, timer 10ms: %.2f ms before, %.2f ms after, unfinished %d, passed %v\n",
		r.Seed, r.TimerBefore, r.TimerAfter, r.Unfinished, r.Passed)
	_, _ = fmt.Fprintln(tw, "phase\tstreams\tstart err\treaders\tconns\tev/s\tttfe p50/p95/p99 ms\tstart p50/p99\tconnect p50/p99\tlat p50/p95/p99 ms\thttp errors\tintegrity\tterminals\tpass")
	phases := r.Phases
	if r.Slow != nil {
		phases = append(slices.Clip(phases), *r.Slow)
	}
	for i := range phases {
		p := &phases[i]
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%.0f\t%.1f/%.1f/%.1f\t%.1f/%.1f\t%.1f/%.1f\t%.1f/%.1f/%.1f\t%s\t%s\t%s\t%v\n",
			p.Name, p.StreamsStarted, p.StartErrors, p.Readers, p.Conns, p.EventsPerS,
			p.TTFE.P50, p.TTFE.P95, p.TTFE.P99, p.StartLat.P50, p.StartLat.P99, p.ConnectLat.P50, p.ConnectLat.P99, p.Latency.P50, p.Latency.P95, p.Latency.P99,
			formatCounts(p.HTTPErrors), formatCounts(p.Integrity), formatCounts(p.Terminals), p.Passed)
	}
	return tw.Flush()
}

func formatCounts(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	keys := slices.Collect(maps.Keys(m))
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}
