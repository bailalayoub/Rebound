package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"
)

// maxFailureDetails bounds the failures kept per scenario in a report.
const maxFailureDetails = 5

// Report is the outcome of a tester run.
type Report struct {
	BaseURL   string           `json:"base_url"`
	Seed      uint64           `json:"seed"`
	Passed    bool             `json:"passed"`
	Scenarios []ScenarioReport `json:"scenarios"`
}

// ScenarioReport aggregates the runs of one scenario.
type ScenarioReport struct {
	ID              string      `json:"id"`
	Name            string      `json:"name"`
	Runs            int         `json:"runs"`
	Failures        int         `json:"failures"`
	Reconnections   int         `json:"reconnections"`
	TTFEP50ms       float64     `json:"ttfe_p50_ms"`
	TTFEP95ms       float64     `json:"ttfe_p95_ms"`
	TerminalP95ms   float64     `json:"terminal_p95_ms"`
	FailureDetails  []Failure   `json:"failure_details,omitempty"`
	ConnectionTrace [][]ConnLog `json:"connection_trace,omitempty"` // per run, with Config.Trace
}

// Failure describes one failed run: which stream, what went wrong, and the connections made.
type Failure struct {
	Run         int       `json:"run"`
	StreamID    string    `json:"stream_id"`
	Error       string    `json:"error"`
	Connections []ConnLog `json:"connections"`
}

// ConnLog is one connection of a reader: the cursor it resumed from, how many events it
// received, and how it ended.
type ConnLog struct {
	Cursor uint64 `json:"cursor"`
	Events int    `json:"events"`
	Ended  string `json:"ended"`
}

func summarize(sc scenario, results []runResult, trace bool) ScenarioReport {
	rep := ScenarioReport{ID: sc.id, Name: sc.name, Runs: len(results)}
	var ttfe, ttt []time.Duration
	for i, res := range results {
		rep.Reconnections += res.reconnections
		ttfe = append(ttfe, res.ttfe...)
		ttt = append(ttt, res.ttt...)
		if trace {
			rep.ConnectionTrace = append(rep.ConnectionTrace, res.trace)
		}
		if res.failure != nil {
			rep.Failures++
			if len(rep.FailureDetails) < maxFailureDetails {
				f := *res.failure
				f.Run = i
				rep.FailureDetails = append(rep.FailureDetails, f)
			}
		}
	}
	rep.TTFEP50ms = millis(percentile(ttfe, 50))
	rep.TTFEP95ms = millis(percentile(ttfe, 95))
	rep.TerminalP95ms = millis(percentile(ttt, 95))
	return rep
}

// percentile returns the nearest-rank percentile p of ds, or 0 when ds is empty.
func percentile(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := slices.Clone(ds)
	slices.Sort(sorted)
	rank := (p*len(sorted) + 99) / 100
	return sorted[max(rank, 1)-1]
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// WriteJSON writes the report as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteTable writes a human-readable summary and the details of every failure.
func (r *Report) WriteTable(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "Rebound conformance — %s — seed %d\n\n", r.BaseURL, r.Seed)
	var rows strings.Builder
	rows.WriteString("ID\tSCENARIO\tRUNS\tFAILURES\tRECONNECTIONS\tTTFE p50\tTTFE p95\tTERMINAL p95\t\n")
	passed := 0
	for i := range r.Scenarios {
		s := &r.Scenarios[i]
		if s.Failures == 0 {
			passed++
		}
		fmt.Fprintf(&rows, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t\n", s.ID, s.Name, s.Runs, s.Failures, s.Reconnections,
			fmtMS(s.TTFEP50ms), fmtMS(s.TTFEP95ms), fmtMS(s.TerminalP95ms))
	}
	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	if _, err := io.WriteString(tw, rows.String()); err != nil {
		return err
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for i := range r.Scenarios {
		s := &r.Scenarios[i]
		for _, f := range s.FailureDetails {
			fmt.Fprintf(&b, "\nFAIL %s run %d, stream %s: %s\n", s.ID, f.Run, f.StreamID, f.Error)
			for i, c := range f.Connections {
				fmt.Fprintf(&b, "  connection %d: after %d → %d events → %s\n", i+1, c.Cursor, c.Events, c.Ended)
			}
		}
	}
	verdict := "PASS"
	if !r.Passed {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "\n%s: %d/%d scenarios passed (reproduce with --seed %d)\n", verdict, passed, len(r.Scenarios), r.Seed)
	_, err := io.WriteString(w, b.String())
	return err
}

func fmtMS(ms float64) string {
	switch {
	case ms == 0:
		return "-"
	case ms >= 1000:
		return fmt.Sprintf("%.2f s", ms/1000)
	default:
		return fmt.Sprintf("%.1f ms", ms)
	}
}
