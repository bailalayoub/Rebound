package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bailalayoub/rebound/internal/loadtest"
)

// column is one cell of the steps table: an aggregate of one series over a phase.
type column struct {
	label, source, metric, agg string
	scale                      float64
}

// parseColumns parses "LABEL=source:metric:agg[*scale]" items, agg being mean, max or min.
func parseColumns(spec string) ([]column, error) {
	var out []column
	for _, item := range splitList(spec) {
		label, rest, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("column %q: want LABEL=source:metric:agg[*scale]", item)
		}
		rest, scaleText, hasScale := strings.Cut(rest, "*")
		parts := strings.Split(rest, ":")
		if len(parts) != 3 || (parts[2] != "mean" && parts[2] != "max" && parts[2] != "min") {
			return nil, fmt.Errorf("column %q: want source:metric:mean|max|min", item)
		}
		c := column{label: label, source: parts[0], metric: parts[1], agg: parts[2], scale: 1}
		if hasScale {
			s, err := strconv.ParseFloat(scaleText, 64)
			if err != nil {
				return nil, fmt.Errorf("column %q: scale: %w", item, err)
			}
			c.scale = s
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errors.New("-columns is required")
	}
	return out, nil
}

func aggregate(values []float64, agg string) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	r := values[0]
	sum := 0.0
	for _, v := range values {
		sum += v
		switch agg {
		case "max":
			r = math.Max(r, v)
		case "min":
			r = math.Min(r, v)
		}
	}
	if agg == "mean" {
		return sum / float64(len(values))
	}
	return r
}

// cmdSteps prints one Markdown row per phase of a run report, aggregating the samples taken
// during the phase (after a settling delay, so that a step's ramp-up does not count).
func cmdSteps(_ context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("steps", stderr)
	reportPath := fs.String("report", "", "the JSON report of a run")
	in := fs.String("in", "", "comma-separated CSV sample files of that run")
	columns := fs.String("columns", "", "comma-separated LABEL=source:metric:mean|max|min[*scale]")
	settle := fs.Duration("settle", 30*time.Second, "ignore the first part of each phase")
	if err := parse(fs, args); err != nil {
		return err
	}
	cols, err := parseColumns(*columns)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(*reportPath)
	if err != nil {
		return err
	}
	var rep loadtest.Report
	if err := json.Unmarshal(data, &rep); err != nil {
		return fmt.Errorf("%s: %w", *reportPath, err)
	}
	var samples []loadtest.Sample
	for _, path := range splitList(*in) {
		f, err := os.Open(path) //nolint:gosec // a path given on the command line
		if err != nil {
			return err
		}
		s, err := loadtest.ReadSamples(f)
		_ = f.Close() // read-only
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		samples = append(samples, s...)
	}
	var b strings.Builder
	b.WriteString("| phase |")
	for _, c := range cols {
		b.WriteString(" " + c.label + " |")
	}
	b.WriteString("\n|---|" + strings.Repeat("---|", len(cols)) + "\n")
	for i := range rep.Phases {
		p := &rep.Phases[i]
		from := rep.Started.Add(time.Duration(p.From*float64(time.Second)) + *settle)
		to := rep.Started.Add(time.Duration(p.To * float64(time.Second)))
		b.WriteString("| " + p.Name + " |")
		for _, c := range cols {
			var values []float64
			for _, s := range loadtest.Series(samples, c.source, c.metric) {
				if !s.At.Before(from) && s.At.Before(to) {
					values = append(values, s.Value*c.scale)
				}
			}
			if v := aggregate(values, c.agg); math.IsNaN(v) {
				b.WriteString(" – |")
			} else {
				b.WriteString(" " + strconv.FormatFloat(v, 'f', 0, 64) + " |")
			}
		}
		b.WriteString("\n")
	}
	_, err = io.WriteString(stdout, b.String())
	return err
}
