package loadtest

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Sample is one measurement of a time series: source is a container or process, metric what was
// measured. Samples are written as CSV rows "unix_s,source,metric,value".
type Sample struct {
	At     time.Time
	Source string
	Metric string
	Value  float64
}

// SampleWriter writes samples as CSV.
type SampleWriter struct {
	w *csv.Writer
}

// NewSampleWriter writes the header and returns a writer of rows.
func NewSampleWriter(w io.Writer) (*SampleWriter, error) {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"unix_s", "source", "metric", "value"}); err != nil {
		return nil, err
	}
	cw.Flush()
	return &SampleWriter{w: cw}, cw.Error()
}

// AppendSampleWriter returns a writer of rows for a file that already has its header.
func AppendSampleWriter(w io.Writer) *SampleWriter {
	return &SampleWriter{w: csv.NewWriter(w)}
}

// Write writes samples and flushes them, so a run cut short keeps what it measured.
func (sw *SampleWriter) Write(samples ...Sample) error {
	for _, s := range samples {
		row := []string{
			strconv.FormatFloat(float64(s.At.UnixMilli())/1000, 'f', 3, 64),
			s.Source, s.Metric, strconv.FormatFloat(s.Value, 'f', -1, 64),
		}
		if err := sw.w.Write(row); err != nil {
			return err
		}
	}
	sw.w.Flush()
	return sw.w.Error()
}

// ReadSamples reads the CSV written by SampleWriter.
func ReadSamples(r io.Reader) ([]Sample, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("loadtest: read samples: %w", err)
	}
	var out []Sample
	for i, row := range rows {
		if i == 0 && len(row) > 0 && row[0] == "unix_s" {
			continue
		}
		if len(row) != 4 {
			return nil, fmt.Errorf("loadtest: samples line %d: want 4 fields, got %d", i+1, len(row))
		}
		at, err1 := strconv.ParseFloat(row[0], 64)
		v, err2 := strconv.ParseFloat(row[3], 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("loadtest: samples line %d: bad number", i+1)
		}
		out = append(out, Sample{At: time.UnixMilli(int64(math.Round(at * 1000))), Source: row[1], Metric: row[2], Value: v})
	}
	return out, nil
}

// ExpvarSamples extracts the process metrics of the example's /debug/vars.
func ExpvarSamples(at time.Time, source string, body []byte) ([]Sample, error) {
	var vars struct {
		Goroutines *float64 `json:"goroutines"`
		RSS        *float64 `json:"rss_bytes"`
		FDs        *float64 `json:"open_fds"`
		MemStats   struct {
			HeapAlloc, HeapInuse, HeapSys, Sys, NumGC float64
		} `json:"memstats"`
	}
	if err := json.Unmarshal(body, &vars); err != nil {
		return nil, fmt.Errorf("loadtest: decode expvar: %w", err)
	}
	if vars.Goroutines == nil || vars.RSS == nil || vars.FDs == nil {
		return nil, errors.New("loadtest: expvar has no goroutines, rss_bytes or open_fds (is it the example with DEBUG_ADDR?)")
	}
	m := vars.MemStats
	out := make([]Sample, 0, 8)
	for _, kv := range []struct {
		k string
		v float64
	}{
		{"goroutines", *vars.Goroutines},
		{"rss_bytes", *vars.RSS},
		{"open_fds", *vars.FDs},
		{"heap_alloc_bytes", m.HeapAlloc},
		{"heap_inuse_bytes", m.HeapInuse},
		{"heap_sys_bytes", m.HeapSys},
		{"sys_bytes", m.Sys},
		{"num_gc", m.NumGC},
	} {
		out = append(out, Sample{At: at, Source: source, Metric: kv.k, Value: kv.v})
	}
	return out, nil
}

// DockerStatsSamples parses the output of `docker stats --no-stream --format '{{json .}}'`: CPU
// percent and memory usage per container.
func DockerStatsSamples(at time.Time, out []byte) ([]Sample, error) {
	var samples []Sample
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var row struct{ Name, CPUPerc, MemUsage, PIDs string }
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("loadtest: docker stats line %q: %w", line, err)
		}
		cpu, err := strconv.ParseFloat(strings.TrimSuffix(row.CPUPerc, "%"), 64)
		if err != nil {
			return nil, fmt.Errorf("loadtest: docker stats cpu %q: %w", row.CPUPerc, err)
		}
		used, _, _ := strings.Cut(row.MemUsage, "/")
		mem, err := ParseBytes(strings.TrimSpace(used))
		if err != nil {
			return nil, err
		}
		samples = append(samples,
			Sample{At: at, Source: row.Name, Metric: "cpu_pct", Value: cpu},
			Sample{At: at, Source: row.Name, Metric: "mem_bytes", Value: mem})
		if pids, err := strconv.ParseFloat(row.PIDs, 64); err == nil {
			samples = append(samples, Sample{At: at, Source: row.Name, Metric: "pids", Value: pids})
		}
	}
	return samples, sc.Err()
}

// ParseBytes parses a docker size such as "12.5MiB", "3.1GB" or "512B".
func ParseBytes(s string) (float64, error) {
	units := []struct {
		suffix string
		mult   float64
	}{
		{"KiB", 1 << 10},
		{"MiB", 1 << 20},
		{"GiB", 1 << 30},
		{"TiB", 1 << 40},
		{"kB", 1e3},
		{"KB", 1e3},
		{"MB", 1e6},
		{"GB", 1e9},
		{"TB", 1e12},
		{"B", 1},
	}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
			if err != nil {
				break
			}
			return v * u.mult, nil
		}
	}
	return 0, fmt.Errorf("loadtest: size %q: want a number and a unit such as MiB", s)
}

// redisMetrics are the INFO fields RedisInfoSamples keeps.
var redisMetrics = []string{
	"instantaneous_ops_per_sec", "connected_clients", "blocked_clients", "used_memory",
	"total_commands_processed", "expired_keys", "evicted_keys",
}

// RedisInfoSamples parses `redis-cli INFO` and `redis-cli DBSIZE` output.
func RedisInfoSamples(at time.Time, source string, info, dbsize []byte) ([]Sample, error) {
	fields := map[string]string{}
	for line := range strings.Lines(string(info)) {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok {
			fields[k] = v
		}
	}
	out := make([]Sample, 0, len(redisMetrics)+1)
	for _, k := range redisMetrics {
		v, err := strconv.ParseFloat(fields[k], 64)
		if err != nil {
			return nil, fmt.Errorf("loadtest: redis INFO has no %s", k)
		}
		out = append(out, Sample{At: at, Source: source, Metric: k, Value: v})
	}
	keys, err := strconv.ParseFloat(strings.TrimSpace(string(dbsize)), 64)
	if err != nil {
		return nil, fmt.Errorf("loadtest: redis DBSIZE %q: %w", dbsize, err)
	}
	return append(out, Sample{At: at, Source: source, Metric: "keys", Value: keys}), nil
}

// Series selects the samples of one source and metric, in time order.
func Series(samples []Sample, source, metric string) []Sample {
	var out []Sample
	for _, s := range samples {
		if s.Source == source && s.Metric == metric {
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b Sample) int { return a.At.Compare(b.At) })
	return out
}

// Trend is the least-squares line through a series, and its range.
type Trend struct {
	N                   int
	First, Last         float64
	Min, Max, Mean      float64
	SlopePerHour        float64 // in the metric's unit per hour
	SlopePctOfMeanPerHr float64 // SlopePerHour as a percentage of Mean
	R2                  float64
}

// FitTrend fits value = a + b·hours by ordinary least squares.
func FitTrend(series []Sample) (Trend, error) {
	if len(series) < 3 {
		return Trend{}, errors.New("loadtest: a trend needs at least 3 samples")
	}
	t0 := series[0].At
	var sx, sy, sxx, sxy float64
	tr := Trend{N: len(series), First: series[0].Value, Last: series[len(series)-1].Value, Min: math.Inf(1), Max: math.Inf(-1)}
	for _, s := range series {
		x, y := s.At.Sub(t0).Hours(), s.Value
		sx, sy, sxx, sxy = sx+x, sy+y, sxx+x*x, sxy+x*y
		tr.Min, tr.Max = math.Min(tr.Min, y), math.Max(tr.Max, y)
	}
	n := float64(len(series))
	den := n*sxx - sx*sx
	if den == 0 {
		return Trend{}, errors.New("loadtest: a trend needs samples at different times")
	}
	tr.Mean = sy / n
	tr.SlopePerHour = (n*sxy - sx*sy) / den
	intercept := (sy - tr.SlopePerHour*sx) / n
	if tr.Mean != 0 {
		tr.SlopePctOfMeanPerHr = 100 * tr.SlopePerHour / tr.Mean
	}
	var ssRes, ssTot float64
	for _, s := range series {
		x, y := s.At.Sub(t0).Hours(), s.Value
		fit := intercept + tr.SlopePerHour*x
		ssRes += (y - fit) * (y - fit)
		ssTot += (y - tr.Mean) * (y - tr.Mean)
	}
	if ssTot > 0 {
		tr.R2 = 1 - ssRes/ssTot
	}
	return tr, nil
}
