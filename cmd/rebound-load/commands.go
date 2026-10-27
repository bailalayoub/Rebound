package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/bailalayoub/rebound/internal/loadtest"
)

func cmdRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cfg := loadtest.DefaultConfig()
	fs := newFlagSet("run", stderr)
	fs.StringVar(&cfg.BaseURL, "base-url", "http://localhost:8080", "server with POST /loadtest/streams and GET /streams/{id}")
	phases := fs.String("phases", "", "comma-separated STREAMSxREADERS[~RAMP]@HOLD, e.g. 50x1-3@3m,1250x2~10s@1m")
	fs.IntVar(&cfg.Stream.Events, "events", cfg.Stream.Events, "data events per stream")
	fs.IntVar(&cfg.Stream.DelayMS, "delay-ms", cfg.Stream.DelayMS, "pause between events, in ms")
	fs.IntVar(&cfg.Stream.Size, "size", cfg.Stream.Size, "pad each event payload to this many bytes")
	drop := fs.String("drop", "none", "none, conn:P, every:MIN-MAX (events) or ms:MIN-MAX")
	fs.Float64Var(&cfg.AuditRate, "audit", 0, "share of streams read again from the start and compared byte for byte")
	fs.DurationVar(&cfg.Window, "window", cfg.Window, "time-series window")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "seed of every random decision; 0 picks one (in the report)")
	fs.DurationVar(&cfg.Slack, "slack", cfg.Slack, "added to a stream's nominal duration for its readers' deadline")
	fs.DurationVar(&cfg.Drain, "drain", cfg.Drain, "wait at most this for the streams in flight after the last phase")
	fs.IntVar(&cfg.SlowStreams, "slow-streams", 0, "streams read by one reader that stops reading")
	slowEvents := fs.Int("slow-events", 2000, "data events of a slow stream")
	slowSize := fs.Int("slow-size", loadtest.MaxSize, "payload bytes of a slow stream's events")
	slowDelay := fs.Int("slow-delay-ms", 5, "pause between a slow stream's events, in ms")
	slowAfter := fs.Int("slow-after", 10, "events a slow reader reads before it stops")
	slowPause := fs.Duration("slow-pause", 60*time.Second, "how long a slow reader stops reading")
	out := fs.String("out", "", "write the JSON report to this file")
	quiet := fs.Bool("quiet", false, "no progress line per window")
	if err := parse(fs, args); err != nil {
		return err
	}
	var err error
	if *phases != "" {
		if cfg.Phases, err = loadtest.ParsePhases(*phases); err != nil {
			return err
		}
	}
	if cfg.Drop, err = loadtest.ParseDropPolicy(*drop); err != nil {
		return err
	}
	if cfg.SlowStreams > 0 {
		cfg.SlowStream = loadtest.StartRequest{Events: *slowEvents, DelayMS: *slowDelay, Size: *slowSize}
		cfg.Slow = &loadtest.SlowPolicy{AfterEvents: *slowAfter, Pause: *slowPause}
	}
	if !*quiet {
		cfg.Progress = stderr
	}
	rep, err := loadtest.Run(ctx, cfg)
	if err != nil {
		return err
	}
	if *out != "" {
		if err := writeFile(*out, rep.WriteJSON); err != nil {
			return err
		}
	}
	if err := rep.WriteTable(stdout); err != nil {
		return err
	}
	if !rep.Passed {
		return errFailed
	}
	return nil
}

func writeFile(path string, write func(io.Writer) error) (err error) {
	f, err := os.Create(path) //nolint:gosec // a path given on the command line
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	return write(f)
}

// openSamples opens path for appending samples, writing the header when it is new.
func openSamples(path string) (*loadtest.SampleWriter, func() error, error) {
	if path == "" {
		sw, err := loadtest.NewSampleWriter(os.Stdout)
		return sw, func() error { return nil }, err
	}
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644) //nolint:gosec // a path given on the command line
	if err != nil {
		return nil, nil, err
	}
	if statErr == nil {
		return loadtest.AppendSampleWriter(f), f.Close, nil
	}
	sw, err := loadtest.NewSampleWriter(f)
	if err != nil {
		_ = f.Close() // the header failed; that error is the one to report
		return nil, nil, err
	}
	return sw, f.Close, nil
}

// every calls sample now and then every interval until ctx is done or duration (if positive) has
// passed.
func every(ctx context.Context, interval, duration time.Duration, sample func(time.Time)) {
	if duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	sample(time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			sample(now)
		}
	}
}

func cmdSample(ctx context.Context, args []string, _, stderr io.Writer) error {
	fs := newFlagSet("sample", stderr)
	url := fs.String("url", "http://127.0.0.1:6060/debug/vars", "the example's expvar endpoint (DEBUG_ADDR)")
	source := fs.String("source", "app", "name of the sampled process in the CSV")
	interval := fs.Duration("every", time.Minute, "sampling interval")
	duration := fs.Duration("duration", 0, "stop after this long; 0 runs until interrupted")
	out := fs.String("out", "", "append CSV samples to this file (default stdout)")
	if err := parse(fs, args); err != nil {
		return err
	}
	sw, closeOut, err := openSamples(*out)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	every(ctx, *interval, *duration, func(now time.Time) {
		body, err := get(ctx, client, *url)
		if err == nil {
			var samples []loadtest.Sample
			if samples, err = loadtest.ExpvarSamples(now, *source, body); err == nil {
				err = sw.Write(samples...)
			}
		}
		if err != nil {
			diag(stderr, "rebound-load sample:", now.Format(time.RFC3339), err)
		}
	})
	return closeOut()
}

func get(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }() // read-only
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 8<<20))
}

func cmdHostSample(ctx context.Context, args []string, _, stderr io.Writer) error {
	fs := newFlagSet("host-sample", stderr)
	containers := fs.String("containers", "", "comma-separated containers for docker stats")
	redis := fs.String("redis", "", "Redis container for INFO and DBSIZE (optional)")
	interval := fs.Duration("every", 10*time.Second, "sampling interval")
	duration := fs.Duration("duration", 0, "stop after this long; 0 runs until interrupted")
	out := fs.String("out", "", "append CSV samples to this file (default stdout)")
	if err := parse(fs, args); err != nil {
		return err
	}
	names := splitList(*containers)
	if len(names) == 0 && *redis == "" {
		return errors.New("host-sample: -containers or -redis is required")
	}
	sw, closeOut, err := openSamples(*out)
	if err != nil {
		return err
	}
	every(ctx, *interval, *duration, func(now time.Time) {
		if len(names) > 0 {
			stats, err := docker(ctx, append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, names...)...)
			var samples []loadtest.Sample
			if err == nil {
				samples, err = loadtest.DockerStatsSamples(now, stats)
			}
			if err == nil {
				err = sw.Write(samples...)
			}
			if err != nil {
				diag(stderr, "rebound-load host-sample: docker stats:", now.Format(time.RFC3339), err)
			}
		}
		if *redis != "" {
			info, err := docker(ctx, "exec", *redis, "redis-cli", "INFO")
			var dbsize []byte
			if err == nil {
				dbsize, err = docker(ctx, "exec", *redis, "redis-cli", "DBSIZE")
			}
			var samples []loadtest.Sample
			if err == nil {
				samples, err = loadtest.RedisInfoSamples(now, "redis", info, dbsize)
			}
			if err == nil {
				err = sw.Write(samples...)
			}
			if err != nil {
				diag(stderr, "rebound-load host-sample: redis:", now.Format(time.RFC3339), err)
			}
		}
	})
	return closeOut()
}

// docker runs the docker CLI with a 30 s bound and returns its standard output.
func docker(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).Output() //nolint:gosec // fixed binary; arguments are container names from the command line
	if err != nil {
		return nil, fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}

func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// loadSeries reads every CSV in files and returns, per "source:metric", the samples between from
// and to (offsets from the earliest sample of all files; to <= 0 means the end).
func loadSeries(files, series string, from, to time.Duration) (keys []string, data map[string][]loadtest.Sample, err error) {
	var all []loadtest.Sample
	for _, path := range splitList(files) {
		f, err := os.Open(path) //nolint:gosec // a path given on the command line
		if err != nil {
			return nil, nil, err
		}
		samples, err := loadtest.ReadSamples(f)
		_ = f.Close() // read-only
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		all = append(all, samples...)
	}
	if len(all) == 0 {
		return nil, nil, errors.New("no samples")
	}
	start := slices.MinFunc(all, func(a, b loadtest.Sample) int { return a.At.Compare(b.At) }).At
	keys = splitList(series)
	if len(keys) == 0 {
		return nil, nil, errors.New("-series is required, e.g. app-1:rss_bytes")
	}
	out := map[string][]loadtest.Sample{}
	for _, key := range keys {
		source, metric, ok := strings.Cut(key, ":")
		if !ok {
			return nil, nil, fmt.Errorf("series %q: want source:metric", key)
		}
		for _, s := range loadtest.Series(all, source, metric) {
			off := s.At.Sub(start)
			if off >= from && (to <= 0 || off <= to) {
				out[key] = append(out[key], s)
			}
		}
	}
	return keys, out, nil
}

func cmdAnalyze(_ context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("analyze", stderr)
	in := fs.String("in", "", "comma-separated CSV sample files")
	series := fs.String("series", "", "comma-separated source:metric")
	from := fs.Duration("from", 0, "ignore samples before this offset (warm-up)")
	to := fs.Duration("to", 0, "ignore samples after this offset; 0: none")
	table := fs.Duration("table-every", 0, "also print one row of values every this long")
	if err := parse(fs, args); err != nil {
		return err
	}
	keys, data, err := loadSeries(*in, *series, *from, *to)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	// The tabwriter buffers every line: Flush returns the first write error.
	_, _ = fmt.Fprintln(tw, "series\tn\tfirst\tlast\tmin\tmax\tmean\tslope/h\tslope %mean/h\tR²")
	for _, key := range keys {
		tr, err := loadtest.FitTrend(data[key])
		if err != nil {
			_, _ = fmt.Fprintf(tw, "%s\t%d\t%v\n", key, len(data[key]), err)
			continue
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%.6g\t%.6g\t%.6g\t%.6g\t%.6g\t%.4g\t%.3f\t%.3f\n", key, tr.N,
			tr.First, tr.Last, tr.Min, tr.Max, tr.Mean, tr.SlopePerHour, tr.SlopePctOfMeanPerHr, tr.R2)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if *table > 0 {
		return writeTable(stdout, keys, data, *table)
	}
	return nil
}

// writeTable prints a Markdown table: one row per step, the last value of each series at or
// before that time.
func writeTable(w io.Writer, keys []string, data map[string][]loadtest.Sample, step time.Duration) error {
	var start, end time.Time
	for _, key := range keys {
		for _, s := range data[key] {
			if start.IsZero() || s.At.Before(start) {
				start = s.At
			}
			if s.At.After(end) {
				end = s.At
			}
		}
	}
	var b strings.Builder
	b.WriteString("| min | " + strings.Join(keys, " | ") + " |\n|---|" + strings.Repeat("---|", len(keys)) + "\n")
	for at := start; !at.After(end.Add(step - time.Nanosecond)); at = at.Add(step) {
		cut := at
		if cut.After(end) {
			cut = end
		}
		b.WriteString("| " + strconv.FormatFloat(cut.Sub(start).Minutes(), 'f', 0, 64))
		for _, key := range keys {
			v := "–"
			for _, s := range data[key] {
				if !s.At.After(cut) {
					v = strconv.FormatFloat(s.Value, 'f', -1, 64)
				}
			}
			b.WriteString(" | " + v)
		}
		b.WriteString(" |\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func cmdChart(_ context.Context, args []string, _, stderr io.Writer) error {
	fs := newFlagSet("chart", stderr)
	in := fs.String("in", "", "comma-separated CSV sample files")
	series := fs.String("series", "", "comma-separated source:metric, one line each")
	title := fs.String("title", "", "chart title")
	unit := fs.String("unit", "", "y axis label")
	scale := fs.Float64("scale", 1, "multiply values by this (1/1048576 for MiB)")
	out := fs.String("out", "", "SVG file to write")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("chart: -out is required")
	}
	keys, data, err := loadSeries(*in, *series, 0, 0)
	if err != nil {
		return err
	}
	lines := make([]loadtest.Line, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, loadtest.Line{Label: key, Points: data[key]})
	}
	return writeFile(*out, func(w io.Writer) error { return loadtest.WriteSVG(w, *title, *unit, *scale, lines) })
}
