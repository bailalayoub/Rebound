package loadtest

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

func TestSampleWriter_RoundTrip(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	sw, err := NewSampleWriter(&buf)
	require.NoError(t, err)
	in := []Sample{{At: t0, Source: "app-1", Metric: "rss_bytes", Value: 123456}, {At: t0.Add(1500 * time.Millisecond), Source: "redis", Metric: "keys", Value: 2}}
	require.NoError(t, sw.Write(in...))
	assert.True(t, strings.HasPrefix(buf.String(), "unix_s,source,metric,value\n"))
	out, err := ReadSamples(&buf)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.True(t, in[1].At.Equal(out[1].At))
	assert.Equal(t, in[1].Value, out[1].Value)
	_, err = ReadSamples(strings.NewReader("1,a,b\n"))
	assert.Error(t, err)
}

func TestExpvarSamples(t *testing.T) {
	t.Parallel()
	got, err := ExpvarSamples(t0, "app-1", []byte(`{"goroutines":42,"rss_bytes":1000,"open_fds":12,"memstats":{"HeapAlloc":5,"HeapInuse":6,"HeapSys":7,"Sys":8,"NumGC":9}}`))
	require.NoError(t, err)
	byMetric := map[string]float64{}
	for _, s := range got {
		byMetric[s.Metric] = s.Value
	}
	assert.Equal(t, map[string]float64{
		"goroutines": 42, "rss_bytes": 1000, "open_fds": 12, "heap_alloc_bytes": 5,
		"heap_inuse_bytes": 6, "heap_sys_bytes": 7, "sys_bytes": 8, "num_gc": 9,
	}, byMetric)
	_, err = ExpvarSamples(t0, "x", []byte(`{"memstats":{}}`))
	assert.Error(t, err, "not the example's expvar")
}

func TestDockerStatsSamples(t *testing.T) {
	t.Parallel()
	out := `{"BlockIO":"0B / 0B","CPUPerc":"12.50%","Container":"abc","ID":"abc","MemPerc":"0.30%","MemUsage":"24.5MiB / 7.448GiB","Name":"deploy-app-1-1","NetIO":"1kB / 2kB","PIDs":"14"}
{"CPUPerc":"0.00%","MemUsage":"3.1MB / 7.448GiB","Name":"deploy-redis-1","PIDs":"6"}
`
	got, err := DockerStatsSamples(t0, []byte(out))
	require.NoError(t, err)
	require.Len(t, got, 6)
	assert.Equal(t, Sample{At: t0, Source: "deploy-app-1-1", Metric: "cpu_pct", Value: 12.5}, got[0])
	assert.Equal(t, 24.5*(1<<20), got[1].Value)
	assert.Equal(t, 3.1e6, got[4].Value)
	_, err = DockerStatsSamples(t0, []byte(`{"CPUPerc":"x%","MemUsage":"1B / 1B","Name":"n"}`))
	assert.Error(t, err)
}

func TestParseBytes(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]float64{"512B": 512, "1KiB": 1024, "1.5GiB": 1.5 * (1 << 30), "2kB": 2000, "0B": 0} {
		got, err := ParseBytes(in)
		require.NoError(t, err, in)
		assert.InDelta(t, want, got, 0.001, in)
	}
	for _, bad := range []string{"", "12", "MiB", "x MiB"} {
		_, err := ParseBytes(bad)
		assert.Error(t, err, bad)
	}
}

func TestRedisInfoSamples(t *testing.T) {
	t.Parallel()
	info := "# Clients\r\nconnected_clients:12\r\nblocked_clients:3\r\n# Memory\r\nused_memory:1048576\r\n" +
		"# Stats\r\ntotal_commands_processed:999\r\ninstantaneous_ops_per_sec:450\r\nexpired_keys:7\r\nevicted_keys:0\r\n"
	got, err := RedisInfoSamples(t0, "redis", []byte(info), []byte("42\n"))
	require.NoError(t, err)
	byMetric := map[string]float64{}
	for _, s := range got {
		byMetric[s.Metric] = s.Value
	}
	assert.Equal(t, 450.0, byMetric["instantaneous_ops_per_sec"])
	assert.Equal(t, 12.0, byMetric["connected_clients"])
	assert.Equal(t, 42.0, byMetric["keys"])
	_, err = RedisInfoSamples(t0, "redis", []byte("connected_clients:1\n"), []byte("1"))
	assert.Error(t, err)
}

func TestFitTrend(t *testing.T) {
	t.Parallel()
	series := make([]Sample, 0, 61)
	for i := range 61 {
		series = append(series, Sample{At: t0.Add(time.Duration(i) * time.Minute), Value: 100 + 2*float64(i)}) // +120 per hour
	}
	tr, err := FitTrend(series)
	require.NoError(t, err)
	assert.InDelta(t, 120, tr.SlopePerHour, 1e-9)
	assert.InDelta(t, 1, tr.R2, 1e-9)
	assert.Equal(t, 100.0, tr.Min)
	assert.Equal(t, 220.0, tr.Max)
	assert.InDelta(t, 100*120/160.0, tr.SlopePctOfMeanPerHr, 1e-9)

	flat := []Sample{{At: t0, Value: 5}, {At: t0.Add(time.Minute), Value: 5}, {At: t0.Add(2 * time.Minute), Value: 5}}
	tr, err = FitTrend(flat)
	require.NoError(t, err)
	assert.Zero(t, tr.SlopePerHour)
	_, err = FitTrend(flat[:2])
	assert.Error(t, err)
	_, err = FitTrend([]Sample{{At: t0}, {At: t0}, {At: t0}})
	assert.Error(t, err)
}

func TestSeriesAndSVG(t *testing.T) {
	t.Parallel()
	samples := []Sample{
		{At: t0.Add(time.Minute), Source: "a", Metric: "m", Value: 2},
		{At: t0, Source: "a", Metric: "m", Value: 1},
		{At: t0, Source: "b", Metric: "m", Value: 9},
	}
	s := Series(samples, "a", "m")
	require.Len(t, s, 2)
	assert.Equal(t, 1.0, s[0].Value)
	var buf bytes.Buffer
	require.NoError(t, WriteSVG(&buf, "RSS <app>", "MiB", 1, []Line{{Label: "a", Points: s}, {Label: "b", Points: Series(samples, "b", "m")}}))
	svg := buf.String()
	assert.True(t, strings.HasPrefix(svg, "<svg "))
	assert.Contains(t, svg, "RSS &lt;app&gt;")
	assert.Equal(t, 2, strings.Count(svg, "<polyline"))
	assert.Error(t, WriteSVG(&buf, "x", "y", 1, nil))
}
