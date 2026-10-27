package loadtest

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// subBits sets the precision of a Histogram: 2^subBits buckets per power of two, so a reported
// quantile is at most 1/64 (1.6 %) above the true value.
const subBits = 6

// numBuckets covers every non-negative int64 nanosecond value.
const numBuckets = (64 - subBits + 1) << subBits

// Histogram records durations in log-linear buckets. Record is safe for concurrent use without a
// lock; read quantiles once the writers are done.
type Histogram struct {
	counts [numBuckets]atomic.Uint64
	n      atomic.Uint64
	sum    atomic.Int64
	max    atomic.Int64
	min    atomic.Int64 // valid when n > 0; math.MaxInt64 before the first value
}

// NewHistogram returns an empty Histogram.
func NewHistogram() *Histogram {
	h := &Histogram{}
	h.min.Store(math.MaxInt64)
	return h
}

func bucketOf(v int64) int {
	if v < 1<<subBits {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1
	m := v >> (e - subBits) // in [64, 128)
	return (e-subBits+1)<<subBits + int(m-1<<subBits)
}

// bucketUpper returns the largest value that falls into bucket i.
func bucketUpper(i int) int64 {
	if i < 1<<subBits {
		return int64(i)
	}
	e := i>>subBits + subBits - 1
	m := int64(1<<subBits + i&(1<<subBits-1))
	if e >= 63 {
		return math.MaxInt64
	}
	return (m+1)<<(e-subBits) - 1
}

// Record adds one duration; negative values count as zero.
func (h *Histogram) Record(d time.Duration) {
	v := max(int64(d), 0)
	h.counts[bucketOf(v)].Add(1)
	h.n.Add(1)
	h.sum.Add(v)
	for {
		cur := h.max.Load()
		if v <= cur || h.max.CompareAndSwap(cur, v) {
			break
		}
	}
	for {
		cur := h.min.Load()
		if v >= cur || h.min.CompareAndSwap(cur, v) {
			break
		}
	}
}

// Count returns the number of recorded values.
func (h *Histogram) Count() uint64 { return h.n.Load() }

// Max returns the largest recorded value, 0 when empty.
func (h *Histogram) Max() time.Duration { return time.Duration(h.max.Load()) }

// Mean returns the mean of the recorded values, 0 when empty.
func (h *Histogram) Mean() time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(h.sum.Load() / int64(n)) //nolint:gosec // n is a count of recorded values
}

// Quantile returns the q-quantile (0 < q ≤ 1): the upper bound of the bucket holding the value of
// rank ceil(q·n), capped at the maximum. It is 0 when empty.
func (h *Histogram) Quantile(q float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(n)))
	rank = min(max(rank, 1), n)
	var seen uint64
	for i := range h.counts {
		seen += h.counts[i].Load()
		if seen >= rank {
			return time.Duration(min(bucketUpper(i), h.max.Load()))
		}
	}
	return h.Max()
}

// Merge adds the values of o to h.
func (h *Histogram) Merge(o *Histogram) {
	if o.n.Load() == 0 {
		return
	}
	for i := range o.counts {
		if c := o.counts[i].Load(); c > 0 {
			h.counts[i].Add(c)
		}
	}
	h.n.Add(o.n.Load())
	h.sum.Add(o.sum.Load())
	for {
		cur, v := h.max.Load(), o.max.Load()
		if v <= cur || h.max.CompareAndSwap(cur, v) {
			break
		}
	}
	for {
		cur, v := h.min.Load(), o.min.Load()
		if v >= cur || h.min.CompareAndSwap(cur, v) {
			break
		}
	}
}

// Summary is the usual percentiles of a Histogram, in milliseconds.
type Summary struct {
	Count uint64  `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
	Mean  float64 `json:"mean_ms"`
}

// Summarize returns the percentiles of h.
func (h *Histogram) Summarize() Summary {
	return Summary{
		Count: h.Count(),
		P50:   ms(h.Quantile(0.50)),
		P95:   ms(h.Quantile(0.95)),
		P99:   ms(h.Quantile(0.99)),
		Max:   ms(h.Max()),
		Mean:  ms(h.Mean()),
	}
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000 }
