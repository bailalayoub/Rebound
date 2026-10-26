package loadtest

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// Window holds what the readers measured during one interval of a run.
type Window struct {
	Start time.Time
	// TTFE is the time from sending the start request to a reader's first event. It is made of
	// StartLat (POST /loadtest/streams until its 201), then for each reader ConnectLat (GET until
	// the response headers), then the wait for the first event.
	TTFE, StartLat, ConnectLat *Histogram
	// Latency is the delay from the producer's write (the payload's T) to a reader receiving the
	// event, for events written while the connection was open; Replay is the same for events
	// written before the connection was opened (a resume replaying what it missed).
	Latency, Replay *Histogram

	events, conns, resumes   atomic.Int64
	started, startErrs, done atomic.Int64
	slowPaused               atomic.Int64

	mu     sync.Mutex
	errors map[string]int // "get:502", "start:transport"
	ended  map[string]int // connection endings

	// Set when the window closes.
	End            time.Time
	Target, Active int64
}

func newWindow(start time.Time) *Window {
	return &Window{
		Start: start, TTFE: NewHistogram(), StartLat: NewHistogram(), ConnectLat: NewHistogram(),
		Latency: NewHistogram(), Replay: NewHistogram(),
		errors: map[string]int{}, ended: map[string]int{},
	}
}

// Recorder routes measurements to the current window. A nil Recorder discards them (the audit
// reads use one).
type Recorder struct {
	cur atomic.Pointer[Window]
}

func newRecorder(now time.Time) *Recorder {
	r := &Recorder{}
	r.cur.Store(newWindow(now))
	return r
}

// rotate closes the current window at now and opens the next one.
func (r *Recorder) rotate(now time.Time, target, active int64) *Window {
	old := r.cur.Swap(newWindow(now))
	old.End, old.Target, old.Active = now, target, active
	return old
}

func (r *Recorder) window() *Window {
	if r == nil {
		return nil
	}
	return r.cur.Load()
}

func (r *Recorder) firstEvent(d time.Duration) {
	if w := r.window(); w != nil {
		w.TTFE.Record(d)
	}
}

func (r *Recorder) started(d time.Duration) {
	if w := r.window(); w != nil {
		w.StartLat.Record(d)
	}
}

func (r *Recorder) connected(d time.Duration) {
	if w := r.window(); w != nil {
		w.ConnectLat.Record(d)
	}
}

func (r *Recorder) event(now, written time.Time, live bool) {
	w := r.window()
	if w == nil {
		return
	}
	w.events.Add(1)
	if live {
		w.Latency.Record(now.Sub(written))
	} else {
		w.Replay.Record(now.Sub(written))
	}
}

func (r *Recorder) conn(log ConnLog, resume bool) {
	w := r.window()
	if w == nil {
		return
	}
	w.conns.Add(1)
	if resume {
		w.resumes.Add(1)
	}
	w.mu.Lock()
	w.ended[log.Ended]++
	w.mu.Unlock()
}

func (r *Recorder) endedBy(end string) {
	if w := r.window(); w != nil {
		w.mu.Lock()
		w.ended[end]++
		w.mu.Unlock()
	}
}

func (r *Recorder) httpError(op, class string) {
	if w := r.window(); w != nil {
		w.mu.Lock()
		w.errors[op+":"+class]++
		w.mu.Unlock()
	}
}

func (r *Recorder) streamStarted(ok bool) {
	if w := r.window(); w != nil {
		if ok {
			w.started.Add(1)
		} else {
			w.startErrs.Add(1)
		}
	}
}

func (r *Recorder) streamDone() {
	if w := r.window(); w != nil {
		w.done.Add(1)
	}
}

func (r *Recorder) slowPaused() {
	if w := r.window(); w != nil {
		w.slowPaused.Add(1)
	}
}

// WindowReport is one closed window, for time series.
type WindowReport struct {
	T              float64        `json:"t_s"` // window end, seconds since the run started
	Target         int64          `json:"target_streams"`
	Active         int64          `json:"active_streams"`
	StreamsStarted int64          `json:"streams_started"`
	StartErrors    int64          `json:"start_errors"`
	StreamsDone    int64          `json:"streams_done"`
	Events         int64          `json:"events"`
	EventsPerS     float64        `json:"events_per_s"`
	Conns          int64          `json:"conns"`
	Resumes        int64          `json:"resumes"`
	TTFE           Summary        `json:"ttfe"`
	StartLat       Summary        `json:"start_latency"`
	ConnectLat     Summary        `json:"connect_latency"`
	Latency        Summary        `json:"latency"`
	Replay         Summary        `json:"replay_latency"`
	Errors         map[string]int `json:"errors,omitempty"`
	Ended          map[string]int `json:"ended,omitempty"`
}

func (w *Window) report(runStart time.Time) WindowReport {
	secs := w.End.Sub(w.Start).Seconds()
	w.mu.Lock()
	defer w.mu.Unlock()
	rep := WindowReport{
		T:              round3(w.End.Sub(runStart).Seconds()),
		Target:         w.Target,
		Active:         w.Active,
		StreamsStarted: w.started.Load(),
		StartErrors:    w.startErrs.Load(),
		StreamsDone:    w.done.Load(),
		Events:         w.events.Load(),
		Conns:          w.conns.Load(),
		Resumes:        w.resumes.Load(),
		TTFE:           w.TTFE.Summarize(),
		StartLat:       w.StartLat.Summarize(),
		ConnectLat:     w.ConnectLat.Summarize(),
		Latency:        w.Latency.Summarize(),
		Replay:         w.Replay.Summarize(),
		Errors:         maps.Clone(w.errors),
		Ended:          maps.Clone(w.ended),
	}
	if secs > 0 {
		rep.EventsPerS = round3(float64(rep.Events) / secs)
	}
	return rep
}

func round3(f float64) float64 { return float64(int64(f*1000+0.5)) / 1000 }
