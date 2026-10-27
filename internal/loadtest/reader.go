package loadtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/internal/sseclient"
)

// DropPolicy decides when a reader drops its connection and reconnects with Last-Event-ID.
type DropPolicy struct {
	// Kind is "none", "conn" (each connection is dropped with probability P after a random
	// number of the remaining events), "every" (after Min..Max events) or "ms" (after Min..Max ms).
	Kind     string
	P        float64
	Min, Max int
}

// ParseDropPolicy parses "none", "conn:0.3", "every:1-5" or "ms:5-50".
func ParseDropPolicy(s string) (DropPolicy, error) {
	kind, arg, _ := strings.Cut(strings.TrimSpace(s), ":")
	switch kind {
	case "", "none":
		return DropPolicy{Kind: "none"}, nil
	case "conn":
		p, err := strconv.ParseFloat(arg, 64)
		if err != nil || p < 0 || p > 1 {
			return DropPolicy{}, fmt.Errorf("loadtest: drop %q: want conn:P with P in [0, 1]", s)
		}
		return DropPolicy{Kind: kind, P: p}, nil
	case "every", "ms":
		lo, hi, ok := strings.Cut(arg, "-")
		if !ok {
			hi = lo
		}
		a, errA := strconv.Atoi(lo)
		b, errB := strconv.Atoi(hi)
		if errA != nil || errB != nil || a < 1 || b < a {
			return DropPolicy{}, fmt.Errorf("loadtest: drop %q: want %s:MIN-MAX with 1 <= MIN <= MAX", s, kind)
		}
		return DropPolicy{Kind: kind, Min: a, Max: b}, nil
	default:
		return DropPolicy{}, fmt.Errorf("loadtest: drop %q: want none, conn:P, every:MIN-MAX or ms:MIN-MAX", s)
	}
}

// String returns the policy in the syntax of ParseDropPolicy.
func (p DropPolicy) String() string {
	switch p.Kind {
	case "conn":
		return "conn:" + strconv.FormatFloat(p.P, 'g', -1, 64)
	case "every", "ms":
		return fmt.Sprintf("%s:%d-%d", p.Kind, p.Min, p.Max)
	default:
		return "none"
	}
}

// plan returns after how many events (0: never) and after how long (0: never) the next
// connection is dropped, given the events still to come.
func (p DropPolicy) plan(rng *mrand.Rand, remaining int) (events int, after time.Duration) {
	switch p.Kind {
	case "conn":
		if remaining > 0 && rng.Float64() < p.P {
			return 1 + rng.IntN(remaining), 0
		}
	case "every":
		return p.Min + rng.IntN(p.Max-p.Min+1), 0
	case "ms":
		return 0, time.Duration(p.Min+rng.IntN(p.Max-p.Min+1)) * time.Millisecond
	}
	return 0, 0
}

// SlowPolicy makes a reader stop reading, without closing its connection, after AfterEvents
// events, for Pause. It then reads what is left, and resumes with Last-Event-ID if the server
// closed the connection meanwhile.
type SlowPolicy struct {
	AfterEvents int
	Pause       time.Duration
}

// ConnLog is one connection of a reader.
type ConnLog struct {
	Cursor   uint64 `json:"cursor"`
	Status   int    `json:"status,omitempty"`
	Upstream string `json:"upstream,omitempty"`
	Events   int    `json:"events"`
	Ended    string `json:"ended"`
}

// Connection endings, the values of ConnLog.Ended.
const (
	EndTerminal  = "terminal"
	EndDropped   = "dropped"      // this reader dropped it, per its DropPolicy
	EndServerEOF = "server_eof"   // the body ended without the terminal event
	EndReadError = "read_error"   // the body failed (reset, timeout)
	EndTransport = "transport"    // no response at all
	EndStatus    = "status"       // a status other than 200
	EndReleased  = "slow_release" // a slow reader found its connection closed by the server
	EndDeadline  = "deadline"     // the reader gave up
)

// reader follows one stream to its terminal event across as many connections as it takes.
type reader struct {
	client   *http.Client
	baseURL  string
	id       string
	events   int // data events the producer writes, for DropPolicy "conn"
	drop     DropPolicy
	slow     *SlowPolicy
	rng      *mrand.Rand
	deadline time.Time
	maxConns int
	stats    *Recorder
	started  time.Time // when the stream's start request was sent, for time to first event
	keepLogs bool

	tally     Tally
	conns     int
	logs      []ConnLog
	upstreams map[string]int // connections per X-Upstream, the first one included
	released  bool           // a slow pause ended with the connection closed by the server
	firstSeen bool
}

// run reads until the terminal event, a final HTTP status, or the deadline.
func (rd *reader) run(ctx context.Context) {
	ctx, cancel := context.WithDeadline(ctx, rd.deadline)
	defer cancel()
	backoff := 50 * time.Millisecond
	for rd.conns < rd.maxConns {
		log, final := rd.connection(ctx)
		rd.conns++
		rd.stats.conn(log, rd.conns > 1)
		if rd.keepLogs {
			rd.logs = append(rd.logs, log)
		}
		if log.Upstream != "" {
			if rd.upstreams == nil {
				rd.upstreams = map[string]int{}
			}
			rd.upstreams[log.Upstream]++
		}
		if final || rd.tally.Terminals > 0 {
			return
		}
		if ctx.Err() != nil {
			return // the connection's log already says deadline
		}
		switch log.Ended {
		case EndTransport, EndStatus:
			// The server or the proxy refused: back off a little before the next attempt.
			if pause(ctx, backoff) != nil {
				rd.stats.endedBy(EndDeadline)
				return
			}
			backoff = min(2*backoff, time.Second)
		default:
			backoff = 50 * time.Millisecond // a drop or a cut body: reconnect at once
		}
	}
}

// connection runs one GET. final is true when no further connection can help: a 4xx status.
func (rd *reader) connection(ctx context.Context) (log ConnLog, final bool) {
	log.Cursor = rd.tally.Last
	dropAfter, dropTime := rd.drop.plan(rd.rng, rd.events+1-int(rd.tally.Last)) //nolint:gosec // seqs of one stream
	connCtx, drop := context.WithCancel(ctx)
	defer drop()
	if dropTime > 0 {
		t := time.AfterFunc(dropTime, drop)
		defer t.Stop()
	}
	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, rd.baseURL+"/streams/"+rd.id, http.NoBody)
	if err != nil {
		log.Ended = EndTransport
		return log, true
	}
	req.Header.Set("Accept", "text/event-stream")
	if rd.tally.Last > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatUint(rd.tally.Last, 10))
	}
	opened := time.Now()
	resp, err := rd.client.Do(req)
	if err == nil {
		rd.stats.connected(time.Since(opened))
	}
	if err != nil {
		switch {
		case ctx.Err() != nil:
			log.Ended = EndDeadline
			return log, false
		case connCtx.Err() != nil:
			log.Ended = EndDropped // the drop timer fired before the headers
			return log, false
		}
		log.Ended = EndTransport
		rd.stats.httpError("get", "transport")
		return log, false
	}
	log.Status, log.Upstream = resp.StatusCode, resp.Header.Get("X-Upstream")
	if resp.StatusCode != http.StatusOK {
		drainClose(resp)
		log.Ended = EndStatus
		rd.stats.httpError("get", strconv.Itoa(resp.StatusCode))
		// 5xx is worth retrying (an instance or the store may come back); 4xx is final.
		return log, resp.StatusCode < 500
	}
	log = rd.readBody(ctx, connCtx, resp.Body, opened, dropAfter, &log)
	if log.Ended != EndTerminal {
		drop() // close the connection itself, as a dying network would, before draining
	}
	drainClose(resp)
	return log, false
}

// readBody reads one 200 response until the terminal event, a drop or the end of the body.
func (rd *reader) readBody(ctx, connCtx context.Context, body io.Reader, opened time.Time, dropAfter int, log *ConnLog) ConnLog {
	frames := sseclient.NewReader(body)
	paused := false
	for {
		f, err := frames.Next()
		if err != nil {
			switch {
			case paused:
				log.Ended = EndReleased
				rd.released = true
			case ctx.Err() != nil:
				log.Ended = EndDeadline // the reader gave up while the stream was still running
			case connCtx.Err() != nil:
				log.Ended = EndDropped
			case errors.Is(err, io.EOF):
				log.Ended = EndServerEOF
			default:
				log.Ended = EndReadError
			}
			return *log
		}
		if !f.IsEvent() {
			continue
		}
		ev, err := toEvent(f)
		if err != nil {
			rd.tally.BadPayload++
			continue
		}
		now := time.Now()
		if !rd.firstSeen {
			rd.firstSeen = true
			rd.stats.firstEvent(now.Sub(rd.started))
		}
		if p, ok := rd.tally.Observe(ev); ok {
			rd.stats.event(now, time.Unix(0, p.T), p.T >= opened.UnixNano())
		}
		log.Events++
		if rd.tally.Terminals > 0 {
			log.Ended = EndTerminal
			return *log
		}
		if rd.slow != nil && !paused && rd.tally.Events >= rd.slow.AfterEvents {
			paused = true
			rd.stats.slowPaused()
			if pause(ctx, rd.slow.Pause) != nil {
				log.Ended = EndDeadline
				return *log
			}
			rd.slow = nil // pause once
		}
		if dropAfter > 0 && log.Events >= dropAfter {
			log.Ended = EndDropped
			return *log
		}
	}
}

// toEvent converts a parsed frame to an event; stored events carry a numeric id.
func toEvent(f sseclient.Frame) (rebound.Event, error) {
	if !f.HasID {
		return rebound.Event{}, fmt.Errorf("loadtest: event %q without id", f.Event)
	}
	seq, err := strconv.ParseUint(f.ID, 10, 64)
	if err != nil {
		return rebound.Event{}, fmt.Errorf("loadtest: event id %q is not a decimal seq", f.ID)
	}
	return rebound.Event{Seq: seq, Kind: f.Event, Data: []byte(f.Data)}, nil
}

// drainClose closes a response body without reading a long stream to its end.
func drainClose(resp *http.Response) {
	// Errors only mean the connection will not be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	_ = resp.Body.Close()
}
