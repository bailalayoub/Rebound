package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/bailalayoub/rebound"
)

// scenario is one row of docs/06 §2.
type scenario struct {
	id   string
	name string
	run  func(ctx context.Context, ri runInfo) runResult
}

// runInfo identifies one run of a scenario and hands out its random sources.
type runInfo struct {
	scenario string
	run      int
	r        *runner
}

// rng returns the random source of reader n of this run (0 for the run itself).
func (ri runInfo) rng(n int) *mrand.Rand { return ri.r.rng(ri.scenario, ri.run, n) }

// runResult is the outcome of one run.
type runResult struct {
	failure       *Failure
	reconnections int
	ttfe, ttt     []time.Duration // time to first event / to terminal, one sample per stream
	trace         []ConnLog
}

// scenarios is the table of docs/06 §2, in order. It is never modified.
var scenarios = []scenario{
	{"S1", "happy path", func(ctx context.Context, ri runInfo) runResult { return ri.followOne(ctx, false, false) }},
	{"S2", "random drops + resume", func(ctx context.Context, ri runInfo) runResult { return ri.followOne(ctx, true, false) }},
	{"S3", "restart from zero", runRestart},
	{"S4", "cancel", runCancel},
	{"S5", "producer failure", runFailure},
	{"S6", "concurrent readers", runConcurrentReaders},
	{"S7", "invalid cursor", runInvalidCursor},
	{"S8", "unknown stream", runUnknownStream},
	{"S9", "already complete", runAlreadyComplete},
	{"S10", "parallel streams", runParallelStreams},
	{"S11", "custom kinds", func(ctx context.Context, ri runInfo) runResult { return ri.followOne(ctx, true, true) }},
	{"S12", "late joiner", runLateJoiner},
}

// failed builds a failed result.
func failed(streamID string, res readResult, err error) runResult {
	return runResult{
		failure:       &Failure{StreamID: streamID, Error: err.Error(), Connections: res.conns},
		reconnections: res.reconnected,
		trace:         res.conns,
	}
}

func timing(started time.Time, res readResult) runResult {
	out := runResult{reconnections: res.reconnected, trace: res.conns}
	if !res.firstEvent.IsZero() {
		out.ttfe = []time.Duration{res.firstEvent.Sub(started)}
	}
	if !res.terminalAt.IsZero() {
		out.ttt = []time.Duration{res.terminalAt.Sub(started)}
	}
	return out
}

// followOne is S1 (no drops), S2 (drops + resume) and S11 (drops + resume, custom kinds).
func (ri runInfo) followOne(ctx context.Context, drops, kinds bool) runResult {
	rng := ri.rng(0)
	req := ri.r.request(rng)
	req.Kinds = kinds
	exp := ExpectedEvents(req)
	id, started, err := ri.r.start(ctx, req)
	if err != nil {
		return failed("", readResult{}, err)
	}
	opts := readOptions{mode: modeResume}
	if drops {
		opts.dropRate = ri.r.cfg.DropRate
	}
	res, err := ri.r.read(ctx, rng, id, opts)
	if err == nil {
		err = verify(exp, res.events, res.terminal)
	}
	if err == nil && !drops && len(res.conns) != 1 {
		err = fmt.Errorf("%d connections without drops, want 1", len(res.conns))
	}
	if err != nil {
		return failed(id, res, err)
	}
	return timing(started, res)
}

// runRestart is S3: every reconnection omits Last-Event-ID, so each connection starts at seq 1
// (checked while reading) and the last one carries the whole stream.
func runRestart(ctx context.Context, ri runInfo) runResult {
	rng := ri.rng(0)
	req := ri.r.request(rng)
	exp := ExpectedEvents(req)
	id, started, err := ri.r.start(ctx, req)
	if err != nil {
		return failed("", readResult{}, err)
	}
	// Each reconnection starts over at seq 1, so drops are capped: with a drop chance after every
	// event, reading a long stream in one piece would practically never happen.
	opts := readOptions{mode: modeRestart, dropRate: ri.r.cfg.DropRate, maxDrops: 1 + rng.IntN(3)}
	res, err := ri.r.read(ctx, rng, id, opts)
	if err == nil {
		err = verify(exp, res.events, res.terminal)
	}
	if err != nil {
		return failed(id, res, err)
	}
	return timing(started, res)
}

// runCancel is S4: cancel at a random early point; the terminal error{cancelled} must follow within
// CancelWithin, and the events received must be a strict prefix of the full stream.
func runCancel(ctx context.Context, ri runInfo) runResult {
	rng := ri.rng(0)
	req := ri.r.request(rng)
	req.DelayMS = max(req.DelayMS, 20) // leave the producer running long after the cancel
	exp := ExpectedEvents(req)
	id, started, err := ri.r.start(ctx, req)
	if err != nil {
		return failed("", readResult{}, err)
	}
	cancelAfter := 1 + rng.IntN(max(1, min(10, len(exp.Events)/4)))
	var (
		cancelledAt time.Time
		cancelErr   error
	)
	res, err := ri.r.read(ctx, rng, id, readOptions{mode: modeResume, afterEvent: func(received int) {
		if received == cancelAfter {
			cancelErr = ri.r.cancel(ctx, id)
			cancelledAt = time.Now()
		}
	}})
	switch {
	case err != nil:
	case cancelErr != nil:
		err = cancelErr
	case cancelledAt.IsZero():
		err = fmt.Errorf("stream ended after %d events, before the cancel point %d", len(res.events), cancelAfter)
	case terminalCode(res.terminal) != "cancelled":
		err = fmt.Errorf("terminal %s, want cancelled", terminalCode(res.terminal))
	case res.terminalAt.Sub(cancelledAt) > ri.r.cfg.CancelWithin:
		err = fmt.Errorf("terminal %v after the cancel, want within %v", res.terminalAt.Sub(cancelledAt).Round(time.Millisecond), ri.r.cfg.CancelWithin)
	case len(res.events) >= len(exp.Events):
		err = errors.New("received the whole stream: not a strict prefix")
	default:
		err = verifyPrefix(exp, res.events)
	}
	if err != nil {
		return failed(id, res, err)
	}
	return timing(started, res)
}

func verifyPrefix(exp Expected, got []rebound.Event) error {
	for i, have := range got {
		want := exp.Events[i]
		if want.Seq != have.Seq || want.Kind != have.Kind || !bytes.Equal(want.Data, have.Data) {
			return fmt.Errorf("first divergence at seq %d: got %s %q, want %s %q",
				want.Seq, have.Kind, abbreviate(have.Data), want.Kind, abbreviate(want.Data))
		}
	}
	return nil
}

// runFailure is S5: the producer fails after k data events, read with drops and resume.
func runFailure(ctx context.Context, ri runInfo) runResult {
	rng := ri.rng(0)
	req := ri.r.request(rng)
	req.FailAfter = rng.IntN(len(Chunks(req.Text, req.Chunk)))
	exp := ExpectedEvents(req)
	id, started, err := ri.r.start(ctx, req)
	if err != nil {
		return failed("", readResult{}, err)
	}
	res, err := ri.r.read(ctx, rng, id, readOptions{mode: modeResume, dropRate: ri.r.cfg.DropRate})
	if err == nil {
		err = verify(exp, res.events, res.terminal)
	}
	if err != nil {
		return failed(id, res, err)
	}
	return timing(started, res)
}

// runConcurrentReaders is S6: Concurrency readers, each with its own drop pattern.
func runConcurrentReaders(ctx context.Context, ri runInfo) runResult {
	req := ri.r.request(ri.rng(0))
	exp := ExpectedEvents(req)
	id, started, err := ri.r.start(ctx, req)
	if err != nil {
		return failed("", readResult{}, err)
	}
	results := make([]readResult, ri.r.cfg.Concurrency)
	errs := make([]error, ri.r.cfg.Concurrency)
	var wg sync.WaitGroup
	for n := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := ri.r.read(ctx, ri.rng(n+1), id, readOptions{mode: modeResume, dropRate: ri.r.cfg.DropRate})
			if err == nil {
				err = verify(exp, res.events, res.terminal)
			}
			results[n], errs[n] = res, err
		}()
	}
	wg.Wait()
	out := runResult{}
	for n, res := range results {
		out.reconnections += res.reconnected
		out.trace = append(out.trace, res.conns...)
		if errs[n] != nil {
			f := failed(id, res, fmt.Errorf("reader %d: %w", n, errs[n]))
			f.reconnections = out.reconnections
			return f
		}
		out.ttfe = append(out.ttfe, timing(started, res).ttfe...)
		out.ttt = append(out.ttt, timing(started, res).ttt...)
	}
	return out
}

// finished starts a stream and reads it to its terminal event, for the scenarios that probe a
// finished stream.
func (ri runInfo) finished(ctx context.Context) (string, readResult, error) {
	rng := ri.rng(0)
	req := ri.r.request(rng)
	req.DelayMS = 0
	exp := ExpectedEvents(req)
	id, _, err := ri.r.start(ctx, req)
	if err != nil {
		return "", readResult{}, err
	}
	res, err := ri.r.read(ctx, rng, id, readOptions{mode: modeResume})
	if err == nil {
		err = verify(exp, res.events, res.terminal)
	}
	return id, res, err
}

// expectStatus checks the status of GET /streams/{id}; a 204 must also have an empty body.
func expectStatus(ctx context.Context, r *runner, id string, cursor *uint64, want int) error {
	code, body, err := r.status(ctx, id, cursor)
	switch {
	case err != nil:
		return err
	case code != want:
		return fmt.Errorf("HTTP %d %s, want %d", code, abbreviate(body), want)
	case want == http.StatusNoContent && len(body) != 0:
		return fmt.Errorf("HTTP 204 with %d body bytes, want none", len(body))
	}
	return nil
}

// runInvalidCursor is S7: Last-Event-ID beyond the end of a finished stream is HTTP 400.
func runInvalidCursor(ctx context.Context, ri runInfo) runResult {
	id, res, err := ri.finished(ctx)
	if err != nil {
		return failed(id, res, err)
	}
	cursor := res.terminal.Seq + 1000
	if err := expectStatus(ctx, ri.r, id, &cursor, http.StatusBadRequest); err != nil {
		return failed(id, res, fmt.Errorf("Last-Event-ID LastSeq+1000: %w", err))
	}
	return runResult{trace: res.conns}
}

// runUnknownStream is S8: a random id is HTTP 404.
func runUnknownStream(ctx context.Context, ri runInfo) runResult {
	rng := ri.rng(0)
	id := fmt.Sprintf("unknown-%016x%016x", rng.Uint64(), rng.Uint64())
	if err := expectStatus(ctx, ri.r, id, nil, http.StatusNotFound); err != nil {
		return failed(id, readResult{}, fmt.Errorf("unknown stream: %w", err))
	}
	return runResult{}
}

// runAlreadyComplete is S9: resuming a finished stream at its terminal seq is HTTP 204, no body.
func runAlreadyComplete(ctx context.Context, ri runInfo) runResult {
	id, res, err := ri.finished(ctx)
	if err != nil {
		return failed(id, res, err)
	}
	cursor := res.terminal.Seq
	if err := expectStatus(ctx, ri.r, id, &cursor, http.StatusNoContent); err != nil {
		return failed(id, res, fmt.Errorf("Last-Event-ID LastSeq: %w", err))
	}
	return runResult{trace: res.conns}
}

// runParallelStreams is S10: Parallel streams started at once, each read with drops and resume.
func runParallelStreams(ctx context.Context, ri runInfo) runResult {
	n := ri.r.cfg.Parallel
	results := make([]runResult, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := runInfo{scenario: ri.scenario, run: ri.run*n + i, r: ri.r}
			results[i] = sub.followOne(ctx, true, false)
		}()
	}
	wg.Wait()
	out := runResult{}
	for i, res := range results {
		out.reconnections += res.reconnections
		out.ttfe = append(out.ttfe, res.ttfe...)
		out.ttt = append(out.ttt, res.ttt...)
		out.trace = append(out.trace, res.trace...)
		if res.failure != nil && out.failure == nil {
			res.failure.Error = fmt.Sprintf("stream %d of %d: %s", i, n, res.failure.Error)
			out.failure = res.failure
		}
	}
	return out
}

// runLateJoiner is S12: connect only after the producer finished; the whole stream is replayed,
// then resuming at the terminal seq is HTTP 204.
func runLateJoiner(ctx context.Context, ri runInfo) runResult {
	rng := ri.rng(0)
	req := ri.r.request(rng)
	exp := ExpectedEvents(req)
	id, _, err := ri.r.start(ctx, req)
	if err != nil {
		return failed("", readResult{}, err)
	}
	// The contract has no status endpoint: wait for the producer's own duration, plus a margin.
	if err := sleep(ctx, time.Duration(len(exp.Events)*req.DelayMS)*time.Millisecond+500*time.Millisecond); err != nil {
		return failed(id, readResult{}, err)
	}
	res, err := ri.r.read(ctx, rng, id, readOptions{mode: modeResume})
	if err == nil {
		err = verify(exp, res.events, res.terminal)
	}
	if err != nil {
		return failed(id, res, err)
	}
	cursor := res.terminal.Seq
	if err := expectStatus(ctx, ri.r, id, &cursor, http.StatusNoContent); err != nil {
		return failed(id, res, fmt.Errorf("second connect at LastSeq: %w", err))
	}
	return runResult{trace: res.conns}
}
