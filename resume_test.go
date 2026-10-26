package rebound

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

var (
	resumeSeed       = flag.Uint64("seed", 0, "seed of TestResume_NoGapNoDup (0: time-based)")
	resumeIterations = flag.Int("iterations", 500, "iterations of TestResume_NoGapNoDup")
)

// expectedStream is what one producer writes, decided before it starts.
type expectedStream struct {
	events []Event // data and custom events, seq 1..n
	fail   bool    // return an error instead of nil
}

// TestResume_NoGapNoDup is the randomized proof of G1/G2: streams of random length and payloads,
// readers that join at random moments, drop after a random number of events and resume from
// their last seq. Every reader must assemble exactly the produced sequence, terminal included.
// Reproduce a failure with: go test -race -run TestResume_NoGapNoDup . -seed=<seed>
func TestResume_NoGapNoDup(t *testing.T) {
	seed := *resumeSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	runResume(t, seed, *resumeIterations, func(c *testutil.Clock) store.Store {
		return memory.New(memory.WithClock(c))
	})
}

// runResume runs iterations of the resume scenario, each on a fresh store from newStore.
func runResume(t *testing.T, seed uint64, iterations int, newStore func(*testutil.Clock) store.Store) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("reproduce with: go test -race -run %s . -seed=%d", t.Name(), seed)
		}
	})
	for iter := range iterations {
		if err := resumeIteration(seed, uint64(iter), newStore); err != nil {
			t.Fatalf("seed=%d iteration=%d: %v", seed, iter, err)
		}
	}
	t.Logf("%d iterations passed, seed %d", iterations, seed)
}

func resumeIteration(seed, iter uint64, newStore func(*testutil.Clock) store.Store) error {
	rng := rand.New(rand.NewPCG(seed, iter))
	clock := testutil.NewClock(t0)
	s := newStore(clock)
	defer func() {
		_ = s.Close() // a store close error cannot change the verdict of this iteration
	}()
	hub, err := New(s, WithClock(clock), WithLogger(quietLogger()))
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
		defer cancel()
		_ = hub.Close(ctx) // every producer has returned by now; an error would surface below anyway
	}()

	streams := 1 + rng.IntN(4)
	var wg sync.WaitGroup
	errs := make(chan error, streams*4)
	for s := range streams {
		exp := randomStream(rng)
		readers := 1 + rng.IntN(3)
		readerSeeds := make([]uint64, readers)
		for r := range readerSeeds {
			readerSeeds[r] = rng.Uint64()
		}
		earlyReaders := rng.IntN(readers + 1) // some readers subscribe before the first event

		release := make(chan struct{})
		st, err := hub.Start(context.Background(), replayProducer(exp, release))
		if err != nil {
			return err
		}
		for r, rs := range readerSeeds {
			if r == earlyReaders {
				close(release)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := readWithDrops(hub, st.ID, rs, exp); err != nil {
					errs <- fmt.Errorf("stream %d (%s) reader %d: %w", s, st.ID, r, err)
				}
			}()
		}
		if earlyReaders == readers {
			close(release)
		}
	}
	wg.Wait()
	close(errs)
	return errors.Join(collectErrs(errs)...)
}

func collectErrs(ch <-chan error) []error {
	out := make([]error, 0, len(ch))
	for err := range ch {
		out = append(out, err)
	}
	return out
}

func randomStream(rng *rand.Rand) expectedStream {
	n := rng.IntN(151)
	exp := expectedStream{events: make([]Event, n), fail: rng.IntN(5) == 0}
	for i := range exp.events {
		kind := "data"
		if rng.IntN(10) == 0 {
			kind = "meta"
		}
		data := make([]byte, rng.IntN(513))
		for j := range data {
			data[j] = byte(rng.UintN(256))
		}
		exp.events[i] = Event{Seq: uint64(i + 1), Kind: kind, Data: data}
	}
	return exp
}

// replayProducer writes exp's events once release is closed, yielding often so that readers
// interleave with appends at every possible point.
func replayProducer(exp expectedStream, release <-chan struct{}) ProducerFunc {
	return func(_ context.Context, w Writer) error {
		<-release
		for i, ev := range exp.events {
			if err := w.WriteEvent(ev.Kind, ev.Data); err != nil {
				return err
			}
			if i%3 == 0 {
				runtime.Gosched()
			}
		}
		if exp.fail {
			return errors.New("planned failure")
		}
		return nil
	}
}

// readWithDrops reads the whole stream through successive subscriptions, each closed after a
// random number of events, resuming from the last seq received, then checks the result.
func readWithDrops(hub *Hub, id string, seed uint64, exp expectedStream) error {
	rng := rand.New(rand.NewPCG(seed, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var got []Event
	var cursor uint64
	for connection := 0; ; connection++ {
		if connection > 1000 {
			return errors.New("more than 1000 connections: no progress")
		}
		sub, err := hub.Subscribe(ctx, id, cursor)
		if err != nil {
			return fmt.Errorf("subscribe after %d: %w", cursor, err)
		}
		budget := 1 + rng.IntN(20)
		eof := false
		for range budget {
			ev, err := sub.Next(ctx)
			if errors.Is(err, io.EOF) {
				eof = true
				break
			}
			if err != nil {
				_ = sub.Close() // the read error is what matters
				return fmt.Errorf("next after %d: %w", cursor, err)
			}
			got = append(got, ev)
			cursor = ev.Seq
		}
		if err := sub.Close(); err != nil {
			return err
		}
		if eof {
			return verify(got, exp)
		}
	}
}

func verify(got []Event, exp expectedStream) error {
	if len(got) != len(exp.events)+1 {
		return fmt.Errorf("received %d events, want %d data + 1 terminal", len(got), len(exp.events))
	}
	for i, ev := range got {
		if ev.Seq != uint64(i+1) {
			return fmt.Errorf("position %d has seq %d: gap or duplicate", i, ev.Seq)
		}
		if i == len(exp.events) {
			break
		}
		want := exp.events[i] // random bytes, stored normalized (ADR 0007)
		if ev.Kind != want.Kind || !bytes.Equal(ev.Data, normalizePayload(want.Data)) {
			return fmt.Errorf("seq %d: got %s %d bytes, want %s %d bytes", ev.Seq, ev.Kind, len(ev.Data), want.Kind, len(normalizePayload(want.Data)))
		}
	}
	last := got[len(got)-1]
	switch {
	case exp.fail && last.Kind != "error":
		return fmt.Errorf("terminal %s, want error (producer_failed)", last.Kind)
	case !exp.fail && last.Kind != "done":
		return fmt.Errorf("terminal %s %s, want done", last.Kind, last.Data)
	}
	return nil
}
