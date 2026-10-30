package core

// Adversarial tests: many concurrent readers at the replay/live boundary, terminal races, Close
// under load, goroutine leaks and large events, on every store. Regression tests for specific
// defects are in regression_test.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
)

// stressSeed seeds the randomized stress tests; failures print it with the reader index.
const stressSeed = 20261011

type stressStore struct {
	name string
	open func(t *testing.T, c *testutil.Clock) store.Store
}

func stressStores() []stressStore {
	return []stressStore{
		{"memory", func(t *testing.T, c *testutil.Clock) store.Store {
			t.Helper()
			s := memory.New(memory.WithClock(c))
			t.Cleanup(func() { _ = s.Close() }) // closing an in-memory store cannot fail usefully
			return s
		}},
		{"miniredis", func(t *testing.T, c *testutil.Clock) store.Store {
			t.Helper()
			return miniredisInstances(t)(t, c)
		}},
	}
}

func stressPayload(seq uint64) []byte { return []byte("ev-" + strconv.FormatUint(seq, 10)) }

// G1/G2 at the replay/live boundary: 50 readers subscribe at random cursors (any value up to the
// current LastSeq) while the producer appends, read a random number of events, drop, and resume
// either where they left or at a new random cursor. Every subscription must deliver exactly
// cursor+1, cursor+2, … up to the terminal event.
func TestStress_ReplayLiveBoundary_50Readers(t *testing.T) {
	t.Parallel()
	for _, sc := range stressStores() {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			total := uint64(3000)
			if sc.name == "miniredis" {
				total = 400 // Lua under -race is slow
			}
			c := testutil.NewClock(t0)
			x := newHarnessOn(t, c, sc.open(t, c), WithMaxEvents(int(total)+10))
			id := x.start(func(_ context.Context, w Writer) error {
				for seq := uint64(1); seq <= total; seq++ {
					if err := w.Write(stressPayload(seq)); err != nil {
						return err
					}
					if seq%8 == 0 {
						runtime.Gosched() // let readers interleave with the appends
					}
				}
				return nil
			})

			const readers = 50
			errs := make([]error, readers)
			var wg sync.WaitGroup
			for r := range readers {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(stressSeed, uint64(r)))
					if err := stressReader(x.hub, id, total, rng); err != nil {
						errs[r] = fmt.Errorf("reader %d (seed %d): %w", r, stressSeed, err)
					}
				})
			}
			wg.Wait()
			for _, err := range errs {
				assert.NoError(t, err)
			}
		})
	}
}

func stressReader(hub *Hub, id string, total uint64, rng *rand.Rand) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var maxSeen uint64
	for attempt := range 5000 {
		st, err := hub.Status(ctx, id)
		if err != nil {
			return err
		}
		cursor := rng.Uint64N(st.LastSeq + 1)
		if attempt > 0 && rng.IntN(2) == 0 {
			cursor = maxSeen
		}
		sub, err := hub.Subscribe(ctx, id, cursor)
		if err != nil {
			return fmt.Errorf("subscribe after %d (last %d): %w", cursor, st.LastSeq, err)
		}
		finished, seen, err := stressReadSome(ctx, sub, cursor, total, rng.IntN(200)+1)
		_ = sub.Close() // always succeeds
		maxSeen = max(maxSeen, seen)
		if err != nil || finished {
			return err
		}
	}
	return errors.New("never reached the terminal event")
}

// stressReadSome reads up to budget events and checks that they are exactly cursor+1, cursor+2, …
func stressReadSome(ctx context.Context, sub *Subscription, cursor, total uint64, budget int) (finished bool, maxSeen uint64, err error) {
	want := cursor + 1
	for range budget {
		ev, err := sub.Next(ctx)
		if errors.Is(err, io.EOF) {
			if want != total+2 {
				return false, want - 1, fmt.Errorf("after %d: EOF before the terminal event (next would be %d)", cursor, want)
			}
			return true, want - 1, nil
		}
		if err != nil {
			return false, want - 1, fmt.Errorf("after %d: next: %w", cursor, err)
		}
		if ev.Seq != want {
			return false, want - 1, fmt.Errorf("after %d: got seq %d, want %d (gap or duplicate)", cursor, ev.Seq, want)
		}
		switch {
		case ev.Seq <= total && (ev.Kind != store.KindData || !bytes.Equal(ev.Data, stressPayload(ev.Seq))):
			return false, want - 1, fmt.Errorf("seq %d: got %s %q", ev.Seq, ev.Kind, ev.Data)
		case ev.Seq == total+1 && ev.Kind != store.KindDone:
			return false, want - 1, fmt.Errorf("seq %d: want terminal done, got %s", ev.Seq, ev.Kind)
		}
		want++
	}
	return false, want - 1, nil
}

// terminalRace starts a producer that has written one event and then waits; three actors then try
// to end the stream at the same instant: the producer returns nil (done), a reader writes
// producer_lost (the path of Subscription.checkLiveness), and Cancel is called. Live readers wait
// on the stream meanwhile. It returns the stored terminal event and the OnStreamEnd reason.
func terminalRace(t *testing.T, sc stressStore) (terminal Event, hookReason string) {
	t.Helper()
	c := testutil.NewClock(t0)
	rec := &recorder{}
	x := newHarnessOn(t, c, sc.open(t, c), WithHooks(rec.hooks()))
	ctx := context.Background()
	release := make(chan struct{})
	id := x.start(func(_ context.Context, w Writer) error {
		if err := w.Write([]byte("a")); err != nil {
			return err
		}
		<-release
		return nil
	})
	testutil.Eventually(t, func() bool {
		st, err := x.hub.Status(ctx, id)
		require.NoError(t, err)
		return st.LastSeq == 1
	}, waitLimit)

	const liveReaders = 4
	seen := make(chan Event, liveReaders)
	var readers sync.WaitGroup
	for range liveReaders {
		sub, err := x.hub.Subscribe(ctx, id, 1)
		require.NoError(t, err)
		readers.Go(func() {
			defer func() { _ = sub.Close() }()
			evs := drain(t, sub)
			if assert.Len(t, evs, 1) {
				seen <- evs[0]
			}
		})
	}

	start := make(chan struct{})
	var racers sync.WaitGroup
	racers.Go(func() { <-start; close(release) })
	racers.Go(func() { <-start; x.hub.writeTerminal(ctx, id, errorEvent(codeProducerLost, "")) })
	racers.Go(func() { <-start; assert.NoError(t, x.hub.Cancel(ctx, id)) })
	close(start)
	racers.Wait()

	st := x.awaitTerminal(id)
	readers.Wait()
	close(seen)
	evs := x.collect(id, 0)
	require.Len(t, evs, 2, "exactly one terminal event after the data event (G7)")
	terminal = evs[1]
	require.True(t, IsTerminal(terminal.Kind))
	require.Equal(t, uint64(2), st.LastSeq)
	if terminal.Kind == store.KindError {
		code, _ := terminalCode(t, terminal)
		require.Equal(t, code, st.ErrorCode, "Status agrees with the stored terminal event")
	}
	for ev := range seen {
		require.Equal(t, terminal, ev, "every live reader received the stored terminal event")
	}
	testutil.Eventually(t, func() bool { return len(rec.endReasons()) == 1 }, waitLimit)
	return terminal, rec.endReasons()[0]
}

// G7 under contention: producer done vs reader producer_lost vs cancel, repeated.
func TestStress_TerminalRace_ExactlyOneTerminal(t *testing.T) {
	t.Parallel()
	for _, sc := range stressStores() {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			iterations := 200
			if sc.name == "miniredis" {
				iterations = 30
			}
			winners := map[string]int{}
			for range iterations {
				ev, _ := terminalRace(t, sc)
				winner := ev.Kind
				if ev.Kind == store.KindError {
					winner, _ = terminalCode(t, ev)
				}
				winners[winner]++
			}
			t.Logf("terminal winners over %d races: %v", iterations, winners)
		})
	}
}

// Cancel received by instance B while a busy producer runs on instance A: the producer's next
// write sees the flag in the shared store (no heartbeat needed), the stream ends with exactly one
// "cancelled" terminal, no data event follows it, and the producer stops writing.
func TestStress_CancelOnInstanceB_BusyProducerOnA(t *testing.T) {
	t.Parallel()
	instances := miniredisInstances(t)
	c := testutil.NewClock(t0)
	a := newHarnessOn(t, c, instances(t, c))
	b := newHarnessOn(t, c, instances(t, c))
	ctx := context.Background()

	var written atomic.Int64
	var lastErr atomic.Value
	id := a.start(func(_ context.Context, w Writer) error {
		for i := 0; ; i++ {
			if err := w.Write([]byte(strconv.Itoa(i))); err != nil {
				lastErr.Store(err)
				return err
			}
			written.Add(1)
		}
	})
	testutil.Eventually(t, func() bool { return written.Load() >= 20 }, waitLimit)

	sub, err := b.hub.Subscribe(ctx, id, 0)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	got := make(chan []Event, 1)
	go func() { got <- drain(t, sub) }()

	require.NoError(t, b.hub.Cancel(ctx, id))
	st := b.awaitTerminal(id) // the clock never moves: no heartbeat is involved
	assert.Equal(t, codeCancelled, st.ErrorCode)
	assert.True(t, st.Cancelled)
	require.ErrorIs(t, lastErr.Load().(error), ErrCancelled) //nolint:forcetypeassert // only errors are stored

	evs := <-got
	require.NotEmpty(t, evs)
	for i, ev := range evs {
		require.Equal(t, uint64(i+1), ev.Seq, "dense sequence")
	}
	code, _ := terminalCode(t, evs[len(evs)-1])
	assert.Equal(t, codeCancelled, code)
	assert.Equal(t, st.LastSeq, evs[len(evs)-1].Seq, "nothing after the terminal event")
	assert.Equal(t, int64(st.LastSeq-1), written.Load(), "every acknowledged write is in the log, then the terminal")
}

// Hub.Close with cooperative, quick and context-ignoring producers, and readers on every stream:
// Close returns at its deadline, every stream ends with exactly one terminal event (done for the
// producers that finished, server_shutdown for the others), every reader receives it, and Start is
// refused afterwards.
func TestStress_CloseWhileProducersAndReadersActive(t *testing.T) {
	t.Parallel()
	for _, sc := range stressStores() {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			c := testutil.NewClock(t0)
			x := newHarnessOn(t, c, sc.open(t, c))
			ctx := context.Background()
			release := make(chan struct{})
			defer close(release)

			paced := func(pctx context.Context, w Writer) error {
				for i := 0; ; i++ {
					if err := w.Write([]byte(strconv.Itoa(i))); err != nil {
						return err
					}
					select {
					case <-pctx.Done():
						return pctx.Err()
					case <-time.After(time.Millisecond):
					}
				}
			}
			quick := func(_ context.Context, w Writer) error { return w.Write([]byte("q")) }
			stubborn := func(_ context.Context, w Writer) error {
				if err := w.Write([]byte("s")); err != nil {
					return err
				}
				<-release // ignores its context
				return nil
			}
			want := map[string]string{}
			for range 5 {
				want[x.start(paced)] = codeServerShutdown
				want[x.start(stubborn)] = codeServerShutdown
				id := x.start(quick)
				x.awaitTerminal(id)
				want[id] = store.KindDone
			}

			type result struct {
				id  string
				evs []Event
			}
			results := make(chan result, 2*len(want))
			var readers sync.WaitGroup
			for id := range want {
				for range 2 {
					sub, err := x.hub.Subscribe(ctx, id, 0)
					require.NoError(t, err)
					readers.Go(func() {
						defer func() { _ = sub.Close() }()
						results <- result{id, drain(t, sub)}
					})
				}
			}

			closeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			err := x.hub.Close(closeCtx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			_, err = x.hub.Start(ctx, quick)
			require.ErrorIs(t, err, ErrHubClosed)

			readers.Wait()
			close(results)
			for r := range results {
				require.NotEmpty(t, r.evs)
				last := r.evs[len(r.evs)-1]
				outcome := last.Kind
				if last.Kind == store.KindError {
					outcome, _ = terminalCode(t, last)
				}
				assert.Equal(t, want[r.id], outcome, "stream %s", r.id)
				for i, ev := range r.evs {
					require.Equal(t, uint64(i+1), ev.Seq)
					require.Equal(t, i == len(r.evs)-1, IsTerminal(ev.Kind), "exactly one terminal, last")
				}
			}
		})
	}
}

// 1,000 subscribe/disconnect cycles on a live stream (half cancelled through the context while
// Next waits, half closed with Close) leave no goroutine behind.
func TestStress_NoGoroutineLeak_SubscribeDisconnectCycles(t *testing.T) {
	x := newHarness(t)
	ctx := context.Background()
	id := x.start(func(pctx context.Context, w Writer) error {
		if err := w.Write([]byte("first")); err != nil {
			return err
		}
		<-pctx.Done()
		return nil
	})
	testutil.Eventually(t, func() bool {
		st, err := x.hub.Status(ctx, id)
		require.NoError(t, err)
		return st.LastSeq == 1
	}, waitLimit)
	before := runtime.NumGoroutine()

	for i := range 1000 {
		sub, err := x.hub.Subscribe(ctx, id, 0)
		require.NoError(t, err)
		nctx, cancel := context.WithCancel(ctx)
		ev, err := sub.Next(nctx)
		require.NoError(t, err)
		require.Equal(t, uint64(1), ev.Seq)
		done := make(chan error, 1)
		go func() {
			_, err := sub.Next(nctx) // waits live: nothing more is written
			done <- err
		}()
		if i%2 == 0 {
			cancel()
		} else {
			require.NoError(t, sub.Close())
		}
		require.Error(t, <-done)
		cancel()
		require.NoError(t, sub.Close())
	}
	testutil.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+2 }, waitLimit)
	t.Logf("goroutines: before %d, after %d", before, runtime.NumGoroutine())
}

// Events of exactly the size limit are stored and replayed byte for byte on both stores, one
// byte more is ErrTooLarge, and resuming in the middle of large events works.
func TestStress_LargeEvents(t *testing.T) {
	t.Parallel()
	for _, sc := range stressStores() {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			c := testutil.NewClock(t0)
			x := newHarnessOn(t, c, sc.open(t, c))
			big := func(i int) []byte {
				b := bytes.Repeat([]byte{byte('a' + i%26)}, defaultMaxEventBytes)
				b[100], b[200] = '\n', 0 // line breaks and NUL inside the payload
				return b
			}
			tooLarge := make(chan error, 1)
			id := x.start(func(_ context.Context, w Writer) error {
				for i := range 20 {
					if err := w.Write(big(i)); err != nil {
						return err
					}
				}
				tooLarge <- w.Write(make([]byte, defaultMaxEventBytes+1))
				return nil
			})
			require.ErrorIs(t, <-tooLarge, ErrTooLarge)
			x.awaitTerminal(id)
			evs := x.collect(id, 7)
			require.Len(t, evs, 14)
			for i, ev := range evs[:13] {
				require.Equal(t, uint64(i+8), ev.Seq)
				require.True(t, bytes.Equal(big(i+7), ev.Data), "event %d differs", ev.Seq)
			}
		})
	}
}

// Generated stream ids are 22 base64url characters from 16 crypto/rand bytes: 100,000 of them are
// distinct and use the whole alphabet.
func TestStress_GeneratedIDs(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 100_000)
	chars := map[rune]bool{}
	for range 100_000 {
		id, err := randomID()
		require.NoError(t, err)
		require.Len(t, id, 22)
		require.NoError(t, validateStreamID(id))
		_, dup := seen[id]
		require.False(t, dup, "duplicate id %s", id)
		seen[id] = struct{}{}
		for _, c := range id {
			chars[c] = true
		}
	}
	assert.Len(t, chars, 64, "base64url alphabet")
}
