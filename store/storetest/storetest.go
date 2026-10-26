package storetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bailalayoub/rebound/store"
)

// Clock is the time source shared by the suite and the store under test. The suite stamps appends
// with Now and calls Advance to exercise TTL expiry: a manual clock moves instantly, a backend
// with real expiry (Redis) may fast-forward or sleep.
type Clock interface {
	Now() time.Time
	Advance(d time.Duration)
}

// Factory returns a store with no streams and the clock it was built with. Each test case calls
// it once; Run closes the store when the case ends. Ids are random, so a backend shared between
// cases (a real Redis) works too.
type Factory func(t *testing.T) (store.Store, Clock)

// waitLimit bounds every wait on the store so that a broken backend fails instead of hanging.
const waitLimit = 5 * time.Second

// Run executes the whole conformance suite against the stores built by newStore.
func Run(t *testing.T, newStore Factory) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, s store.Store, c Clock)
	}{
		{"CreateAndExists", testCreateAndExists},
		{"UnknownStream", testUnknownStream},
		{"AppendAssignsDenseSeq", testAppendAssignsDenseSeq},
		{"ReadAfterCursor", testReadAfterCursor},
		{"DataIsCopied", testDataIsCopied},
		{"BlockingReadWakesOnAppend", testBlockingReadWakesOnAppend},
		{"BlockingReadReturnsOnCtxCancel", testBlockingReadReturnsOnCtxCancel},
		{"BlockingReadTimesOut", testBlockingReadTimesOut},
		{"TerminalDoneFlipsState", testTerminalDoneFlipsState},
		{"TerminalErrorRecordsCode", testTerminalErrorRecordsCode},
		{"SingleTerminalUnderConcurrency", testSingleTerminalUnderConcurrency},
		{"ConcurrentAppendsStayDense", testConcurrentAppendsStayDense},
		{"CancelledRejectsNonForce", testCancelledRejectsNonForce},
		{"MaxEventsIsAHardCap", testMaxEventsIsAHardCap},
		{"ExpectedSeqReplayIsIdempotent", testExpectedSeqReplayIsIdempotent},
		{"ExpectedSeqConflicts", testExpectedSeqConflicts},
		{"IdleTTLExpiry", testIdleTTLExpiry},
		{"RetentionExpiry", testRetentionExpiry},
		{"StatusConsistentAfterEachAppend", testStatusConsistentAfterEachAppend},
		{"Touch", testTouch},
		{"CloseWakesBlockedReads", testCloseWakesBlockedReads},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, c := newStore(t)
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			tc.fn(t, s, c)
		})
	}
}

func testCreateAndExists(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	m := meta(c)
	m.Attrs = map[string]string{"user_id": "u-42", "model": "fake"}
	mustCreate(t, s, id, m)

	if err := s.Create(ctx, id, m); !errors.Is(err, store.ErrExists) {
		t.Fatalf("second Create: got %v, want ErrExists", err)
	}
	st := mustStatus(t, s, id)
	if st.ID != id || st.State != store.StateRunning || st.Cancelled || st.ErrorCode != "" {
		t.Fatalf("fresh status: %+v", st)
	}
	if st.FirstSeq != 0 || st.LastSeq != 0 {
		t.Fatalf("fresh stream: FirstSeq=%d LastSeq=%d, want 0 0", st.FirstSeq, st.LastSeq)
	}
	if !sameMillis(st.CreatedAt, m.CreatedAt) {
		t.Fatalf("CreatedAt = %v, want %v", st.CreatedAt, m.CreatedAt)
	}
	if len(st.Meta) != 2 || st.Meta["user_id"] != "u-42" || st.Meta["model"] != "fake" {
		t.Fatalf("Meta = %v, want the Attrs given to Create", st.Meta)
	}
	st.Meta["user_id"] = "mutated"
	if got := mustStatus(t, s, id).Meta["user_id"]; got != "u-42" {
		t.Fatalf("Status.Meta aliases store state: got %q after mutating a snapshot", got)
	}
}

func testUnknownStream(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	if _, err := s.Status(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Status: got %v, want ErrNotFound", err)
	}
	if _, err := s.Append(ctx, id, data(c, "x")); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Append: got %v, want ErrNotFound", err)
	}
	if _, err := s.Read(ctx, id, 0, 10, 0); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Read: got %v, want ErrNotFound", err)
	}
	if err := s.Cancel(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Cancel: got %v, want ErrNotFound", err)
	}
	if err := s.Touch(ctx, id, c.Now()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Touch: got %v, want ErrNotFound", err)
	}
}

func testAppendAssignsDenseSeq(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	for want := uint64(1); want <= 10; want++ {
		if got := mustAppend(t, s, id, data(c, fmt.Sprint(want))); got != want {
			t.Fatalf("append #%d got seq %d", want, got)
		}
	}
	st := mustStatus(t, s, id)
	if st.FirstSeq != 1 || st.LastSeq != 10 {
		t.Fatalf("FirstSeq=%d LastSeq=%d, want 1 10", st.FirstSeq, st.LastSeq)
	}
}

func testReadAfterCursor(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	want := []store.Event{
		{Seq: 1, Kind: "data", Data: []byte(`{"text":"Bon"}`)},
		{Seq: 2, Kind: "meta", Data: []byte(`{"instance":"app-1"}`)},
		{Seq: 3, Kind: "data", Data: []byte{}},
		{Seq: 4, Kind: "data", Data: []byte("line 1\nline 2\r\n\x00\xff")},
		{Seq: 5, Kind: "data", Data: []byte("jour")},
	}
	for _, ev := range want {
		mustAppend(t, s, id, store.AppendRequest{Kind: ev.Kind, Data: ev.Data, At: c.Now()})
	}

	got, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil {
		t.Fatalf("Read all: %v", err)
	}
	assertEvents(t, "Read(after=0, no limit)", got, want)

	got, err = s.Read(ctx, id, 2, 2, 0)
	if err != nil {
		t.Fatalf("Read(after=2, limit=2): %v", err)
	}
	assertEvents(t, "Read(after=2, limit=2)", got, want[2:4])

	got, err = s.Read(ctx, id, 5, 10, 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("Read(after=LastSeq, block=0) = %v, %v; want no events, no error", got, err)
	}
}

func testDataIsCopied(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	buf := []byte("original")
	mustAppend(t, s, id, store.AppendRequest{Kind: "data", Data: buf, At: c.Now()})
	copy(buf, "MUTATED!")

	first, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("Read: %v, %v", first, err)
	}
	if string(first[0].Data) != "original" {
		t.Fatalf("store kept a reference to the caller's buffer: %q", first[0].Data)
	}
	copy(first[0].Data, "MUTATED!")
	second, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil || len(second) != 1 || string(second[0].Data) != "original" {
		t.Fatalf("Read returned the store's own buffer: %v, %v", second, err)
	}
}

func testBlockingReadWakesOnAppend(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	mustAppend(t, s, id, data(c, "one"))
	res := asyncRead(s, id, 1, 10, time.Hour)
	mustAppend(t, s, id, data(c, "two"))

	r := awaitRead(t, res)
	if r.err != nil {
		t.Fatalf("blocking Read: %v", r.err)
	}
	assertEvents(t, "blocking Read", r.events, []store.Event{{Seq: 2, Kind: "data", Data: []byte("two")}})
}

func testBlockingReadReturnsOnCtxCancel(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan readResult, 1)
	go func() {
		evs, err := s.Read(ctx, id, 0, 10, time.Hour)
		res <- readResult{evs, err}
	}()
	cancel()
	if r := awaitRead(t, res); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("Read after cancel: got %v, %v; want context.Canceled", r.events, r.err)
	}

	if _, err := s.Read(ctx, id, 0, 10, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read with a done context: got %v, want context.Canceled", err)
	}
}

func testBlockingReadTimesOut(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	const block = 50 * time.Millisecond
	res := asyncRead(s, id, 0, 10, block)

	// Keep advancing the clock until the read gives up: a manual clock fires the read's timer once
	// it is armed, a real clock simply lets the 50 ms elapse.
	deadline := time.Now().Add(waitLimit)
	for {
		select {
		case r := <-res:
			if r.err != nil || len(r.events) != 0 {
				t.Fatalf("Read after block timeout = %v, %v; want (nil, nil)", r.events, r.err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("blocking Read never timed out")
		}
		c.Advance(block)
		waitBriefly()
	}
}

func testTerminalDoneFlipsState(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	mustAppend(t, s, id, data(c, "a"))
	if seq := mustAppend(t, s, id, store.AppendRequest{Kind: "done", Data: []byte("{}"), At: c.Now()}); seq != 2 {
		t.Fatalf("terminal seq = %d, want 2", seq)
	}
	st := mustStatus(t, s, id)
	if st.State != store.StateDone || st.ErrorCode != "" || st.LastSeq != 2 {
		t.Fatalf("after done: %+v", st)
	}
	if _, err := s.Append(ctx, id, data(c, "late")); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("data append after terminal: got %v, want ErrClosed", err)
	}
	forced := store.AppendRequest{Kind: "error", Data: []byte(`{"code":"cancelled"}`), At: c.Now(), Force: true}
	if _, err := s.Append(ctx, id, forced); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("second terminal append: got %v, want ErrClosed", err)
	}
	evs, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	assertEvents(t, "log after done", evs, []store.Event{
		{Seq: 1, Kind: "data", Data: []byte("a")},
		{Seq: 2, Kind: "done", Data: []byte("{}")},
	})
}

func testTerminalErrorRecordsCode(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	payload := []byte(`{"code":"producer_lost","message":"no heartbeat"}`)
	mustAppend(t, s, id, store.AppendRequest{Kind: "error", Data: payload, At: c.Now(), Force: true})
	st := mustStatus(t, s, id)
	if st.State != store.StateError || st.ErrorCode != "producer_lost" || st.LastSeq != 1 {
		t.Fatalf("after error terminal: %+v", st)
	}
}

func testSingleTerminalUnderConcurrency(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	mustAppend(t, s, id, data(c, "before"))

	const writers = 50
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		wins   []uint64
		others []error
	)
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := store.AppendRequest{
				Kind:  "error",
				Data:  []byte(fmt.Sprintf(`{"code":"writer_%d"}`, i)),
				At:    c.Now(),
				Force: true,
			}
			seq, err := s.Append(ctx, id, req)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins = append(wins, seq)
			} else if !errors.Is(err, store.ErrClosed) {
				others = append(others, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(others) > 0 {
		t.Fatalf("unexpected errors from concurrent terminal appends: %v", others)
	}
	if len(wins) != 1 || wins[0] != 2 {
		t.Fatalf("successful terminal appends = %v, want exactly one with seq 2", wins)
	}
	evs, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(evs) != 2 || !store.IsTerminal(evs[1].Kind) {
		t.Fatalf("log = %v, want one data event then one terminal", evs)
	}
	if st := mustStatus(t, s, id); st.State != store.StateError || !strings.HasPrefix(st.ErrorCode, "writer_") {
		t.Fatalf("status after race: %+v", st)
	}
}

func testConcurrentAppendsStayDense(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	const writers, perWriter = 20, 25
	seen := make(chan uint64, writers*perWriter)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				seq, err := s.Append(ctx, id, data(c, fmt.Sprintf("%d-%d", w, i)))
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				seen <- seq
			}
		}()
	}
	wg.Wait()
	close(seen)
	got := make(map[uint64]bool, writers*perWriter)
	for seq := range seen {
		if got[seq] {
			t.Fatalf("seq %d assigned twice", seq)
		}
		got[seq] = true
	}
	for seq := uint64(1); seq <= writers*perWriter; seq++ {
		if !got[seq] {
			t.Fatalf("seq %d never assigned", seq)
		}
	}
	evs, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil || len(evs) != writers*perWriter {
		t.Fatalf("Read: %d events, %v; want %d", len(evs), err, writers*perWriter)
	}
	want := uint64(1)
	for _, ev := range evs {
		if ev.Seq != want {
			t.Fatalf("event at position %d has seq %d: log out of order", want, ev.Seq)
		}
		want++
	}
}

func testCancelledRejectsNonForce(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	mustAppend(t, s, id, data(c, "a"))
	if err := s.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := s.Cancel(ctx, id); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}
	if st := mustStatus(t, s, id); !st.Cancelled || st.State != store.StateRunning {
		t.Fatalf("after Cancel: %+v", st)
	}
	if _, err := s.Append(ctx, id, data(c, "b")); !errors.Is(err, store.ErrCancelled) {
		t.Fatalf("non-forced append on cancelled stream: got %v, want ErrCancelled", err)
	}
	forcedData := data(c, "forced")
	forcedData.Force = true
	if seq := mustAppend(t, s, id, forcedData); seq != 2 {
		t.Fatalf("forced data append seq = %d, want 2", seq)
	}
	terminal := store.AppendRequest{Kind: "error", Data: []byte(`{"code":"cancelled"}`), At: c.Now(), Force: true}
	if seq := mustAppend(t, s, id, terminal); seq != 3 {
		t.Fatalf("forced terminal seq = %d, want 3", seq)
	}
	if err := s.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel on a terminal stream: %v", err)
	}
	if st := mustStatus(t, s, id); st.State != store.StateError || st.ErrorCode != "cancelled" {
		t.Fatalf("after cancelled terminal: %+v", st)
	}
}

// MaxEvents is a hard cap (ADR 0004): the log is never trimmed, a non-terminal append beyond the cap
// fails with ErrTooManyEvents and changes nothing, and the terminal append is still accepted.
func testMaxEventsIsAHardCap(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	const maxEvents = 10
	m := meta(c)
	m.MaxEvents = maxEvents
	mustCreate(t, s, id, m)
	for range maxEvents {
		mustAppend(t, s, id, data(c, "x"))
	}
	for _, req := range []store.AppendRequest{data(c, "one too many"), {Kind: "data", Data: []byte("forced"), At: c.Now(), Force: true}} {
		if _, err := s.Append(ctx, id, req); !errors.Is(err, store.ErrTooManyEvents) {
			t.Fatalf("append #%d (force=%v) beyond MaxEvents: got %v, want ErrTooManyEvents", maxEvents+1, req.Force, err)
		}
	}
	if st := mustStatus(t, s, id); st.LastSeq != maxEvents || st.FirstSeq != 1 || st.State != store.StateRunning {
		t.Fatalf("after rejected appends: %+v, want FirstSeq 1, LastSeq %d, running", st, maxEvents)
	}
	terminal := store.AppendRequest{Kind: "error", Data: []byte(`{"code":"max_events_exceeded"}`), At: c.Now(), Force: true}
	if seq := mustAppend(t, s, id, terminal); seq != maxEvents+1 {
		t.Fatalf("terminal seq = %d, want %d", seq, maxEvents+1)
	}
	evs, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil || len(evs) != maxEvents+1 || evs[0].Seq != 1 {
		t.Fatalf("Read(after=0) = %d events (first %v), %v; want the whole untrimmed log", len(evs), evs[:min(1, len(evs))], err)
	}
	if st := mustStatus(t, s, id); st.ErrorCode != "max_events_exceeded" || st.FirstSeq != 1 {
		t.Fatalf("after terminal: %+v", st)
	}
}

// An append with an expected Seq that is already stored with the same kind and data is the replay of
// a write whose reply was lost: it returns that Seq and changes nothing (ADR 0005), even once the
// stream is cancelled or terminal.
func testExpectedSeqReplayIsIdempotent(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	first := store.AppendRequest{Kind: "data", Data: []byte("token-1"), At: c.Now(), Seq: 1}
	second := store.AppendRequest{Kind: "meta", Data: []byte(`{"i":2}`), At: c.Now(), Seq: 2}
	for range 3 {
		if seq := mustAppend(t, s, id, first); seq != 1 {
			t.Fatalf("replayed append seq = %d, want 1", seq)
		}
	}
	if seq := mustAppend(t, s, id, second); seq != 2 {
		t.Fatalf("second append seq = %d, want 2", seq)
	}
	if seq := mustAppend(t, s, id, first); seq != 1 {
		t.Fatalf("late replay of seq 1 = %d, want 1", seq)
	}
	if err := s.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if seq := mustAppend(t, s, id, second); seq != 2 {
		t.Fatalf("replay on a cancelled stream = %d, want 2", seq)
	}
	mustAppend(t, s, id, store.AppendRequest{Kind: "done", Data: []byte("{}"), At: c.Now(), Force: true})
	if seq := mustAppend(t, s, id, second); seq != 2 {
		t.Fatalf("replay on a terminal stream = %d, want 2", seq)
	}
	evs, err := s.Read(ctx, id, 0, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	assertEvents(t, "log after replays", evs, []store.Event{
		{Seq: 1, Kind: "data", Data: []byte("token-1")},
		{Seq: 2, Kind: "meta", Data: []byte(`{"i":2}`)},
		{Seq: 3, Kind: "done", Data: []byte("{}")},
	})
}

// An expected Seq that does not match the log is a conflict, or ErrClosed once the stream is
// terminal; nothing is written either way.
func testExpectedSeqConflicts(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	mustAppend(t, s, id, store.AppendRequest{Kind: "data", Data: []byte("a"), At: c.Now(), Seq: 1})
	cases := map[string]store.AppendRequest{
		"same seq, other data": {Kind: "data", Data: []byte("b"), At: c.Now(), Seq: 1},
		"same seq, other kind": {Kind: "meta", Data: []byte("a"), At: c.Now(), Seq: 1},
		"seq ahead":            {Kind: "data", Data: []byte("c"), At: c.Now(), Seq: 3},
	}
	for name, req := range cases {
		if _, err := s.Append(ctx, id, req); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("%s: got %v, want ErrConflict", name, err)
		}
	}
	mustAppend(t, s, id, store.AppendRequest{Kind: "error", Data: []byte(`{"code":"producer_lost"}`), At: c.Now(), Force: true})
	if _, err := s.Append(ctx, id, store.AppendRequest{Kind: "data", Data: []byte("b"), At: c.Now(), Seq: 2}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("expected seq taken by the terminal: got %v, want ErrClosed", err)
	}
	if st := mustStatus(t, s, id); st.LastSeq != 2 {
		t.Fatalf("LastSeq = %d after rejected appends, want 2", st.LastSeq)
	}
}

func testIdleTTLExpiry(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	m := meta(c)
	m.IdleTTL = 2 * time.Second
	m.Retention = time.Hour
	mustCreate(t, s, id, m)

	c.Advance(1500 * time.Millisecond)
	mustAppend(t, s, id, data(c, "keeps it alive"))
	c.Advance(1500 * time.Millisecond)
	if err := s.Touch(ctx, id, c.Now()); err != nil {
		t.Fatalf("Touch within IdleTTL of the last append: %v", err)
	}
	c.Advance(1500 * time.Millisecond)
	mustStatus(t, s, id) // 1.5 s after the touch: still alive

	c.Advance(time.Second)
	if _, err := s.Status(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Status 2.5 s after the last activity: got %v, want ErrNotFound", err)
	}
	if _, err := s.Read(ctx, id, 0, 10, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Read of an expired stream: got %v, want ErrNotFound", err)
	}
	if err := s.Create(ctx, id, meta(c)); err != nil {
		t.Fatalf("Create after expiry reuses the id: %v", err)
	}
	if st := mustStatus(t, s, id); st.LastSeq != 0 {
		t.Fatalf("recreated stream kept old events: %+v", st)
	}
}

func testRetentionExpiry(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	m := meta(c)
	m.IdleTTL = time.Hour
	m.Retention = 2 * time.Second
	mustCreate(t, s, id, m)
	mustAppend(t, s, id, store.AppendRequest{Kind: "done", Data: []byte("{}"), At: c.Now()})

	c.Advance(1500 * time.Millisecond)
	if err := s.Touch(ctx, id, c.Now()); err != nil {
		t.Fatalf("Touch on a terminal stream: %v", err)
	}
	c.Advance(time.Second)
	if _, err := s.Status(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Status 2.5 s after done with Retention=2s: got %v, want ErrNotFound (Touch must not extend retention)", err)
	}
}

func testStatusConsistentAfterEachAppend(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	for i := range 20 {
		c.Advance(10 * time.Millisecond)
		req := data(c, fmt.Sprint(i))
		seq := mustAppend(t, s, id, req)
		st := mustStatus(t, s, id)
		if st.LastSeq != seq || st.FirstSeq != 1 || !sameMillis(st.LastActivity, req.At) {
			t.Fatalf("after append %d (seq %d, at %v): %+v", i, seq, req.At, st)
		}
	}
}

func testTouch(t *testing.T, s store.Store, c Clock) {
	ctx := context.Background()
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	at := c.Now().Add(42 * time.Second)
	if err := s.Touch(ctx, id, at); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if st := mustStatus(t, s, id); !sameMillis(st.LastActivity, at) || st.LastSeq != 0 {
		t.Fatalf("after Touch: %+v, want LastActivity %v and no event", st, at)
	}
}

func testCloseWakesBlockedReads(t *testing.T, s store.Store, c Clock) {
	id := newID(t)
	mustCreate(t, s, id, meta(c))
	res := asyncRead(s, id, 0, 10, time.Hour)
	waitBriefly() // give the read a chance to block; the assertion holds either way
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	awaitRead(t, res) // any result is fine, as long as it comes back
}

// --- helpers ---

type readResult struct {
	events []store.Event
	err    error
}

func asyncRead(s store.Store, id string, after uint64, limit int, block time.Duration) <-chan readResult {
	res := make(chan readResult, 1)
	go func() {
		evs, err := s.Read(context.Background(), id, after, limit, block)
		res <- readResult{evs, err}
	}()
	return res
}

func awaitRead(t *testing.T, res <-chan readResult) readResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(waitLimit):
		t.Fatal("Read did not return")
		return readResult{}
	}
}

// waitBriefly yields to other goroutines for a few milliseconds without time.Sleep.
func waitBriefly() {
	<-time.After(5 * time.Millisecond)
}

func meta(c Clock) store.Meta {
	return store.Meta{CreatedAt: c.Now(), IdleTTL: time.Hour, Retention: time.Hour, MaxEvents: 10_000}
}

func data(c Clock, s string) store.AppendRequest {
	return store.AppendRequest{Kind: "data", Data: []byte(s), At: c.Now()}
}

func newID(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random id: %v", err)
	}
	name := strings.NewReplacer("/", "-", " ", "_").Replace(t.Name())
	return name + "-" + hex.EncodeToString(b[:])
}

func mustCreate(t *testing.T, s store.Store, id string, m store.Meta) {
	t.Helper()
	if err := s.Create(context.Background(), id, m); err != nil {
		t.Fatalf("Create %s: %v", id, err)
	}
}

func mustAppend(t *testing.T, s store.Store, id string, req store.AppendRequest) uint64 {
	t.Helper()
	seq, err := s.Append(context.Background(), id, req)
	if err != nil {
		t.Fatalf("Append %s %q: %v", req.Kind, req.Data, err)
	}
	return seq
}

func mustStatus(t *testing.T, s store.Store, id string) store.Status {
	t.Helper()
	st, err := s.Status(context.Background(), id)
	if err != nil {
		t.Fatalf("Status %s: %v", id, err)
	}
	return st
}

func assertEvents(t *testing.T, what string, got, want []store.Event) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d events %v, want %d %v", what, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i].Seq != want[i].Seq || got[i].Kind != want[i].Kind || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("%s: event %d = {%d %s %q}, want {%d %s %q}", what, i,
				got[i].Seq, got[i].Kind, got[i].Data, want[i].Seq, want[i].Kind, want[i].Data)
		}
	}
}

// sameMillis compares times at millisecond precision, the resolution of the Redis store.
func sameMillis(a, b time.Time) bool {
	return a.UnixMilli() == b.UnixMilli()
}
