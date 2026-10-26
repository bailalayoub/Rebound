package memory

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/bailalayoub/rebound/store"
)

// ErrStoreClosed is returned by every method after Close.
var ErrStoreClosed = errors.New("memory store: closed")

// defaultSweepInterval is how often, in clock time, Create removes expired streams.
const defaultSweepInterval = time.Minute

// Store is an in-memory store.Store. Create it with New.
//
// Expiry is lazy: a stream whose TTL has passed reads as unknown immediately, and expired entries
// are removed by a sweep that Create runs at most once per sweep interval. There is no background
// goroutine, so nothing has to be stopped and tests on a manual clock stay deterministic.
type Store struct {
	clock      store.Clock
	sweepEvery time.Duration

	mu        sync.RWMutex
	streams   map[string]*stream
	lastSweep time.Time
	closed    bool
	done      chan struct{} // closed by Close to wake blocked reads
}

var _ store.Store = (*Store)(nil)

// stream is the state of one stream; every field is guarded by Store.mu.
type stream struct {
	meta         store.Meta
	state        store.State
	errorCode    string
	cancelled    bool
	events       []store.Event // contiguous, seq firstSeq..lastSeq
	firstSeq     uint64
	lastSeq      uint64
	lastActivity time.Time
	expiresAt    time.Time     // zero: never
	notify       chan struct{} // closed and replaced on every append
}

// Option configures a Store.
type Option func(*Store)

// WithClock sets the clock used for TTLs and blocking reads (default store.RealClock).
func WithClock(c store.Clock) Option {
	return func(s *Store) {
		if c != nil {
			s.clock = c
		}
	}
}

// WithSweepInterval sets how often Create removes expired streams (default 1 minute).
func WithSweepInterval(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.sweepEvery = d
		}
	}
}

// New returns an empty in-memory store.
func New(opts ...Option) *Store {
	s := &Store{
		clock:      store.RealClock{},
		sweepEvery: defaultSweepInterval,
		streams:    make(map[string]*stream),
		done:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.lastSweep = s.clock.Now()
	return s
}

// Create registers a new running stream, or returns store.ErrExists.
func (s *Store) Create(_ context.Context, id string, m store.Meta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	now := s.clock.Now()
	s.sweepLocked(now)
	if _, ok := s.liveLocked(id, now); ok {
		return fmt.Errorf("memory store: create %s: %w", id, store.ErrExists)
	}
	m.Attrs = maps.Clone(m.Attrs)
	s.streams[id] = &stream{
		meta:         m,
		state:        store.StateRunning,
		lastActivity: m.CreatedAt,
		expiresAt:    deadline(now, m.IdleTTL),
		notify:       make(chan struct{}),
	}
	return nil
}

// Append stores ev with the next sequence number; see store.Store.
func (s *Store) Append(_ context.Context, id string, ev store.AppendRequest) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}
	now := s.clock.Now()
	st, ok := s.liveLocked(id, now)
	if !ok {
		return 0, fmt.Errorf("memory store: append %s: %w", id, store.ErrNotFound)
	}
	if ev.Seq > 0 && ev.Seq <= st.lastSeq {
		return st.replay(id, ev)
	}
	switch {
	case st.state.IsTerminal():
		return 0, fmt.Errorf("memory store: append %s: %w", id, store.ErrClosed)
	case ev.Seq > st.lastSeq+1:
		return 0, fmt.Errorf("memory store: append %s seq %d after %d: %w", id, ev.Seq, st.lastSeq, store.ErrConflict)
	case st.cancelled && !ev.Force:
		return 0, fmt.Errorf("memory store: append %s: %w", id, store.ErrCancelled)
	case !store.IsTerminal(ev.Kind) && st.meta.MaxEvents > 0 && st.lastSeq >= uint64(st.meta.MaxEvents):
		return 0, fmt.Errorf("memory store: append %s: %d events: %w", id, st.lastSeq, store.ErrTooManyEvents)
	}

	st.lastSeq++
	st.events = append(st.events, store.Event{Seq: st.lastSeq, Kind: ev.Kind, Data: slices.Clone(ev.Data)})
	if st.firstSeq == 0 {
		st.firstSeq = st.lastSeq
	}
	st.lastActivity = ev.At
	if ev.At.IsZero() {
		st.lastActivity = now
	}
	if store.IsTerminal(ev.Kind) {
		st.state = store.StateDone
		if ev.Kind == store.KindError {
			st.state = store.StateError
			st.errorCode = errorCode(ev.Data)
		}
		st.expiresAt = deadline(now, st.meta.Retention)
	} else {
		st.expiresAt = deadline(now, st.meta.IdleTTL)
	}
	close(st.notify)
	st.notify = make(chan struct{})
	return st.lastSeq, nil
}

// Read returns events after afterSeq, blocking up to block when none is available yet.
func (s *Store) Read(ctx context.Context, id string, afterSeq uint64, limit int, block time.Duration) ([]store.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	evs, notify, final, err := s.readOnce(id, afterSeq, limit)
	if err != nil || len(evs) > 0 || final || block <= 0 {
		return evs, err
	}
	timer := s.clock.NewTimer(block)
	defer timer.Stop()
	for {
		select {
		case <-notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C():
			return nil, nil
		case <-s.done:
			return nil, ErrStoreClosed
		}
		evs, notify, final, err = s.readOnce(id, afterSeq, limit)
		if err != nil || len(evs) > 0 || final {
			return evs, err
		}
	}
}

// readOnce returns the available events, the channel that the next append closes, and whether
// the stream is terminal with nothing left after afterSeq (no event can ever arrive).
func (s *Store) readOnce(id string, afterSeq uint64, limit int) (evs []store.Event, notify <-chan struct{}, final bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, nil, false, ErrStoreClosed
	}
	st, ok := s.liveLocked(id, s.clock.Now())
	if !ok {
		return nil, nil, false, fmt.Errorf("memory store: read %s: %w", id, store.ErrNotFound)
	}
	if st.firstSeq > 0 && afterSeq+1 < st.firstSeq {
		return nil, nil, false, fmt.Errorf("memory store: read %s after %d (first retained %d): %w",
			id, afterSeq, st.firstSeq, store.ErrGap)
	}
	if afterSeq >= st.lastSeq {
		return nil, st.notify, st.state.IsTerminal(), nil
	}
	start, _ := slices.BinarySearchFunc(st.events, afterSeq+1, func(ev store.Event, seq uint64) int {
		return cmp.Compare(ev.Seq, seq)
	})
	end := len(st.events)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	evs = make([]store.Event, 0, end-start)
	for _, ev := range st.events[start:end] {
		evs = append(evs, store.Event{Seq: ev.Seq, Kind: ev.Kind, Data: slices.Clone(ev.Data)})
	}
	return evs, st.notify, false, nil
}

// Status returns a snapshot of the stream's metadata.
func (s *Store) Status(_ context.Context, id string) (store.Status, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return store.Status{}, ErrStoreClosed
	}
	st, ok := s.liveLocked(id, s.clock.Now())
	if !ok {
		return store.Status{}, fmt.Errorf("memory store: status %s: %w", id, store.ErrNotFound)
	}
	return store.Status{
		ID:           id,
		State:        st.state,
		ErrorCode:    st.errorCode,
		FirstSeq:     st.firstSeq,
		LastSeq:      st.lastSeq,
		Cancelled:    st.cancelled,
		CreatedAt:    st.meta.CreatedAt,
		LastActivity: st.lastActivity,
		Meta:         maps.Clone(st.meta.Attrs),
	}, nil
}

// Cancel sets the cancelled flag.
func (s *Store) Cancel(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	st, ok := s.liveLocked(id, s.clock.Now())
	if !ok {
		return fmt.Errorf("memory store: cancel %s: %w", id, store.ErrNotFound)
	}
	st.cancelled = true
	return nil
}

// Touch records liveness of a running stream; it does nothing on a terminal one.
func (s *Store) Touch(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	now := s.clock.Now()
	st, ok := s.liveLocked(id, now)
	if !ok {
		return fmt.Errorf("memory store: touch %s: %w", id, store.ErrNotFound)
	}
	if st.state.IsTerminal() {
		return nil
	}
	st.lastActivity = at
	st.expiresAt = deadline(now, st.meta.IdleTTL)
	return nil
}

// Close drops every stream and wakes blocked reads. It is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.streams = nil
		close(s.done)
	}
	return nil
}

// liveLocked returns the stream if it exists and has not expired at now.
func (s *Store) liveLocked(id string, now time.Time) (*stream, bool) {
	st := s.streams[id]
	if st == nil || st.expired(now) {
		return nil, false
	}
	return st, true
}

// sweepLocked removes expired streams, at most once per sweep interval.
func (s *Store) sweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < s.sweepEvery {
		return
	}
	s.lastSweep = now
	maps.DeleteFunc(s.streams, func(_ string, st *stream) bool { return st.expired(now) })
}

func (st *stream) expired(now time.Time) bool {
	return !st.expiresAt.IsZero() && !now.Before(st.expiresAt)
}

// replay answers an append whose expected seq is already in the log (ADR 0005): the same event
// again is the retry of a write whose reply was lost, anything else conflicts.
func (st *stream) replay(id string, ev store.AppendRequest) (uint64, error) {
	stored := st.events[ev.Seq-st.firstSeq] // never trimmed: firstSeq is 1
	if stored.Kind == ev.Kind && bytes.Equal(stored.Data, ev.Data) {
		return ev.Seq, nil
	}
	if st.state.IsTerminal() {
		return 0, fmt.Errorf("memory store: append %s: %w", id, store.ErrClosed)
	}
	return 0, fmt.Errorf("memory store: append %s seq %d: %w", id, ev.Seq, store.ErrConflict)
}

// deadline returns now+ttl, or the zero time (never) when ttl is not positive.
func deadline(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

// errorCode extracts the "code" field of a terminal error payload (spec §5).
func errorCode(data []byte) string {
	var payload struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		// The payload is opaque to the store; one without a JSON code simply records no code.
		return ""
	}
	return payload.Code
}
