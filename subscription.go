package rebound

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bailalayoub/rebound/store"
)

// errSubscriptionClosed is returned by Next after Close.
var errSubscriptionClosed = errors.New("rebound: subscription closed")

// Subscription delivers a stream's events strictly after a cursor, replayed or live alike, up to
// and including the terminal event. One goroutine calls Next; Close may be called from any
// goroutine to stop it.
type Subscription struct {
	hub *Hub
	id  string

	// Owned by the goroutine calling Next.
	readCursor uint64  // last seq fetched from the store
	buf        []Event // fetched, not yet delivered
	eof        bool    // the terminal event has been delivered (or was before the cursor)
	// Liveness is observed, never computed from another host's clock (ADR 0006): the last LastSeq
	// and LastActivity seen, and when this reader last saw them change, on its own clock.
	seenSeq      uint64
	seenActivity time.Time
	changedAt    time.Time

	mu         sync.Mutex
	closed     bool
	cancelRead context.CancelFunc // interrupts the store read in progress, if any
}

// Subscribe returns a Subscription to the events of stream id with Seq > afterSeq; 0 means from
// the beginning. It fails with ErrNotFound for unknown, expired or malformed ids, ErrInvalidCursor
// if afterSeq is beyond the last event, and ErrGap if a store trimmed the events right after
// afterSeq (only possible on a terminal stream, ADR 0004).
func (h *Hub) Subscribe(ctx context.Context, id string, afterSeq uint64) (*Subscription, error) {
	if err := checkID(id); err != nil {
		return nil, fmt.Errorf("rebound: subscribe: %w", err)
	}
	st, err := h.store.Status(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("rebound: subscribe %s: %w", id, noEOF(err))
	}
	if afterSeq > st.LastSeq {
		return nil, fmt.Errorf("rebound: subscribe %s after %d (last seq %d): %w", id, afterSeq, st.LastSeq, ErrInvalidCursor)
	}
	if st.FirstSeq > 0 && afterSeq+1 < st.FirstSeq {
		return nil, fmt.Errorf("rebound: subscribe %s after %d (first retained %d): %w", id, afterSeq, st.FirstSeq, ErrGap)
	}
	h.hookSubscribe(id, afterSeq)
	return &Subscription{
		hub:          h,
		id:           id,
		readCursor:   afterSeq,
		eof:          st.State.IsTerminal() && afterSeq == st.LastSeq,
		seenSeq:      st.LastSeq,
		seenActivity: st.LastActivity,
		changedAt:    h.cfg.clock.Now(),
	}, nil
}

// Next blocks until the next event is available or ctx is done. After it has delivered the
// terminal event ("done" or "error") it returns io.EOF, and only then: no other error matches
// io.EOF. While waiting, it ends a stream whose producer showed no activity for StaleAfter with
// error "producer_lost".
func (s *Subscription) Next(ctx context.Context) (Event, error) {
	for {
		if len(s.buf) > 0 {
			ev := s.buf[0]
			s.buf = s.buf[1:]
			if store.IsTerminal(ev.Kind) {
				s.eof = true
				s.buf = nil
			}
			return ev, nil
		}
		if s.eof {
			return Event{}, io.EOF
		}
		evs, err := s.read(ctx)
		if err != nil {
			return Event{}, err
		}
		if len(evs) > 0 {
			s.buf = evs
			s.readCursor = evs[len(evs)-1].Seq
			continue
		}
		if err := s.checkLiveness(ctx); err != nil {
			return Event{}, err
		}
	}
}

// read performs one blocking store read that Close can interrupt.
func (s *Subscription) read(ctx context.Context) ([]Event, error) {
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errSubscriptionClosed
	}
	s.cancelRead = cancel
	s.mu.Unlock()

	evs, err := s.hub.store.Read(readCtx, s.id, s.readCursor, readBatch, s.hub.readBlock)

	s.mu.Lock()
	s.cancelRead = nil
	closed := s.closed
	s.mu.Unlock()
	switch {
	case closed:
		return nil, errSubscriptionClosed
	case err == nil:
		return evs, nil
	case ctx.Err() != nil:
		return nil, ctx.Err()
	default:
		return nil, fmt.Errorf("rebound: next %s after %d: %w", s.id, s.readCursor, noEOF(err))
	}
}

// noEOF keeps io.EOF out of the errors of Next, where it means "terminal event delivered": some
// store clients report a closed connection as io.EOF (ADR 0005).
func noEOF(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("store connection closed: " + err.Error())
	}
	return err
}

// checkLiveness runs when a read found nothing. If neither LastSeq nor LastActivity has changed
// for longer than StaleAfter of this reader's own clock, the producer is gone, so this reader
// writes the terminal event itself (G5). Timestamps written by other hosts are only compared for
// equality, so clock skew between instances cannot end a healthy stream (ADR 0006).
func (s *Subscription) checkLiveness(ctx context.Context) error {
	st, err := s.hub.store.Status(ctx, s.id)
	if err != nil {
		return fmt.Errorf("rebound: next %s: %w", s.id, noEOF(err))
	}
	if st.State.IsTerminal() {
		return nil // the next read returns the terminal event
	}
	now := s.hub.cfg.clock.Now()
	if st.LastSeq != s.seenSeq || !st.LastActivity.Equal(s.seenActivity) {
		s.seenSeq, s.seenActivity, s.changedAt = st.LastSeq, st.LastActivity, now
		return nil
	}
	silence := now.Sub(s.changedAt)
	if silence <= s.hub.cfg.staleAfter {
		return nil
	}
	s.hub.cfg.logger.Warn("rebound: producer lost", "stream_id", s.id, "reason", codeProducerLost,
		"duration_ms", silence.Milliseconds())
	s.hub.writeTerminal(ctx, s.id, errorEvent(codeProducerLost, ""))
	return nil
}

// Close ends the subscription and interrupts a Next in progress. It is idempotent. The stream
// itself is unaffected.
func (s *Subscription) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.cancelRead != nil {
		s.cancelRead()
	}
	s.mu.Unlock()
	s.hub.hookUnsubscribe(s.id)
	return nil
}
