package redis

import (
	"context"
	_ "embed" // Lua scripts
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/bailalayoub/rebound/store"
)

// ErrStoreClosed is returned by every method after Close.
var ErrStoreClosed = errors.New("redis store: closed")

const (
	defaultKeyPrefix = "rebound"
	// unlimitedReadBatch caps a Read whose caller sets no limit.
	unlimitedReadBatch = 512
	// defaultBlockingReads is the WithMaxBlockingReads default when the pool size is unknown.
	defaultBlockingReads = 16
	// maxBlockSlice bounds one XREAD BLOCK. go-redis cannot interrupt a blocked command when its
	// context is cancelled, so long blocks are split into slices and ctx/Close are checked between
	// them: a cancelled read returns within this delay.
	maxBlockSlice = 250 * time.Millisecond
)

var (
	//go:embed lua/create.lua
	createSrc string
	//go:embed lua/append.lua
	appendSrc string
	//go:embed lua/read.lua
	readSrc string
	//go:embed lua/status.lua
	statusSrc string
	//go:embed lua/cancel.lua
	cancelSrc string
	//go:embed lua/touch.lua
	touchSrc string
)

// Store is a store.Store backed by Redis Streams. Create it with New.
//
// Each stream uses two keys sharing a hash tag, so they live on one Redis Cluster slot:
// "<prefix>:{<id>}:meta" (hash) and "<prefix>:{<id>}:log" (stream, entry ids "<seq>-0"). Every
// operation that reads and writes runs as one Lua script (EVALSHA with EVAL fallback), so it is
// atomic across any number of instances.
type Store struct {
	client           goredis.UniversalClient
	prefix           string
	clock            store.Clock
	maxBlockingReads int
	// blocking holds one token per read waiting in XREAD BLOCK, which pins a pooled connection.
	blocking chan struct{}

	create, append, read, status, cancel, touch *goredis.Script

	closeOnce sync.Once
	done      chan struct{} // closed by Close to wake blocked reads
}

var _ store.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// WithKeyPrefix sets the prefix of every key (default "rebound").
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) {
		if prefix != "" {
			s.prefix = prefix
		}
	}
}

// WithMaxBlockingReads bounds how many waiting reads hold a pooled connection in XREAD BLOCK at
// once (default: half the PoolSize of a *redis.Client, else 16). Other waiting reads poll every
// 250 ms without holding a connection, so readers cannot starve appends and heartbeats (ADR 0008).
// Give the client a pool larger than this bound.
func WithMaxBlockingReads(n int) Option {
	return func(s *Store) {
		if n > 0 {
			s.maxBlockingReads = n
		}
	}
}

// WithClock sets the clock used to stamp appends that carry no time (default store.RealClock).
// TTLs are enforced by Redis itself.
func WithClock(c store.Clock) Option {
	return func(s *Store) {
		if c != nil {
			s.clock = c
		}
	}
}

// New returns a Store using client, which may be a single node, a Sentinel failover client or a
// Cluster client. The caller owns the client: Close does not close it.
func New(client goredis.UniversalClient, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, errors.New("redis store: nil client")
	}
	s := &Store{
		client: client,
		prefix: defaultKeyPrefix,
		clock:  store.RealClock{},
		create: goredis.NewScript(createSrc),
		append: goredis.NewScript(appendSrc),
		read:   goredis.NewScript(readSrc),
		status: goredis.NewScript(statusSrc),
		cancel: goredis.NewScript(cancelSrc),
		touch:  goredis.NewScript(touchSrc),
		done:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.maxBlockingReads == 0 {
		s.maxBlockingReads = defaultBlockingReads
		if c, ok := client.(*goredis.Client); ok {
			s.maxBlockingReads = max(c.Options().PoolSize/2, 1)
		}
	}
	s.blocking = make(chan struct{}, s.maxBlockingReads)
	return s, nil
}

// noEOF keeps io.EOF out of the error chain: go-redis reports a closed connection as io.EOF, which
// means "end of stream" to readers (spec §6, ADR 0005).
func noEOF(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("connection closed: " + err.Error())
	}
	return err
}

// keys returns the meta and log keys of a stream, sharing the hash tag {id}.
func (s *Store) keys(id string) []string {
	tag := s.prefix + ":{" + id + "}"
	return []string{tag + ":meta", tag + ":log"}
}

func (s *Store) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Create registers a new running stream, or returns store.ErrExists.
func (s *Store) Create(ctx context.Context, id string, m store.Meta) error {
	if s.closed() {
		return ErrStoreClosed
	}
	attrs, err := json.Marshal(m.Attrs)
	if err != nil {
		return fmt.Errorf("redis store: create %s: encode attrs: %w", id, err)
	}
	created, err := s.create.Run(ctx, s.client, s.keys(id),
		m.CreatedAt.UnixMilli(), m.IdleTTL.Milliseconds(), m.Retention.Milliseconds(), m.MaxEvents, attrs).Int()
	if err != nil {
		return fmt.Errorf("redis store: create %s: %w", id, noEOF(err))
	}
	if created == 0 {
		return fmt.Errorf("redis store: create %s: %w", id, store.ErrExists)
	}
	return nil
}

// Append stores ev with the next sequence number; see store.Store.
func (s *Store) Append(ctx context.Context, id string, ev store.AppendRequest) (uint64, error) {
	if s.closed() {
		return 0, ErrStoreClosed
	}
	at := ev.At
	if at.IsZero() {
		at = s.clock.Now()
	}
	force := "0"
	if ev.Force {
		force = "1"
	}
	terminalState, code := "", ""
	if store.IsTerminal(ev.Kind) {
		terminalState = string(store.StateDone)
		if ev.Kind == store.KindError {
			terminalState, code = string(store.StateError), errorCode(ev.Data)
		}
	}
	seq, err := s.append.Run(ctx, s.client, s.keys(id),
		ev.Kind, ev.Data, at.UnixMilli(), force, terminalState, code, ev.Seq).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis store: append %s: %w", id, noEOF(err))
	}
	switch seq {
	case -1:
		return 0, fmt.Errorf("redis store: append %s: %w", id, store.ErrNotFound)
	case -2:
		return 0, fmt.Errorf("redis store: append %s: %w", id, store.ErrClosed)
	case -3:
		return 0, fmt.Errorf("redis store: append %s: %w", id, store.ErrCancelled)
	case -4:
		return 0, fmt.Errorf("redis store: append %s: %w", id, store.ErrTooManyEvents)
	case -5:
		return 0, fmt.Errorf("redis store: append %s seq %d: %w", id, ev.Seq, store.ErrConflict)
	}
	return uint64(seq), nil //nolint:gosec // seq is a positive HINCRBY result here
}

// Read returns events after afterSeq, blocking up to block when none is available yet. The
// events always come from one atomic script (existence, gap and range checked together); XREAD
// BLOCK only serves as the wake-up signal for new appends.
func (s *Store) Read(ctx context.Context, id string, afterSeq uint64, limit int, block time.Duration) ([]store.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = unlimitedReadBatch
	}
	evs, final, err := s.readOnce(ctx, id, afterSeq, limit)
	if err != nil || len(evs) > 0 || final || block <= 0 {
		return evs, err
	}
	deadline := time.Now().Add(block)
	for {
		slice := min(time.Until(deadline), maxBlockSlice)
		if slice < time.Millisecond {
			return nil, nil
		}
		if err := s.wait(ctx, id, afterSeq, slice); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.done:
			return nil, ErrStoreClosed
		default:
		}
		evs, final, err := s.readOnce(ctx, id, afterSeq, limit)
		if err != nil || len(evs) > 0 || final {
			return evs, err
		}
	}
}

// wait returns when an event may have been appended after afterSeq, or after slice. At most
// maxBlockingReads waits hold a pooled connection in XREAD BLOCK; the others wait for the slice
// without one, then re-read.
func (s *Store) wait(ctx context.Context, id string, afterSeq uint64, slice time.Duration) error {
	select {
	case s.blocking <- struct{}{}:
		defer func() { <-s.blocking }()
		cursor := strconv.FormatUint(afterSeq, 10) + "-0"
		err := s.client.XRead(ctx, &goredis.XReadArgs{Streams: []string{s.keys(id)[1], cursor}, Count: 1, Block: slice}).Err()
		if err != nil && !errors.Is(err, goredis.Nil) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("redis store: read %s: wait: %w", id, noEOF(err))
		}
		return nil
	default:
	}
	t := time.NewTimer(slice)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrStoreClosed
	}
}

// readOnce runs read.lua. final is true when the stream is terminal with nothing after afterSeq.
func (s *Store) readOnce(ctx context.Context, id string, afterSeq uint64, limit int) (evs []store.Event, final bool, err error) {
	if s.closed() {
		return nil, false, ErrStoreClosed
	}
	from := strconv.FormatUint(afterSeq+1, 10) + "-0"
	res, err := s.read.Run(ctx, s.client, s.keys(id), from, limit, afterSeq).Result()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, false, ctxErr
		}
		return nil, false, fmt.Errorf("redis store: read %s: %w", id, noEOF(err))
	}
	if code, ok := res.(int64); ok && code == -1 {
		return nil, false, fmt.Errorf("redis store: read %s: %w", id, store.ErrNotFound)
	}
	parts, ok := res.([]any)
	if !ok || len(parts) < 2 {
		return nil, false, fmt.Errorf("redis store: read %s: unexpected reply %T", id, res)
	}
	if code, ok := parts[0].(int64); ok && code == -2 {
		return nil, false, fmt.Errorf("redis store: read %s after %d (first retained %v): %w", id, afterSeq, parts[1], store.ErrGap)
	}
	if len(parts) != 3 {
		return nil, false, fmt.Errorf("redis store: read %s: unexpected reply length %d", id, len(parts))
	}
	state, _ := parts[0].(string)
	lastSeq, _ := parts[1].(int64)
	entries, _ := parts[2].([]any)
	evs, err = parseEntries(entries)
	if err != nil {
		return nil, false, fmt.Errorf("redis store: read %s: %w", id, err)
	}
	final = len(evs) == 0 && store.State(state).IsTerminal() && int64(afterSeq) >= lastSeq //nolint:gosec // afterSeq is a cursor below 2^63
	return evs, final, nil
}

// Status returns a snapshot of the stream's metadata.
func (s *Store) Status(ctx context.Context, id string) (store.Status, error) {
	if s.closed() {
		return store.Status{}, ErrStoreClosed
	}
	res, err := s.status.Run(ctx, s.client, s.keys(id)).Result()
	if err != nil {
		return store.Status{}, fmt.Errorf("redis store: status %s: %w", id, noEOF(err))
	}
	if code, ok := res.(int64); ok && code == -1 {
		return store.Status{}, fmt.Errorf("redis store: status %s: %w", id, store.ErrNotFound)
	}
	parts, ok := res.([]any)
	if !ok || len(parts) != 2 {
		return store.Status{}, fmt.Errorf("redis store: status %s: unexpected reply %T", id, res)
	}
	fields, _ := parts[0].([]any)
	firstID, _ := parts[1].(string)
	st, err := parseStatus(id, fields, firstID)
	if err != nil {
		return store.Status{}, fmt.Errorf("redis store: status %s: %w", id, err)
	}
	return st, nil
}

// Cancel sets the cancelled flag.
func (s *Store) Cancel(ctx context.Context, id string) error {
	if s.closed() {
		return ErrStoreClosed
	}
	res, err := s.cancel.Run(ctx, s.client, s.keys(id)[:1]).Int()
	if err != nil {
		return fmt.Errorf("redis store: cancel %s: %w", id, noEOF(err))
	}
	if res == -1 {
		return fmt.Errorf("redis store: cancel %s: %w", id, store.ErrNotFound)
	}
	return nil
}

// Touch records liveness of a running stream; it does nothing on a terminal one.
func (s *Store) Touch(ctx context.Context, id string, at time.Time) error {
	if s.closed() {
		return ErrStoreClosed
	}
	res, err := s.touch.Run(ctx, s.client, s.keys(id), at.UnixMilli()).Int()
	if err != nil {
		return fmt.Errorf("redis store: touch %s: %w", id, noEOF(err))
	}
	if res == -1 {
		return fmt.Errorf("redis store: touch %s: %w", id, store.ErrNotFound)
	}
	return nil
}

// Close wakes blocked reads and makes every later call fail with ErrStoreClosed. It does not close
// the Redis client, which the caller owns. It is idempotent.
func (s *Store) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}

// parseEntries converts XRANGE entries ({id, {field, value, …}}) to events.
func parseEntries(entries []any) ([]store.Event, error) {
	evs := make([]store.Event, 0, len(entries))
	for _, raw := range entries {
		entry, ok := raw.([]any)
		if !ok || len(entry) != 2 {
			return nil, fmt.Errorf("malformed stream entry %v", raw)
		}
		id, _ := entry[0].(string)
		seq, err := parseSeq(id)
		if err != nil {
			return nil, err
		}
		fields, _ := entry[1].([]any)
		ev := store.Event{Seq: seq}
		for i := 0; i+1 < len(fields); i += 2 {
			name, _ := fields[i].(string)
			value, _ := fields[i+1].(string)
			switch name {
			case "kind":
				ev.Kind = value
			case "data":
				ev.Data = []byte(value)
			}
		}
		evs = append(evs, ev)
	}
	return evs, nil
}

// parseSeq extracts seq from a stream entry id "<seq>-0".
func parseSeq(id string) (uint64, error) {
	ms, _, _ := strings.Cut(id, "-")
	seq, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed entry id %q", id)
	}
	return seq, nil
}

// parseStatus decodes the HGETALL reply of a meta hash.
func parseStatus(id string, fields []any, firstID string) (store.Status, error) {
	m := make(map[string]string, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		k, _ := fields[i].(string)
		v, _ := fields[i+1].(string)
		m[k] = v
	}
	st := store.Status{
		ID:           id,
		State:        store.State(m["state"]),
		ErrorCode:    m["error_code"],
		Cancelled:    m["cancelled"] == "1",
		CreatedAt:    fromMillis(m["created_ms"]),
		LastActivity: fromMillis(m["last_activity_ms"]),
	}
	var err error
	if st.LastSeq, err = strconv.ParseUint(m["seq"], 10, 64); err != nil {
		return store.Status{}, fmt.Errorf("malformed seq %q", m["seq"])
	}
	if firstID != "" {
		if st.FirstSeq, err = parseSeq(firstID); err != nil {
			return store.Status{}, err
		}
	}
	if attrs := m["attrs"]; attrs != "" && attrs != "null" {
		if err := json.Unmarshal([]byte(attrs), &st.Meta); err != nil {
			return store.Status{}, fmt.Errorf("malformed attrs: %w", err)
		}
	}
	return st, nil
}

// fromMillis parses a Unix time in milliseconds; an unparsable value gives the zero time.
func fromMillis(s string) time.Time {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
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
