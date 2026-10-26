package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/storetest"
)

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

// miniClock advances the suite's clock and miniredis's TTLs together.
type miniClock struct {
	*testutil.Clock
	mr *miniredis.Miniredis
}

func (c miniClock) Advance(d time.Duration) {
	c.mr.FastForward(d)
	c.Clock.Advance(d)
}

func newMini(t *testing.T, opts ...Option) (*Store, *miniredis.Miniredis, *testutil.Clock) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	c := testutil.NewClock(t0)
	s, err := New(client, append([]Option{WithClock(c)}, opts...)...)
	require.NoError(t, err)
	return s, mr, c
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (store.Store, storetest.Clock) {
		s, mr, c := newMini(t)
		return s, miniClock{Clock: c, mr: mr}
	})
}

func meta(c *testutil.Clock) store.Meta {
	return store.Meta{CreatedAt: c.Now(), IdleTTL: time.Hour, Retention: time.Hour, MaxEvents: 100}
}

// crc16 is the CRC16/XMODEM Redis Cluster uses for key slots.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc ^= uint16(c) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// keySlot implements the Redis Cluster rule: hash only the {tag} when the key has a non-empty one.
func keySlot(key string) uint16 {
	if open := strings.IndexByte(key, '{'); open >= 0 {
		if end := strings.IndexByte(key[open+1:], '}'); end > 0 {
			key = key[open+1 : open+1+end]
		}
	}
	return crc16([]byte(key)) % 16384
}

func TestKeys_ShareOneClusterSlot(t *testing.T) {
	t.Parallel()
	assert.Equal(t, uint16(12739), crc16([]byte("123456789"))%16384, "reference vector of the cluster spec")
	s, _, _ := newMini(t)
	for _, id := range []string{"a", "chat-1", "AbC_123-xyz.~", strings.Repeat("z", 128)} {
		keys := s.keys(id)
		require.Len(t, keys, 2)
		assert.Equal(t, "rebound:{"+id+"}:meta", keys[0])
		assert.Equal(t, "rebound:{"+id+"}:log", keys[1])
		assert.Equal(t, keySlot(keys[0]), keySlot(keys[1]), "id %q", id)
	}
}

func TestKeyPrefixAndLayout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, mr, c := newMini(t, WithKeyPrefix("app1"))
	require.NoError(t, s.Create(ctx, "s1", meta(c)))
	for range 3 {
		_, err := s.Append(ctx, "s1", store.AppendRequest{Kind: "data", Data: []byte("x"), At: c.Now()})
		require.NoError(t, err)
	}
	assert.True(t, mr.Exists("app1:{s1}:meta"))
	assert.True(t, mr.Exists("app1:{s1}:log"))
	assert.Equal(t, "3", mr.HGet("app1:{s1}:meta", "seq"))
	assert.True(t, mr.TTL("app1:{s1}:log") > 0, "the log carries the idle TTL")

	evs, err := s.Read(ctx, "s1", 0, 2, 0)
	require.NoError(t, err)
	assert.Len(t, evs, 2, "the limit applies")
	evs, err = s.Read(ctx, "s1", 0, 0, 0)
	require.NoError(t, err)
	assert.Len(t, evs, 3, "no limit given")
}

// Waiting reads beyond WithMaxBlockingReads poll instead of holding a pooled connection in
// XREAD BLOCK (ADR 0008); they still wake up when an event arrives.
// waitLimit bounds real-time waits in this file.
const waitLimit = 5 * time.Second

func TestBlockingReadsAreBounded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, c := newMini(t, WithMaxBlockingReads(1))
	require.Equal(t, 1, cap(s.blocking))
	require.NoError(t, s.Create(ctx, "s1", meta(c)))
	const readers = 4
	got := make(chan error, readers)
	for range readers {
		go func() {
			evs, err := s.Read(ctx, "s1", 0, 10, 10*time.Second)
			if err == nil && len(evs) != 1 {
				err = fmt.Errorf("got %d events", len(evs))
			}
			got <- err
		}()
	}
	testutil.Eventually(t, func() bool { return len(s.blocking) == 1 }, waitLimit) // one holds XREAD BLOCK
	_, err := s.Append(ctx, "s1", store.AppendRequest{Kind: "data", Data: []byte("x"), At: c.Now()})
	require.NoError(t, err)
	for range readers {
		select {
		case err := <-got:
			require.NoError(t, err)
		case <-time.After(waitLimit):
			t.Fatal("a waiting read did not wake up")
		}
	}
	assert.Zero(t, len(s.blocking), "every token released")
}

func TestScriptsSurviveScriptFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, c := newMini(t)
	require.NoError(t, s.Create(ctx, "a", meta(c)))
	require.NoError(t, s.client.ScriptFlush(ctx).Err())
	seq, err := s.Append(ctx, "a", store.AppendRequest{Kind: "data", Data: []byte("x")})
	require.NoError(t, err, "EVALSHA falls back to EVAL on NOSCRIPT")
	assert.Equal(t, uint64(1), seq)
}

func TestAppendWithoutTimestampUsesClock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, c := newMini(t)
	require.NoError(t, s.Create(ctx, "a", meta(c)))
	c.Advance(time.Second)
	_, err := s.Append(ctx, "a", store.AppendRequest{Kind: "data"})
	require.NoError(t, err)
	st, err := s.Status(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, c.Now(), st.LastActivity)
}

func TestNoTTLWhenZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, mr, c := newMini(t)
	require.NoError(t, s.Create(ctx, "forever", store.Meta{CreatedAt: c.Now()}))
	_, err := s.Append(ctx, "forever", store.AppendRequest{Kind: "data", Data: []byte("x")})
	require.NoError(t, err)
	assert.Zero(t, mr.TTL("rebound:{forever}:meta"))
	assert.Zero(t, mr.TTL("rebound:{forever}:log"))
	mr.FastForward(24 * time.Hour)
	_, err = s.Status(ctx, "forever")
	require.NoError(t, err)
}

func TestCreateClearsOrphanLog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, mr, c := newMini(t)
	_, err := mr.XAdd("rebound:{a}:log", "7-0", []string{"kind", "data", "data", "stale"})
	require.NoError(t, err)
	require.NoError(t, s.Create(ctx, "a", meta(c)))
	seq, err := s.Append(ctx, "a", store.AppendRequest{Kind: "data", Data: []byte("fresh")})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq)
}

func TestNewRejectsNilClient(t *testing.T) {
	t.Parallel()
	_, err := New(nil)
	require.Error(t, err)
	s, err := New(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0", PoolSize: 40}), WithKeyPrefix(""), WithMaxBlockingReads(0), WithClock(nil))
	require.NoError(t, err)
	assert.Equal(t, defaultKeyPrefix, s.prefix)
	assert.Equal(t, 20, s.maxBlockingReads, "half the pool of a *redis.Client")
	assert.Equal(t, store.RealClock{}, s.clock)
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{"127.0.0.1:0"}})
	t.Cleanup(func() { _ = cluster.Close() })
	s, err = New(cluster)
	require.NoError(t, err)
	assert.Equal(t, defaultBlockingReads, s.maxBlockingReads, "pool size unknown")
}

func TestClosedStoreRejectsEverything(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _, c := newMini(t)
	require.NoError(t, s.Create(ctx, "a", meta(c)))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "idempotent")
	require.ErrorIs(t, s.Create(ctx, "b", meta(c)), ErrStoreClosed)
	_, err := s.Append(ctx, "a", store.AppendRequest{Kind: "data"})
	require.ErrorIs(t, err, ErrStoreClosed)
	_, err = s.Read(ctx, "a", 0, 0, time.Hour)
	require.ErrorIs(t, err, ErrStoreClosed)
	_, err = s.Status(ctx, "a")
	require.ErrorIs(t, err, ErrStoreClosed)
	require.ErrorIs(t, s.Cancel(ctx, "a"), ErrStoreClosed)
	require.ErrorIs(t, s.Touch(ctx, "a", c.Now()), ErrStoreClosed)
}

func TestServerFailuresAreWrapped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mr := miniredis.RunT(t)
	// One attempt per command: retrying a dead server only slows the test down.
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	c := testutil.NewClock(t0)
	s, err := New(client, WithClock(c))
	require.NoError(t, err)
	require.NoError(t, s.Create(ctx, "a", meta(c)))
	mr.Close()

	check := func(op string, err error) {
		t.Helper()
		require.Error(t, err, op)
		assert.True(t, strings.HasPrefix(err.Error(), "redis store: "+op), "%s: %v", op, err)
		for _, sentinel := range []error{store.ErrNotFound, store.ErrClosed, store.ErrGap} {
			assert.NotErrorIs(t, err, sentinel, op)
		}
	}
	check("create", s.Create(ctx, "b", meta(c)))
	_, err = s.Append(ctx, "a", store.AppendRequest{Kind: "data"})
	check("append", err)
	_, err = s.Read(ctx, "a", 0, 0, 0)
	check("read", err)
	_, err = s.Status(ctx, "a")
	check("status", err)
	check("cancel", s.Cancel(ctx, "a"))
	check("touch", s.Touch(ctx, "a", c.Now()))
}

// A server that disappears while a read is blocked surfaces as an error, not a hang.
func TestBlockedReadFailsWhenServerGoesAway(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, mr, c := newMini(t)
	require.NoError(t, s.Create(ctx, "a", meta(c)))
	res := make(chan error, 1)
	go func() {
		_, err := s.Read(ctx, "a", 0, 10, time.Hour)
		res <- err
	}()
	testutil.Eventually(t, func() bool { return mr.CurrentConnectionCount() > 0 }, 5*time.Second)
	mr.Close()
	select {
	case err := <-res:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("blocked read did not return after the server went away")
	}
}

func TestCorruptDataIsReported(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, mr, c := newMini(t)
	require.NoError(t, s.Create(ctx, "a", meta(c)))

	mr.HSet("rebound:{a}:meta", "attrs", "{not json")
	_, err := s.Status(ctx, "a")
	require.ErrorContains(t, err, "malformed attrs")

	mr.HSet("rebound:{a}:meta", "attrs", "null", "seq", "x")
	_, err = s.Status(ctx, "a")
	require.ErrorContains(t, err, "malformed seq")
}

func TestParsers(t *testing.T) {
	t.Parallel()
	_, err := parseSeq("abc-0")
	require.Error(t, err)
	seq, err := parseSeq("42-0")
	require.NoError(t, err)
	assert.Equal(t, uint64(42), seq)

	_, err = parseEntries([]any{"not an entry"})
	require.Error(t, err)
	_, err = parseEntries([]any{[]any{"x-0", []any{}}})
	require.Error(t, err)
	evs, err := parseEntries([]any{[]any{"3-0", []any{"kind", "meta", "data", "d", "extra", "ignored"}}})
	require.NoError(t, err)
	assert.Equal(t, []store.Event{{Seq: 3, Kind: "meta", Data: []byte("d")}}, evs)

	_, err = parseStatus("a", []any{"seq", "1"}, "bad")
	require.Error(t, err)
	assert.True(t, fromMillis("nope").IsZero())

	assert.Equal(t, "timeout", errorCode([]byte(`{"code":"timeout"}`)))
	assert.Equal(t, "", errorCode([]byte(`nope`)))
}

func TestReadReturnsPromptlyOnCancel(t *testing.T) {
	t.Parallel()
	s, _, c := newMini(t)
	require.NoError(t, s.Create(context.Background(), "a", meta(c)))
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := s.Read(ctx, "a", 0, 10, time.Hour)
		res <- err
	}()
	cancel()
	require.ErrorIs(t, <-res, context.Canceled)
	assert.Less(t, time.Since(start), 2*time.Second, "bounded by one block slice")
}
