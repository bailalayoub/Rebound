//go:build integration

package core

// Chaos regression tests against a real Redis (REDIS_ADDR; every test uses its own key prefix).
// A TCP proxy in front of Redis simulates a restart (every connection dropped, new ones refused),
// latency, a hung server (requests forwarded, replies discarded) and replies lost on a dropped
// connection.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	redisstore "github.com/bailalayoub/rebound/store/redis"
)

var chaosPrefixes atomic.Int64

func chaosPrefix() string {
	return fmt.Sprintf("chaos%d-%d", time.Now().UnixNano(), chaosPrefixes.Add(1))
}

// chaosProxy forwards TCP connections to a Redis server and injects faults.
type chaosProxy struct {
	ln     net.Listener
	target string

	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	down      bool          // refuse new connections, drop existing ones
	blackhole bool          // forward requests, discard replies
	dropReply bool          // forward requests, drop the connection when the reply arrives
	latency   time.Duration // added before forwarding each chunk, both ways
}

func newChaosProxy(t *testing.T, target string) *chaosProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &chaosProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close() // test teardown
		p.dropAll()
	})
	return p
}

func (p *chaosProxy) addr() string { return p.ln.Addr().String() }

func (p *chaosProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			_ = c.Close() // a restarting server: connection refused/reset
			continue
		}
		s, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = c.Close() // upstream unavailable
			continue
		}
		p.mu.Lock()
		p.conns[c], p.conns[s] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(s, c, false)
		go p.pipe(c, s, true)
	}
}

func (p *chaosProxy) pipe(dst, src net.Conn, reply bool) {
	defer func() {
		_, _ = dst.Close(), src.Close()
		p.mu.Lock()
		delete(p.conns, dst)
		delete(p.conns, src)
		p.mu.Unlock()
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		lat, blackhole, dropReply, down := p.latency, p.blackhole, p.dropReply, p.down
		p.mu.Unlock()
		if down || (reply && dropReply) {
			return
		}
		if reply && blackhole {
			continue
		}
		if lat > 0 {
			time.Sleep(lat)
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (p *chaosProxy) set(f func(*chaosProxy)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(p)
}

// restart simulates a Redis restart: every connection is dropped and new ones are refused until up.
func (p *chaosProxy) restart() {
	p.set(func(p *chaosProxy) { p.down = true })
	p.dropAll()
}

func (p *chaosProxy) up() { p.set(func(p *chaosProxy) { p.down = false }) }

func (p *chaosProxy) dropAll() {
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close() // simulated failure
	}
}

func chaosClient(t *testing.T, opts *goredis.Options) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func chaosStoreOn(t *testing.T, client *goredis.Client, prefix string, opts ...redisstore.Option) *redisstore.Store {
	t.Helper()
	s, err := redisstore.New(client, append([]redisstore.Option{redisstore.WithKeyPrefix(prefix)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func chaosHub(t *testing.T, s store.Store, opts ...Option) *Hub {
	t.Helper()
	hub, err := New(s, append([]Option{WithLogger(quietLogger())}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hub.Close(ctx) // teardown
	})
	return hub
}

// ADR 0005, store level: when the reply of an append is lost (timeout, or connection dropped
// after Redis ran the script), the event is stored although Append failed; retrying the same
// request with its expected Seq returns that seq and stores nothing more.
func TestChaos_RedisRetryAfterLostReplyStoresOnce(t *testing.T) {
	addr := redisAddr()
	for _, fault := range []string{"reply lost (timeout)", "connection dropped after the request"} {
		t.Run(fault, func(t *testing.T) {
			p := newChaosProxy(t, addr)
			prefix := chaosPrefix()
			s := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: p.addr(), ReadTimeout: 300 * time.Millisecond}), prefix)
			direct := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: addr}), prefix)
			ctx := context.Background()

			require.NoError(t, s.Create(ctx, "dup", store.Meta{CreatedAt: time.Now(), IdleTTL: time.Minute, Retention: time.Minute}))
			_, err := s.Append(ctx, "dup", store.AppendRequest{Kind: "data", Data: []byte("token-1"), Seq: 1})
			require.NoError(t, err)

			second := store.AppendRequest{Kind: "data", Data: []byte("token-2"), Seq: 2}
			p.set(func(p *chaosProxy) { p.blackhole, p.dropReply = fault[0] == 'r', fault[0] == 'c' })
			_, err = s.Append(ctx, "dup", second)
			p.set(func(p *chaosProxy) { p.blackhole, p.dropReply = false, false })
			require.Error(t, err, "the reply was lost")
			assert.NotErrorIs(t, err, io.EOF, "no io.EOF from the store (ADR 0005)")

			seq, err := s.Append(ctx, "dup", second) // what the Writer does
			require.NoError(t, err)
			assert.Equal(t, uint64(2), seq)
			evs, err := direct.Read(ctx, "dup", 0, 0, 0)
			require.NoError(t, err)
			require.Len(t, evs, 2, "token-2 stored exactly once: %v", evs)
			assert.Equal(t, "token-2", string(evs[1].Data))
		})
	}
}

// ADR 0005, Hub level: a Write whose replies are lost for a while is retried by the Writer
// and succeeds; the token is in the log exactly once.
func TestChaos_RedisWriteSurvivesLostRepliesWithoutDuplicate(t *testing.T) {
	addr := redisAddr()
	p := newChaosProxy(t, addr)
	prefix := chaosPrefix()
	s := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: p.addr(), ReadTimeout: 300 * time.Millisecond}), prefix)
	direct := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: addr}), prefix)
	hub := chaosHub(t, s, WithStaleAfter(10*time.Second))
	ctx := context.Background()

	step := make(chan struct{})
	written := make(chan error, 1)
	st, err := hub.Start(ctx, func(_ context.Context, w Writer) error {
		if err := w.Write([]byte("token-1")); err != nil {
			return err
		}
		<-step
		written <- w.Write([]byte("token-2"))
		return nil
	})
	require.NoError(t, err)
	testutil.Eventually(t, func() bool {
		got, err := direct.Status(ctx, st.ID)
		return err == nil && got.LastSeq == 1
	}, 5*time.Second)

	p.set(func(p *chaosProxy) { p.blackhole = true }) // Redis runs the appends; the replies are lost
	close(step)
	<-time.After(time.Second)
	p.set(func(p *chaosProxy) { p.blackhole = false })
	require.NoError(t, <-written, "the Writer retried until a reply came back")

	testutil.Eventually(t, func() bool {
		got, err := direct.Status(ctx, st.ID)
		return err == nil && got.State.IsTerminal()
	}, 10*time.Second)
	evs, err := direct.Read(ctx, st.ID, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, evs, 3, "token-1, token-2 once, done: %v", evs)
	assert.Equal(t, []string{"token-1", "token-2", "{}"}, []string{string(evs[0].Data), string(evs[1].Data), string(evs[2].Data)})
}

// ADR 0005: a Redis restart under a waiting reader is an error for Next, never io.EOF, the
// end-of-stream signal of the documented consumer loop.
func TestChaos_RedisConnectionDropIsNotEndOfStream(t *testing.T) {
	p := newChaosProxy(t, redisAddr())
	hub := chaosHub(t, chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: p.addr()}), chaosPrefix()))
	ctx := context.Background()
	st, err := hub.Start(ctx, func(pctx context.Context, w Writer) error {
		if err := w.Write([]byte("first")); err != nil {
			return err
		}
		<-pctx.Done()
		return nil
	})
	require.NoError(t, err)
	sub, err := hub.Subscribe(ctx, st.ID, 0)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	ev, err := sub.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "first", string(ev.Data))

	got := make(chan error, 1)
	go func() {
		_, err := sub.Next(ctx) // waits in XREAD BLOCK
		got <- err
	}()
	<-time.After(300 * time.Millisecond)
	p.restart() // connections dropped and refused: go-redis's own retries fail too
	defer p.up()
	select {
	case err = <-got:
	case <-time.After(30 * time.Second):
		t.Fatal("Next did not return after the Redis restart")
	}
	require.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF), "Next returned %q, which matches io.EOF", err)
}

// clientOutcome is what readLikeAClient observed.
type clientOutcome struct {
	seqs     int
	terminal string
	notFound bool
	errs     []string
	gapDup   []string
}

// readLikeAClient follows a stream like the TypeScript client: resume after the last seq on any
// error, stop at the terminal event or on ErrNotFound. It checks G1/G2 across reconnections.
func readLikeAClient(hub *Hub, id string, budget time.Duration) clientOutcome {
	var o clientOutcome
	ctx := context.Background()
	cursor := uint64(0)
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		sub, err := hub.Subscribe(ctx, id, cursor)
		if err != nil {
			o.errs = append(o.errs, err.Error())
			if errors.Is(err, ErrNotFound) {
				o.notFound = true
				return o
			}
			<-time.After(100 * time.Millisecond)
			continue
		}
		for {
			nctx, cancel := context.WithDeadline(ctx, deadline)
			ev, err := sub.Next(nctx)
			cancel()
			if errors.Is(err, io.EOF) {
				_ = sub.Close()
				if o.terminal == "" {
					o.errs = append(o.errs, "io.EOF before the terminal event")
				}
				return o
			}
			if err != nil {
				o.errs = append(o.errs, err.Error())
				break
			}
			if ev.Seq != cursor+1 {
				o.gapDup = append(o.gapDup, fmt.Sprintf("after %d got %d", cursor, ev.Seq))
			}
			cursor = ev.Seq
			o.seqs++
			if IsTerminal(ev.Kind) {
				o.terminal = ev.Kind
			}
		}
		_ = sub.Close()
		<-time.After(100 * time.Millisecond)
	}
	o.errs = append(o.errs, "gave up: budget exhausted")
	return o
}

// Redis restarts 1 s into a 3 s generation and is back 2 s later. With its data
// (persistence), the generation completes: the Writer's retries (StaleAfter/2 = 5 s) cover the
// outage, and readers resume to the end with no gap or duplicate. Without its data, the stream is
// gone: readers get ErrNotFound (HTTP 404), never a truncated "complete" stream.
func TestChaos_RedisRestartMidStream(t *testing.T) {
	addr := redisAddr()
	for _, lose := range []bool{false, true} {
		name := map[bool]string{false: "data kept", true: "data lost"}[lose]
		t.Run(name, func(t *testing.T) {
			p := newChaosProxy(t, addr)
			prefix := chaosPrefix()
			s := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: p.addr()}), prefix)
			directClient := chaosClient(t, &goredis.Options{Addr: addr})
			hub := chaosHub(t, s, WithHeartbeatInterval(200*time.Millisecond), WithStaleAfter(10*time.Second))

			const total = 60
			var written atomic.Int64
			st, err := hub.Start(context.Background(), func(pctx context.Context, w Writer) error {
				for i := 1; i <= total; i++ {
					if err := w.Write([]byte(strconv.Itoa(i))); err != nil {
						return err // what typical producer code does
					}
					written.Add(1)
					select {
					case <-pctx.Done():
						return pctx.Err()
					case <-time.After(50 * time.Millisecond):
					}
				}
				return nil
			})
			require.NoError(t, err)
			res := make(chan clientOutcome, 1)
			go func() { res <- readLikeAClient(hub, st.ID, 25*time.Second) }()

			<-time.After(time.Second)
			p.restart()
			if lose {
				keys, err := directClient.Keys(context.Background(), prefix+":*").Result()
				require.NoError(t, err)
				if len(keys) > 0 {
					require.NoError(t, directClient.Del(context.Background(), keys...).Err())
				}
			}
			<-time.After(2 * time.Second)
			p.up()

			o := <-res
			t.Logf("producer: %d/%d writes acknowledged; reader: %d events, terminal %q, not found %v, %d errors",
				written.Load(), total, o.seqs, o.terminal, o.notFound, len(o.errs))
			assert.Empty(t, o.gapDup, "G1/G2 across reconnections")
			if lose {
				assert.True(t, o.notFound, "a stream lost with Redis's data is unknown, not complete")
				assert.Empty(t, o.terminal)
				return
			}
			assert.Equal(t, int64(total), written.Load(), "every write succeeded despite the outage")
			assert.Equal(t, "done", o.terminal)
			assert.Equal(t, total+1, o.seqs, "all events and the terminal, each once")
		})
	}
}

// With 20 ms added each way on every Redis round trip, the randomized resume scenario still
// delivers every event exactly once.
func TestChaos_RedisLatency_ResumeNoGapNoDup(t *testing.T) {
	p := newChaosProxy(t, redisAddr())
	p.set(func(p *chaosProxy) { p.latency = 20 * time.Millisecond })
	client := chaosClient(t, &goredis.Options{Addr: p.addr(), PoolSize: 64})
	runResume(t, stressSeed, 3, func(c *testutil.Clock) store.Store {
		s, err := redisstore.New(client, redisstore.WithClock(c), redisstore.WithKeyPrefix(chaosPrefix()))
		require.NoError(t, err)
		return s
	})
}

// ADR 0006: Close(ctx) with a 500 ms deadline while Redis hangs (accepts commands, never
// answers) returns about one second after its deadline; it took 4.6 s for 3 streams before.
func TestChaos_CloseWithHungRedisReturnsNearItsDeadline(t *testing.T) {
	p := newChaosProxy(t, redisAddr())
	s := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: p.addr(), ReadTimeout: time.Second}), chaosPrefix())
	hub, err := New(s, WithLogger(quietLogger()))
	require.NoError(t, err)
	for range 3 {
		_, err := hub.Start(context.Background(), func(pctx context.Context, _ Writer) error {
			<-pctx.Done()
			return nil
		})
		require.NoError(t, err)
	}
	p.set(func(p *chaosProxy) { p.blackhole = true })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = hub.Close(ctx)
	took := time.Since(start)
	t.Logf("Close returned after %v: %v", took.Round(time.Millisecond), err)
	assert.Less(t, took, 500*time.Millisecond+shutdownWriteTimeout+500*time.Millisecond)
}

// ADR 0008: waiting readers no longer queue appends behind their XREAD BLOCK. With 64
// waiting readers on a pool of 8, append p99 was 1.41 s before (p50 863 ms).
func TestChaos_WaitingReadersDoNotStarveAppends(t *testing.T) {
	measure := func(readers int) (p50, p99 time.Duration) {
		client := chaosClient(t, &goredis.Options{Addr: redisAddr(), PoolSize: 8})
		hub, err := New(chaosStoreOn(t, client, chaosPrefix()), WithLogger(quietLogger()))
		require.NoError(t, err)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = hub.Close(ctx) // teardown
		}()
		idle, err := hub.Start(context.Background(), func(pctx context.Context, _ Writer) error {
			<-pctx.Done()
			return nil
		})
		require.NoError(t, err)
		rctx, stop := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for range readers {
			sub, err := hub.Subscribe(rctx, idle.ID, 0)
			require.NoError(t, err)
			wg.Go(func() {
				defer func() { _ = sub.Close() }()
				_, _ = sub.Next(rctx) // waits until stopped
			})
		}
		<-time.After(500 * time.Millisecond) // let the readers reach their wait

		var lat []time.Duration
		done := make(chan struct{})
		_, err = hub.Start(context.Background(), func(_ context.Context, w Writer) error {
			defer close(done)
			for i := range 40 {
				t0 := time.Now()
				if err := w.Write([]byte(strconv.Itoa(i))); err != nil {
					return err
				}
				lat = append(lat, time.Since(t0))
			}
			return nil
		})
		require.NoError(t, err)
		<-done
		stop()
		wg.Wait()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		return lat[len(lat)/2], lat[len(lat)*99/100]
	}
	for _, readers := range []int{0, 8, 64} {
		p50, p99 := measure(readers)
		t.Logf("pool 8, %3d waiting readers: append p50 %v, p99 %v", readers, p50.Round(time.Microsecond), p99.Round(time.Microsecond))
		assert.Less(t, p99, 200*time.Millisecond, "%d readers", readers)
	}
}

// ADR 0004: MaxEvents is exact on Redis too (MAXLEN ~ kept 50 events for a cap of 10).
func TestChaos_RedisMaxEventsIsExact(t *testing.T) {
	s := chaosStoreOn(t, chaosClient(t, &goredis.Options{Addr: redisAddr()}), chaosPrefix())
	ctx := context.Background()
	require.NoError(t, s.Create(ctx, "cap", store.Meta{CreatedAt: time.Now(), IdleTTL: time.Minute, Retention: time.Minute, MaxEvents: 10}))
	accepted := 0
	for i := range 250 {
		_, err := s.Append(ctx, "cap", store.AppendRequest{Kind: "data", Data: []byte(strconv.Itoa(i))})
		if err == nil {
			accepted++
			continue
		}
		require.ErrorIs(t, err, store.ErrTooManyEvents)
	}
	st, err := s.Status(ctx, "cap")
	require.NoError(t, err)
	assert.Equal(t, 10, accepted)
	assert.Equal(t, uint64(1), st.FirstSeq)
	assert.Equal(t, uint64(10), st.LastSeq)
}
