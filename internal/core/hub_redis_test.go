package core

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	redisstore "github.com/bailalayoub/rebound/store/redis"
)

// instanceFactory returns a new Store per call, all sharing one backend, the way separate server
// instances share one Redis.
type instanceFactory func(t *testing.T, c *testutil.Clock) store.Store

func miniredisInstances(t *testing.T) instanceFactory {
	t.Helper()
	mr := miniredis.RunT(t)
	return func(t *testing.T, c *testutil.Clock) store.Store {
		t.Helper()
		return newRedisInstance(t, mr.Addr(), "rebound", c)
	}
}

func newRedisInstance(t *testing.T, addr, prefix string, c *testutil.Clock) store.Store {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	s, err := redisstore.New(client, redisstore.WithClock(c), redisstore.WithKeyPrefix(prefix))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Close())
		require.NoError(t, client.Close())
	})
	return s
}

// testTwoHubsShareOneStore proves G4: a producer on hub A, a reader and a cancel on hub B behave
// exactly as on one instance.
func testTwoHubsShareOneStore(t *testing.T, instances instanceFactory) {
	c := testutil.NewClock(t0)
	a := newHarnessOn(t, c, instances(t, c), WithHeartbeatInterval(2*time.Second))
	b := newHarnessOn(t, c, instances(t, c))
	ctx := context.Background()

	wrote := make(chan struct{})
	afterCancel := make(chan error, 1)
	id := a.start(func(pctx context.Context, w Writer) error {
		for i := range 3 {
			if err := w.Write([]byte(fmt.Sprint(i))); err != nil {
				return err
			}
		}
		close(wrote)
		<-pctx.Done()
		afterCancel <- w.Write([]byte("too late"))
		return nil
	}, WithMeta(map[string]string{"user_id": "u1"}))

	// The reader on hub B follows the stream written on hub A, live.
	sub, err := b.hub.Subscribe(ctx, id, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, sub.Close()) }()
	readCtx, cancel := context.WithTimeout(ctx, waitLimit)
	defer cancel()
	for i := range 3 {
		ev, err := sub.Next(readCtx)
		require.NoError(t, err)
		assert.Equal(t, uint64(i+1), ev.Seq)
		assert.Equal(t, fmt.Sprint(i), string(ev.Data))
	}
	<-wrote
	st, err := b.hub.Status(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, st.State)
	assert.Equal(t, map[string]string{"user_id": "u1"}, st.Meta)

	// Cancel from hub B: hub A's producer learns it at its next heartbeat.
	require.NoError(t, b.hub.Cancel(ctx, id))
	c.AwaitWaiters(t, supervisorWaiters)
	c.Advance(2 * time.Second)
	require.ErrorIs(t, <-afterCancel, ErrCancelled)

	ev, err := sub.Next(readCtx)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), ev.Seq)
	code, _ := terminalCode(t, ev)
	assert.Equal(t, codeCancelled, code)
	_, err = sub.Next(readCtx)
	require.ErrorIs(t, err, io.EOF)

	// A third "instance" joining late replays the whole stream.
	late := newHarnessOn(t, c, instances(t, c))
	evs := late.collect(id, 0)
	require.Len(t, evs, 4)
	assert.Equal(t, "error", evs[3].Kind)
}

func TestG4_TwoHubsShareOneRedis(t *testing.T) {
	t.Parallel()
	testTwoHubsShareOneStore(t, miniredisInstances(t))
}

func TestResume_NoGapNoDup_Miniredis(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	// A short run: the full 200-iteration proof against Redis is TestIntegration_Resume_RealRedis.
	runResume(t, 1, 5, func(c *testutil.Clock) store.Store {
		s, err := redisstore.New(client, redisstore.WithClock(c))
		require.NoError(t, err)
		return s
	})
}
