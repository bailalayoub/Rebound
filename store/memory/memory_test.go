package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/storetest"
)

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) (store.Store, storetest.Clock) {
		c := testutil.NewClock(t0)
		return New(WithClock(c)), c
	})
}

func newTestStore() (*Store, *testutil.Clock) {
	c := testutil.NewClock(t0)
	return New(WithClock(c), WithSweepInterval(time.Minute)), c
}

func shortMeta(c *testutil.Clock) store.Meta {
	return store.Meta{CreatedAt: c.Now(), IdleTTL: time.Second, Retention: time.Second}
}

func TestSweepRemovesExpiredStreamsOnCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, c := newTestStore()
	require.NoError(t, s.Create(ctx, "old", shortMeta(c)))
	require.NoError(t, s.Create(ctx, "forever", store.Meta{CreatedAt: c.Now()})) // no TTL

	c.Advance(30 * time.Second)
	require.NoError(t, s.Create(ctx, "early", shortMeta(c)))
	assert.Len(t, s.streams, 3, "no sweep before the interval has elapsed")

	c.Advance(31 * time.Second)
	require.NoError(t, s.Create(ctx, "new", shortMeta(c)))
	assert.ElementsMatch(t, []string{"forever", "new"}, keys(s.streams))
}

func keys(m map[string]*stream) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestClosedStoreRejectsEverything(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, c := newTestStore()
	require.NoError(t, s.Create(ctx, "a", shortMeta(c)))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close is idempotent")

	require.ErrorIs(t, s.Create(ctx, "b", shortMeta(c)), ErrStoreClosed)
	_, err := s.Append(ctx, "a", store.AppendRequest{Kind: "data"})
	require.ErrorIs(t, err, ErrStoreClosed)
	_, err = s.Read(ctx, "a", 0, 0, time.Hour)
	require.ErrorIs(t, err, ErrStoreClosed)
	_, err = s.Status(ctx, "a")
	require.ErrorIs(t, err, ErrStoreClosed)
	require.ErrorIs(t, s.Cancel(ctx, "a"), ErrStoreClosed)
	require.ErrorIs(t, s.Touch(ctx, "a", c.Now()), ErrStoreClosed)
}

func TestReadOnFinishedStreamDoesNotBlock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, c := newTestStore()
	require.NoError(t, s.Create(ctx, "a", shortMeta(c)))
	_, err := s.Append(ctx, "a", store.AppendRequest{Kind: "done", Data: []byte("{}")})
	require.NoError(t, err)

	// The fake clock never advances: returning proves the read did not wait for its timer.
	evs, err := s.Read(ctx, "a", 1, 10, time.Hour)
	require.NoError(t, err)
	assert.Empty(t, evs)
	assert.Zero(t, c.Waiters())
}

func TestErrorCodeFromPayload(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		`{"code":"timeout","message":"x"}`: "timeout",
		`{"message":"no code"}`:            "",
		`not json`:                         "",
		``:                                 "",
		`{"code":42}`:                      "",
	}
	for payload, want := range cases {
		assert.Equal(t, want, errorCode([]byte(payload)), "payload %q", payload)
	}
}

func TestAppendWithoutTimestampUsesClock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, c := newTestStore()
	require.NoError(t, s.Create(ctx, "a", shortMeta(c)))
	c.Advance(time.Millisecond)
	_, err := s.Append(ctx, "a", store.AppendRequest{Kind: "data"})
	require.NoError(t, err)
	st, err := s.Status(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, c.Now(), st.LastActivity)
}

func TestDefaultsAndIgnoredOptions(t *testing.T) {
	t.Parallel()
	s := New(WithClock(nil), WithSweepInterval(0))
	assert.Equal(t, store.RealClock{}, s.clock)
	assert.Equal(t, defaultSweepInterval, s.sweepEvery)
}
