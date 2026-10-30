//go:build integration

package core

import (
	"flag"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/internal/testutil"
	"github.com/bailalayoub/rebound/store"
	redisstore "github.com/bailalayoub/rebound/store/redis"
)

// Integration tests need a real Redis at REDIS_ADDR (default localhost:6379).

var redisResumeIterations = flag.Int("redis-iterations", 200, "iterations of TestIntegration_Resume_RealRedis")

func redisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// G4 against a real Redis: producer on hub A, reader and cancel on hub B.
func TestIntegration_TwoHubsShareOneRedis(t *testing.T) {
	prefix := "it-g4-" + time.Now().Format("150405.000000000")
	testTwoHubsShareOneStore(t, func(t *testing.T, c *testutil.Clock) store.Store {
		t.Helper()
		return newRedisInstance(t, redisAddr(), prefix, c)
	})
}

// TestIntegration_Resume_RealRedis is TestResume_NoGapNoDup against a real Redis (M3: 200
// iterations). Reproduce with: go test -race -tags=integration -run TestIntegration_Resume_RealRedis . -seed=<seed>
func TestIntegration_Resume_RealRedis(t *testing.T) {
	seed := *resumeSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	runResume(t, seed, *redisResumeIterations, func(c *testutil.Clock) store.Store {
		s, err := redisstore.New(client, redisstore.WithClock(c), redisstore.WithKeyPrefix("it-resume"))
		require.NoError(t, err)
		return s
	})
}
