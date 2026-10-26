//go:build integration

package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/storetest"
)

// Integration tests need a real Redis at REDIS_ADDR (default localhost:6379) and a Redis started
// with --cluster-enabled yes at REDIS_CLUSTER_ADDR (default localhost:7000); see docker-compose.yml.

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// wallClock is the storetest clock for a real server: TTLs elapse in real time.
type wallClock struct{}

func (wallClock) Now() time.Time          { return time.Now() }
func (wallClock) Advance(d time.Duration) { time.Sleep(d) }

func randomPrefix(t *testing.T) string {
	t.Helper()
	var b [6]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	return "it-" + hex.EncodeToString(b[:])
}

func TestIntegration_RedisVersion(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer func() { require.NoError(t, client.Close()) }()
	info, err := client.Info(context.Background(), "server").Result()
	require.NoError(t, err)
	var version string
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			version = v
		}
	}
	parts := strings.SplitN(version, ".", 3)
	require.GreaterOrEqual(t, len(parts), 2, "redis_version %q", version)
	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])
	require.True(t, major > 6 || (major == 6 && minor >= 2), "Redis %s is older than the required 6.2", version)
	t.Logf("Redis %s", version)
}

func TestIntegration_Conformance(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, client.Ping(context.Background()).Err())
	storetest.Run(t, func(t *testing.T) (store.Store, storetest.Clock) {
		s, err := New(client, WithKeyPrefix(randomPrefix(t)))
		require.NoError(t, err)
		return s, wallClock{}
	})
}

// TestIntegration_ConformanceCluster runs the suite through a Cluster client: a script touching
// keys of two slots would fail with CROSSSLOT, so this proves the {id} hash-tag layout.
func TestIntegration_ConformanceCluster(t *testing.T) {
	addr := envOr("REDIS_CLUSTER_ADDR", "localhost:7000")
	ensureSingleNodeCluster(t, addr)
	client := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{addr}})
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	s, err := New(client)
	require.NoError(t, err)
	for _, id := range []string{"a", "chat-42", "x.y~z"} {
		keys := s.keys(id)
		metaSlot, err := client.ClusterKeySlot(context.Background(), keys[0]).Result()
		require.NoError(t, err)
		logSlot, err := client.ClusterKeySlot(context.Background(), keys[1]).Result()
		require.NoError(t, err)
		require.Equal(t, metaSlot, logSlot, "keys of %q", id)
		require.Equal(t, int64(keySlot(keys[0])), metaSlot, "our CRC16 agrees with the server")
	}

	storetest.Run(t, func(t *testing.T) (store.Store, storetest.Clock) {
		s, err := New(client, WithKeyPrefix(randomPrefix(t)))
		require.NoError(t, err)
		return s, wallClock{}
	})
}

// ensureSingleNodeCluster assigns every slot to the node at addr if it has none yet, and waits
// for the cluster to report ok.
func ensureSingleNodeCluster(t *testing.T, addr string) {
	t.Helper()
	ctx := context.Background()
	node := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { require.NoError(t, node.Close()) }()
	info, err := node.ClusterInfo(ctx).Result()
	require.NoError(t, err, "REDIS_CLUSTER_ADDR must point at a Redis started with --cluster-enabled yes")
	if strings.Contains(info, "cluster_slots_assigned:0") {
		require.NoError(t, node.ClusterAddSlotsRange(ctx, 0, 16383).Err())
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err = node.ClusterInfo(ctx).Result()
		require.NoError(t, err)
		if strings.Contains(info, "cluster_state:ok") {
			return
		}
		require.True(t, time.Now().Before(deadline), "cluster did not reach state ok:\n%s", info)
		time.Sleep(100 * time.Millisecond)
	}
}
