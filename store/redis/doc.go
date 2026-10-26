// Package redis is a store.Store backed by Redis Streams, for deployments with several server
// instances: a producer on one instance and readers on others share one log.
//
// # Layout
//
// Each stream uses two keys sharing the hash tag {id}, so they live on one Redis Cluster slot:
// "rebound:{id}:meta" (a hash: state, seq, cancelled, timestamps, TTLs, attrs) and
// "rebound:{id}:log" (a stream whose entry ids are "<seq>-0", fields kind and data). Every
// operation runs as one Lua script, so sequence numbers, idempotent replays of an append whose
// reply was lost, the MaxEvents cap, the single terminal event and TTL changes are atomic across
// instances. Readers read the log itself (no Pub/Sub), using XREAD BLOCK only to wake up.
//
// # Connections
//
// A reader waiting in XREAD BLOCK holds a pooled connection for up to 250 ms at a time. At most
// WithMaxBlockingReads such waits run at once (default: half the PoolSize of a *redis.Client,
// else 16); other waiting readers poll every 250 ms without holding a connection, so producers'
// appends and heartbeats always find one. Size the pool for your concurrent producers plus that
// bound. Errors never wrap io.EOF: go-redis reports a closed connection that way, and io.EOF means
// "end of stream" to readers.
//
// # Requirements
//
// Redis 6.2 or newer (tested with 6.2 and 7.4), standalone, Sentinel or Cluster: pass any
// redis.UniversalClient. Scripts are loaded with EVALSHA and fall back to EVAL, so a SCRIPT FLUSH
// or a failover is harmless. The store does not close the client it is given.
//
// # Memory sizing
//
// A stored event costs roughly its payload plus 60-80 bytes of stream and field overhead. Streams
// stay in memory while they run and for Retention after they end, and each holds at most
// MaxEvents events plus its terminal event (a hard cap; nothing is trimmed). So:
//
//	memory ≈ resident streams × min(events per stream, MaxEvents) × (average payload + 80 B)
//	resident streams ≈ streams started per minute × (average duration + Retention) in minutes
//
// Example: 1 000 generations per minute, 1 minute each, Retention 10 minutes, 500 events of
// 20 bytes per generation: 11 000 streams × 500 × 100 B ≈ 550 MB. Shorten Retention to bound
// memory; MaxEvents bounds the worst case (a producer that writes more ends with
// "max_events_exceeded").
package redis
