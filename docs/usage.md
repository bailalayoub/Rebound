# Using Rebound

The integration guide: adapters, Redis, the HTTP contract, the options, and how to extend Rebound
with your own store or a server in another language. Start with the
[quick start](../README.md#quick-start-60-seconds) if you have not run it yet.

```sh
go get github.com/bailalayoub/rebound
```

Go 1.26 or newer (the two supported Go releases).

## net/http

The quick start is the whole integration. Add a cancel endpoint with:

```go
streams := httpx.NewHandler(hub)
mux.Handle("GET /streams/{id}", streams.Stream(httpx.PathValue("id")))
mux.Handle("POST /streams/{id}/cancel", streams.Cancel(httpx.PathValue("id")))
```

## Gin

```go
streams := httpx.NewHandler(hub)
r.GET("/streams/:id", ginx.Stream(streams, "id"))
r.POST("/streams/:id/cancel", ginx.Cancel(streams, "id"))
```

A complete Gin version of the quick start is the [`ginx` package example](../ginx/example_test.go).

## Redis, for several instances

```go
client := goredis.NewClient(&goredis.Options{Addr: "redis:6379", PoolSize: 256})
st, err := redis.New(client) // github.com/bailalayoub/rebound/store/redis
hub, err := rebound.New(st)
```

The Redis store uses Redis Streams and Lua (Redis 6.2+, standalone, Sentinel or Cluster).
Waiting readers share the client's pool: at most half of it (`redis.WithMaxBlockingReads`) waits in
`XREAD BLOCK`, the others poll every 250 ms, so appends and heartbeats always find a connection.
Clocks of the instances need not agree: liveness is observed, never compared across hosts.
Memory is about `resident streams × events kept × (payload + 80 B)`; the
[`store/redis` docs](../store/redis/doc.go) have a worked example.

## HTTP contract

| Request | Response |
|---|---|
| `GET /streams/{id}` with optional `Last-Event-ID: n` (or `?last_event_id=n`) | `200` SSE from `n+1`; `204` already complete; `400` bad cursor; `404` unknown or expired; `410` only if a custom store trimmed a finished stream; `401`/`403` via `WithAuthorize` |
| `POST /streams/{id}/cancel` | `202 {"status":"cancelling"}` (idempotent); `404` unknown |

Each event is `id: <seq>` / `event: <kind>` / `data: …`, with `: ping` comments every 15 s and one
terminal `event: done` or `event: error` (`{"code":"cancelled","message":"…"}`). Error codes:
`cancelled`, `producer_failed`, `producer_panic`, `timeout`, `producer_lost`, `server_shutdown`,
`max_events_exceeded`. The full contract is the [spec](02-spec.md).

## Options

| Option | Default | |
|---|---|---|
| `WithMaxDuration` | 10 min | hard cap on a producer (`timeout`) |
| `WithHeartbeatInterval` | 2 s | producer liveness and cross-instance cancel check |
| `WithStaleAfter` | 15 s | silence, observed by a reader, after which it ends the stream (`producer_lost`) |
| `WithRetention` | 10 min | how long a finished stream stays readable |
| `WithIdleTTL` | MaxDuration + StaleAfter + 1 min | TTL of a running stream, refreshed by activity |
| `WithMaxEvents` | 10 000 | hard cap on events per stream (`max_events_exceeded`) |
| `WithExposeProducerErrors` | false | send the producer's error text to clients |
| `WithLogger`, `WithClock`, `WithIDGenerator`, `WithHooks` | | logging, tests, ids, metrics |

Per stream: `WithMeta` (passed to `Authorize`), `WithStreamID`, `WithStreamMaxDuration`.
HTTP handler: `WithAuthorize`, `WithPingInterval`, `WithReconnectDelay`, `WithWriteTimeout`, `WithLogger`.

## TypeScript client

[`@bailalayoub/rebound-client`](../clients/typescript) follows a stream from the browser or
Node 18+: `fetch`-based (custom headers), automatic resume with `Last-Event-ID`, backoff with
jitter, never delivers an event twice, zero dependencies.

## Try it with a UI

[`examples/chat`](../examples/chat) is a complete chat with a UI that survives reloads
(`make demo`, fake model, no key). [`deploy/`](../deploy) runs two instances behind nginx
round-robin on one Redis, and [`playground/`](../playground) runs the same on a real model (OpenAI,
Groq, OpenRouter, Gemini or Ollama) with one `docker compose` command. These are local demos
without authentication.

## Write a store

Implement the six methods of [`store.Store`](../store/store.go) (`Create`, `Append`, `Read`,
`Status`, `Cancel`, `Touch`); the doc comments state the contract (dense sequence numbers, exactly
one terminal, idempotent append with an expected sequence number, blocking reads). Then run the
shared suite from your tests:

```go
func TestMyStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (store.Store, storetest.Clock) {
		c := newTestClock()         // a storetest.Clock you can advance
		return mystore.New(c), c
	})
}
```

[`store/memory`](../store/memory) is the reference implementation (about 350 lines). Then run
`TestResume_NoGapNoDup` and the conformance tester against your store, as the Redis store does.

## Port the server to another language

[The conformance tester](06-conformance.md) describes the small contract `rebound-conformance`
tests over plain HTTP: a Python or Node server that passes it gives the same guarantees to the same
TypeScript client.
