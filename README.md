# Rebound

**Resumable Server-Sent Events for Go: a client that loses its connection in the middle of an LLM
answer reconnects with `Last-Event-ID`, gets exactly the events it missed, and keeps following live.**

LLM chats stream their answer over SSE, tied to one HTTP request. When that connection drops (mobile
handover, closed laptop, reloaded tab, proxy timeout, rolling deploy), the answer is lost or keeps
generating into a dead socket, and the reconnection may land on an instance that never heard of it.

[![CI](https://github.com/bailalayoub/rebound/actions/workflows/ci.yml/badge.svg)](https://github.com/bailalayoub/rebound/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/bailalayoub/rebound.svg)](https://pkg.go.dev/github.com/bailalayoub/rebound)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Rebound decouples generation from the connection: a producer appends numbered events to a log, and
any number of readers join or reconnect at any point, on any instance.

## Quick start (60 seconds)

```sh
mkdir chat && cd chat
go mod init example.com/chat
go get github.com/bailalayoub/rebound
```

Save this as `main.go`:

<!-- quickstart:start -->
```go
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/store/memory"
)

func main() {
	hub, err := rebound.New(memory.New()) // store/redis for several instances
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	// Start the generation, detached from this request; reply with its id at once.
	mux.HandleFunc("POST /chat", func(w http.ResponseWriter, r *http.Request) {
		st, err := hub.Start(r.Context(), func(ctx context.Context, out rebound.Writer) error {
			for _, token := range []string{"Hello", ", ", "world"} { // your LLM tokens here
				if err := out.WriteJSON(map[string]string{"text": token}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, st.ID+"\n")
	})
	// Follow, and resume with Last-Event-ID, any stream.
	mux.Handle("GET /streams/{id}", httpx.NewHandler(hub).Stream(httpx.PathValue("id")))
	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
```
<!-- quickstart:end -->

```sh
go mod tidy   # records the dependencies in go.mod and go.sum
go run .      # listens on :8080 (change the port in main.go if it is taken)
```

In another terminal, with the stream id the first command prints:

```sh
curl -s -X POST localhost:8080/chat                       # Nq0…
curl -N localhost:8080/streams/Nq0…                       # id: 1 … id: 4 event: done
curl -N -H 'Last-Event-ID: 2' localhost:8080/streams/Nq0… # only id: 3 and id: 4
curl -i -H 'Last-Event-ID: 4' localhost:8080/streams/Nq0… # 204: you already have everything
```

Windows PowerShell: run the commands one per line (`&&` is not supported) and type `curl.exe`,
not `curl` (an alias of `Invoke-WebRequest`).

## How it works

**[How it works, with diagrams](docs/how-it-works.md)** walks through a disconnect, a reload and a
reconnection to another instance, step by step. In French: [Comment ça marche](docs/fr/comment-ca-marche.md).

## Guarantees

- **Ordered, no gap, no duplicate.** A reader resuming after event `n` receives exactly `n+1, n+2, …`,
  whether those events are in the past or still to come. Replay and live are the same read.
- **The producer ignores connections.** Only `Cancel`, `MaxDuration` or `Close` stop a generation;
  the request that started it can go away.
- **Any instance.** With the Redis store, the producer runs on instance A while readers and cancels
  hit B and C. No sticky sessions.
- **Every stream ends.** Exactly one terminal event (`done`, or `error` with a code), even when the
  producer hangs or its process dies (`producer_lost`).
- **Each event once.** A write whose reply was lost is retried with its expected sequence number:
  a short Redis outage neither duplicates a token nor ends the generation.
- **Bounded.** Event size, events per stream, idle TTL and retention are capped.

These are checked by a randomized resume test (memory and Redis) and by a chaos tester that drops
30 % of connections against two instances behind a round-robin proxy, with zero gaps or duplicates.

**Stores.** [`store/memory`](store/memory) for one process and tests; [`store/redis`](store/redis)
(Redis Streams and Lua, Redis 6.2+, standalone, Sentinel or Cluster) for several instances. Any other
backend implements [`store.Store`](store/store.go) and proves itself with
[`store/storetest`](store/storetest).

**TypeScript client.** [`@bailalayoub/rebound-client`](clients/typescript) follows a stream from the
browser or Node 18+: `fetch`-based (custom headers), automatic resume with `Last-Event-ID`, backoff
with jitter, never delivers an event twice, zero dependencies.

## Performance

Measured on one laptop (AMD Ryzen 5 4600H, 6 cores / 12 threads, 15.4 GB RAM, Docker Desktop on
Windows 10), with the load generator on the same machine; streams of 20 events/s, 1–3 readers each.
Method, raw outputs and every run: [docs/benchmarks/v0.1.0/README.md](docs/benchmarks/v0.1.0/README.md).

| setup | knee (streams in flight) | delivered events/s at the knee | event latency p50 / p99 at the knee |
|---|---|---|---|
| two instances, one Redis 7.4, nginx (`deploy/`, nginx limits raised) | 150 | about 5 300 | 6.1 / 210 ms |
| one instance, memory store | 1 000 | about 37 000 | 1.1 / 65.5 ms |

On Redis the limit is Redis itself (one core). Below the knee, in 2 hours at 75 streams with 30 %
of connections dropped, readers resuming through round-robin about 1 000 times each, Redis paused
or restarted with an append-only file, an instance killed or stopped: no gap, no duplicate, and
exactly one terminal per stream.

## Use it in your project

```sh
go get github.com/bailalayoub/rebound
```

Go 1.26 or newer (the two supported Go releases).

### net/http

The quick start above is the whole integration. Add a cancel endpoint with:

```go
streams := httpx.NewHandler(hub)
mux.Handle("GET /streams/{id}", streams.Stream(httpx.PathValue("id")))
mux.Handle("POST /streams/{id}/cancel", streams.Cancel(httpx.PathValue("id")))
```

### Gin

```go
streams := httpx.NewHandler(hub)
r.GET("/streams/:id", ginx.Stream(streams, "id"))
r.POST("/streams/:id/cancel", ginx.Cancel(streams, "id"))
```

A complete Gin version of the quick start is the [`ginx` package example](ginx/example_test.go).

### Redis, for several instances

```go
client := goredis.NewClient(&goredis.Options{Addr: "redis:6379", PoolSize: 256})
st, err := redis.New(client) // github.com/bailalayoub/rebound/store/redis
hub, err := rebound.New(st)
```

Waiting readers share the client's pool: at most half of it (`redis.WithMaxBlockingReads`) waits in
`XREAD BLOCK`, the others poll every 250 ms, so appends and heartbeats always find a connection.
Clocks of the instances need not agree: liveness is observed, never compared across hosts.
Memory is about `resident streams × events kept × (payload + 80 B)`; the
[`store/redis` docs](store/redis/doc.go) have a worked example.

### HTTP contract

| Request | Response |
|---|---|
| `GET /streams/{id}` with optional `Last-Event-ID: n` (or `?last_event_id=n`) | `200` SSE from `n+1`; `204` already complete; `400` bad cursor; `404` unknown or expired; `410` only if a custom store trimmed a finished stream; `401`/`403` via `WithAuthorize` |
| `POST /streams/{id}/cancel` | `202 {"status":"cancelling"}` (idempotent); `404` unknown |

Each event is `id: <seq>` / `event: <kind>` / `data: …`, with `: ping` comments every 15 s and one
terminal `event: done` or `event: error` (`{"code":"cancelled","message":"…"}`). Error codes:
`cancelled`, `producer_failed`, `producer_panic`, `timeout`, `producer_lost`, `server_shutdown`,
`max_events_exceeded`. The full contract is [`docs/02-spec.md`](docs/02-spec.md).

### Options

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

### Try it with a UI

[`examples/chat`](examples/chat) is a complete chat with a UI that survives reloads (`make demo`,
fake model, no key). [`deploy/`](deploy) runs two instances behind nginx round-robin on one Redis,
and [`playground/`](playground) runs the same on a real model (OpenAI, Groq, OpenRouter, Gemini or
Ollama) with one `docker compose` command. These are local demos without authentication.

## Fork it / build on it

```
.                    package rebound: Hub, producers, subscriptions, options, hooks
├── store/           the Store interface and shared types
│   ├── memory/      in-process store
│   ├── redis/       Redis Streams store (Lua scripts in lua/)
│   └── storetest/   conformance suite every store must pass
├── sse/             SSE encoder and Last-Event-ID parsing
├── httpx/           net/http handlers (stream, cancel)
├── ginx/            Gin adapter
├── cmd/rebound-conformance/  black-box chaos tester for any server speaking the contract
├── clients/typescript/       @bailalayoub/rebound-client
├── examples/chat/   demo app (Gin, fake or OpenAI-compatible model, web UI)
├── deploy/, playground/      two instances + Redis + nginx, with the fake or a real model
├── internal/        test clock, SSE test client, conformance engine, repository checks
└── docs/            problem, spec, architecture, conformance, code standards, ADRs
```

**Run the tests.** `make check` (format, vet, lint, `go test -race`) and `make cover` need Go 1.26
and the linters from `make tools`; `scripts/in-docker.sh check` runs the same thing in a container
with everything installed. `make test-integration` needs Redis (`make redis-up`),
`make conformance` runs the chaos tester against the example server, `make client-test` tests the
TypeScript client. [CONTRIBUTING.md](.github/CONTRIBUTING.md) has the details.

**Write a store.** Implement the six methods of [`store.Store`](store/store.go) (`Create`, `Append`,
`Read`, `Status`, `Cancel`, `Touch`); the doc comments state the contract (dense sequence numbers,
exactly one terminal, idempotent append with an expected sequence number, blocking reads). Then run
the shared suite from your tests:

```go
func TestMyStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (store.Store, storetest.Clock) {
		c := newTestClock()         // a storetest.Clock you can advance
		return mystore.New(c), c
	})
}
```

[`store/memory`](store/memory) is the reference implementation (about 350 lines). Then run
`TestResume_NoGapNoDup` and the conformance tester against your store, as the Redis store does.

**Port the server to another language.** [`docs/06-conformance.md`](docs/06-conformance.md)
describes the small contract `rebound-conformance` tests over plain HTTP: a Python or Node server
that passes it gives the same guarantees to the same TypeScript client.

## Contributing

Issues and pull requests are welcome. Read [CONTRIBUTING.md](.github/CONTRIBUTING.md) first: tests come
first, everything runs under `-race`, and changes to the public API, the wire protocol or the
dependencies start with an ADR. Security issues go through
[private advisories](.github/SECURITY.md), never public issues.

## Roadmap / ideas

v0.1.0 does what is described above and is tested hard, but it has known limits:

- **The producer is not resumed elsewhere.** It runs on the instance that started it; if that
  process dies, readers end the stream with `producer_lost` and the client has to start a new
  generation. Hand-off to another instance is the most valuable next step.
- **Redis Cluster is tested on a single node.** Keys are hash-tagged so a stream lives in one slot,
  but CI runs a one-node cluster; a multi-shard run with failover is not covered yet.
- **Two stores only.** A Postgres store (`LISTEN/NOTIFY` for live reads) would suit teams without
  Redis.
- **No metrics package.** `WithHooks` reports stream start and end, subscriptions and events; a
  Prometheus or OpenTelemetry adapter on top of it is not written yet.
- **HTTP adapters** exist for net/http and Gin only; chi, echo and fiber would be thin wrappers.
- **One server language.** The conformance tester is language-agnostic; Python and Node servers
  passing it would let other stacks use the same client.
- **SSE only.** A WebSocket transport could reuse the same store and guarantees, if there is demand.

See [`docs/project/roadmap.md`](docs/project/roadmap.md) for what each milestone delivered.

## License

[MIT](LICENSE) © 2026 Ayoub Bailal
