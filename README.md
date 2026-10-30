# Rebound

**Resumable LLM streaming for Go servers.** If a client loses its connection in the middle of an
answer, it reconnects, gets exactly the tokens it missed, and keeps following live, on any
instance.

[![CI](https://github.com/bailalayoub/rebound/actions/workflows/ci.yml/badge.svg)](https://github.com/bailalayoub/rebound/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/bailalayoub/rebound.svg)](https://pkg.go.dev/github.com/bailalayoub/rebound)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## The problem

LLM chats stream their answer over Server-Sent Events, tied to one HTTP request. When that
connection drops (mobile handover, closed laptop, reloaded tab, proxy timeout, rolling deploy), the
answer is lost or keeps generating into a dead socket. You pay the tokens twice, the user waits
twice, and with several instances the reconnection lands on one that never heard of the answer.

## The solution

```
POST /chat ──▶ Hub.Start ──▶ producer goroutine ──▶ append 1, 2, 3, … ──▶ store (memory or Redis)
                              (keeps running when                            │
                               the client leaves)                            ▼
GET /streams/{id} + Last-Event-ID: 2 ◀──────────── replay 3, then live 4, 5, … done
```

1. The generation runs in a goroutine **detached from the request** and appends numbered events to
   a log.
2. Clients read that log over SSE. On reconnect, the standard `Last-Event-ID` header says where to
   resume: replay and live are one read, so there is no gap and no duplicate.
3. With the Redis store the log is shared: the producer runs on instance A, the client reconnects
   to B. No sticky sessions.
4. Every stream ends with exactly one `done` or `error`, even if the producer hangs or its process
   dies.

## What it is not

- **Not serverless.** Rebound is a library inside your own Go server, and the generation runs in
  that process. It needs a process that keeps running after the response is sent: a VM, a
  container, Kubernetes. On functions that are frozen after the response (AWS Lambda style), the
  generation stops and readers end the stream with `producer_lost`.
- **Not an LLM client.** You call your model (OpenAI, Ollama, anything) yourself and write its
  tokens to Rebound.
- **Not a chat history database.** A finished stream stays readable for 10 minutes by default
  (`WithRetention`), long enough to resume. Store conversations in your own database.
- **Not a hosted service.** Nothing to sign up for: `go get` it, add two routes.

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

Windows PowerShell: run the commands one per line and type `curl.exe`, not `curl`.

Next: Gin, Redis, cancel, options and the HTTP contract are in **[Using Rebound](docs/usage.md)**.
A full chat UI that survives reloads: `make demo`.

## Where to look

```
rebound.go, options.go   the public API: what you import (start here)
internal/core/           the engine behind it, with its unit tests
httpx/ ginx/             HTTP handlers for net/http and Gin
store/                   where events live: memory/ (one instance) or redis/ (several)
sse/                     SSE encoding and Last-Event-ID parsing
clients/typescript/      browser/Node client that resumes by itself
examples/                quickstart/, chat/ (web UI), deploy/ and playground/ (Docker)
docs/                    how it works, spec, ADRs, benchmarks
cmd/ internal/ scripts/  tooling: chaos tester, load tests, CI helpers (not needed to use it)
```

**The library**

| Path | What it is |
|---|---|
| [`rebound.go`](rebound.go) | **Start here.** `New`, `Hub` (`Start`, `Subscribe`, `Cancel`, `Status`, `Close`), `Writer`, errors |
| [`options.go`](options.go) | every `With…` option and its default, `Hooks` for metrics |
| [`example_test.go`](example_test.go) | runnable examples, also shown on pkg.go.dev |
| [`internal/core/`](internal/core) | the implementation: [`producer.go`](internal/core/producer.go) runs a generation (heartbeats, timeouts, retries, final `done`/`error`), [`subscription.go`](internal/core/subscription.go) reads one (replay, then live), and the unit tests (`*_test.go`; `*integration_test.go` need Redis) |
| [`httpx/`](httpx), [`ginx/`](ginx) | HTTP handlers: stream with `Last-Event-ID`, cancel |
| [`store/memory/`](store/memory) | in-process store: one instance, tests |
| [`store/redis/`](store/redis) | Redis Streams store: several instances (Lua scripts in `lua/`) |
| [`store/`](store), [`store/storetest/`](store/storetest) | the `Store` interface, and the test suite any store must pass |
| [`sse/`](sse) | SSE encoding and `Last-Event-ID` parsing (golden files in `testdata/`) |
| [`clients/typescript/`](clients/typescript) | browser/Node client: resumes automatically, zero dependencies |

**Try it**

| Folder | What it is |
|---|---|
| [`examples/quickstart/`](examples/quickstart) | the quick start above, as a file (CI checks they match) |
| [`examples/chat/`](examples/chat) | a chat app with a web UI, fake or real model (`make demo`) |
| [`examples/deploy/`](examples/deploy) | 2 instances + Redis + nginx round-robin, fake model, one `docker compose` |
| [`examples/playground/`](examples/playground) | the same on a real LLM (OpenAI, Groq, OpenRouter, Gemini, Ollama) |

**Tooling: you can ignore it to use the library**

| Folder | What it is |
|---|---|
| [`cmd/`](cmd) | `rebound-conformance` (chaos tester for any server speaking the protocol), `rebound-load` (stress harness) |
| [`internal/`](internal) | besides `core/`: test helpers, the conformance and load-test engines, repository checks |
| [`scripts/`](scripts) | CI and Docker helpers, the dev Redis (`make redis-up`), the stress campaign |
| [`docs/`](docs) | [how it works](docs/how-it-works.md), spec, architecture, ADRs, [benchmarks](docs/benchmarks/v0.1.0/README.md) |
| [`.github/`](.github) | CI, issue templates, [CONTRIBUTING](.github/CONTRIBUTING.md), [SECURITY](.github/SECURITY.md) |

## Guarantees

- **Ordered, no gap, no duplicate.** A reader resuming after event `n` receives exactly
  `n+1, n+2, …`, whether those events are in the past or still to come.
- **The producer ignores connections.** Only `Cancel`, `MaxDuration` or `Close` stop it.
- **Any instance** with Redis (standalone, Sentinel or Cluster).
- **Every stream ends** with exactly one `done` or `error` (`producer_lost` if its process dies).
- **Each event once,** even when a Redis write is retried after a short outage.
- **Bounded:** event size, events per stream, idle TTL and retention are capped.

Checked by a randomized resume test (memory and Redis) and a chaos tester that drops 30 % of
connections against two instances behind a round-robin proxy.

## Performance

On one laptop (Ryzen 5 4600H, Docker Desktop), streams of 20 events/s:

| setup | streams in flight at the knee | events/s delivered | latency p50 / p99 |
|---|---|---|---|
| 2 instances, 1 Redis, nginx | 150 | ~5 300 | 6.1 / 210 ms |
| 1 instance, memory store | 1 000 | ~37 000 | 1.1 / 65.5 ms |

On Redis the limit is Redis itself (one core). Below the knee, 2 hours with 30 % of connections
dropped, Redis paused or restarted with an append-only file, an instance killed: no gap, no duplicate. Method and every raw
output: [stress campaign v0.1.0](docs/benchmarks/v0.1.0/README.md).

## Limits and roadmap

The producer is not taken over by another instance if its process dies (readers get
`producer_lost`); Redis Cluster is tested on one node only; there is no Postgres store, no
metrics adapter, and HTTP adapters exist for net/http and Gin only. Details:
[roadmap](docs/project/roadmap.md).

## Contributing

Issues and pull requests are welcome: read [CONTRIBUTING](.github/CONTRIBUTING.md) first
(`make check` runs everything; `scripts/in-docker.sh check` without a local toolchain). Security
issues go through [private advisories](.github/SECURITY.md), never public issues.

## License

[MIT](LICENSE) © 2026 Ayoub Bailal
