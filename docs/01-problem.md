# 01 — The problem

## What happens today

Almost every LLM chat streams its answer over Server-Sent Events, and almost every implementation
ties the generation to the HTTP connection:

```
Browser ──POST /chat──▶ Server ──▶ LLM provider
   ◀───── tokens over SSE ─────┘
```

The handler calls the model and copies tokens into the response body as they arrive. The moment
the connection drops — mobile network handover, laptop lid closed, tab reloaded, proxy idle
timeout, a deploy rolling the pod — one of two things happens:

- **The server cancels the model call.** The partial answer is gone. The user asks again. You pay
  the tokens twice and the user waits twice.
- **The server keeps generating into a dead socket.** You pay for tokens nobody receives.

In both cases there is no way to *resume*. And with more than one server instance behind a load
balancer it gets worse: the reconnection lands on an instance that has never heard of this
generation.

## Why the obvious fixes are not enough

- **`EventSource` auto-reconnect.** The browser does reconnect and sends `Last-Event-ID`, but
  (a) it only does `GET`, while chat endpoints are `POST` with a body, and (b) it is useless unless
  the server has *kept* the events somewhere and can replay from that id. Nobody keeps them by
  default.
- **"Just retry the request."** Regenerates a different answer, doubles the cost, and the user
  sees the text restart from zero.
- **WebSockets.** Same problem: a socket is a connection, not a log. Reconnecting still needs a
  server-side buffer and a cursor.
- **Pub/Sub fan-out.** Pub/Sub has no memory: a reconnecting client gets nothing that was published
  while it was away, and mixing "replay from a buffer" with "subscribe to live" races produce
  duplicates or gaps unless done carefully.

## What already exists (and why this project still makes sense)

| Ecosystem | Existing work | Gap |
|---|---|---|
| JavaScript | Vercel `resumable-stream` (Redis-backed resumable streams for the AI SDK) | JS only |
| Python | `fastapi-resumable-stream`, `resumable-sse` (2026) | Python only |
| Go | Generic SSE servers such as `tmaxmax/go-sse` (in-memory replay provider) and `r3labs/sse` | In-process only; no detached producer lifecycle, no multi-instance store, no cancel/terminal semantics, nothing specific to long LLM generations |
| Everywhere | Dozens of hand-rolled "Last-Event-ID + XRANGE" implementations in application repos | Each one re-discovers the same race conditions |

Rebound is the Go answer to this specific problem: **a small library where the producer lives
independently of connections, a store keeps the ordered log, and any number of readers can join or
reconnect at any sequence number — on any instance.**

> Before building, re-check pkg.go.dev and GitHub for "resumable sse", "last-event-id replay",
> "resumable stream" in Go. If a maintained library already does this, stop and reconsider.

## Who it is for

- Teams running Go backends that stream LLM output (Gin, chi, `net/http`).
- Products used on mobile networks, where disconnects are the norm, not the exception.
- Anyone who wants "reload the page and the answer is still coming" without rebuilding their stack.

## The one-sentence promise

> Start a generation once; let clients connect, drop, reconnect and switch servers as much as they
> like; every client ends up with exactly the same, complete, ordered sequence of events.

## Success criteria for v0.1.0

1. The conformance tester (`docs/06-conformance.md`) runs 200 randomized disconnect/resume runs
   against both the in-memory and the Redis backend with **zero** gaps, duplicates or mismatches.
2. The same tester passes against a two-instance deployment behind a round-robin proxy.
3. A new user can add resumability to an existing Gin SSE handler in **under 30 lines** following
   the README.
4. `make check` is green with the race detector and the coverage thresholds.
