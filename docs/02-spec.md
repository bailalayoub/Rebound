# 02 — Specification (source of truth)

Status: **v0.1 draft, binding for M1–M5.** Changes require an ADR in `docs/adr/`.

## 1. Vocabulary

| Term | Meaning |
|---|---|
| **Stream** | One generation. Identified by a `StreamID` (string, 22 chars, base64url of 16 random bytes from `crypto/rand`; custom ids allowed via option). |
| **Producer** | The user-supplied function that writes events. Runs in a goroutine owned by the Hub, **detached from any HTTP request**. Exactly one producer per stream. |
| **Event** | `{Seq uint64, Kind string, Data []byte}`. `Seq` starts at 1 and is dense (1, 2, 3 …) within a stream. `Data` is UTF-8 text with LF line endings: the `Writer` replaces invalid UTF-8 with U+FFFD and CR/CRLF with LF before storing (ADR 0007), so every reader receives exactly the stored bytes. |
| **Kind** | `data` (default), a custom kind matching `^[a-z][a-z0-9_]{0,31}$`, or a terminal kind: `done`, `error`. Reserved and rejected for user events: `done`, `error`, `open`, `ping`. |
| **Terminal event** | The last event of a stream, kind `done` or `error`. A stream has at most one. After it, appends fail with `ErrClosed`. |
| **Reader / Subscription** | A consumer that receives events strictly after a given `Seq`. Any number per stream, on any instance. |
| **Store** | The ordered log + metadata backend. v0.1 ships `memory` and `redis`. |
| **Hub** | The orchestrator: starts producers, serves subscriptions, enforces liveness, timeouts and cancellation. |

## 2. Guarantees

- **G1 Ordering.** Within a stream, every reader observes events in increasing `Seq` with no reordering.
- **G2 No gap, no duplicate.** A reader that asks for events after `n` receives exactly `n+1, n+2, …` up to the terminal event, whether those events are in the past (replay) or arrive later (live), with no special handling at the boundary.
- **G3 Producer independence.** The producer runs to completion (or failure/cancel/timeout) regardless of whether any reader is connected.
- **G4 Instance independence.** With the Redis store, a producer on instance A and readers on instances B and C behave exactly as on one instance.
- **G5 Termination.** Every stream reaches a terminal event. While the producer's process lives: within `MaxDuration + HeartbeatInterval` of its creation, even if `produce` ignores its context (ADR 0006). If the process dies: a reader observing the stream writes `producer_lost` after `StaleAfter` without activity, measured on the reader's own clock.
- **G6 Bounded resources.** Logs are capped (`MaxEvents`, a hard cap: a running stream is never trimmed, ADR 0004), expire (`Retention`, `IdleTTL`), and a single stream cannot block the Hub.
- **G7 Idempotent terminal.** Whoever tries to write a terminal event (producer, hub, a reader detecting a lost producer), exactly one terminal event is stored; all others observe it.

## 3. Public Go API

Package `github.com/bailalayoub/rebound`.

```go
// Hub
func New(s store.Store, opts ...Option) (*Hub, error)
func (h *Hub) Start(ctx context.Context, produce ProducerFunc, opts ...StartOption) (*Stream, error)
func (h *Hub) Subscribe(ctx context.Context, id string, afterSeq uint64) (*Subscription, error)
func (h *Hub) Cancel(ctx context.Context, id string) error
func (h *Hub) Status(ctx context.Context, id string) (Status, error)
func (h *Hub) Close(ctx context.Context) error // waits for producers until ctx deadline, then fails them with "server_shutdown"

type ProducerFunc func(ctx context.Context, w Writer) error

type Writer interface {
    Write(data []byte) error                 // kind "data"
    WriteEvent(kind string, data []byte) error
    WriteJSON(v any) error                    // kind "data", JSON-encoded
    StreamID() string
}

type Stream struct{ ID string }

type Event struct {
    Seq  uint64
    Kind string
    Data []byte
}

type Subscription struct{ /* unexported */ }
func (s *Subscription) Next(ctx context.Context) (Event, error) // returns io.EOF after the terminal event has been delivered
func (s *Subscription) Close() error

type State string // "running" | "done" | "error"

type Status struct {
    ID           string
    State        State
    ErrorCode    string            // set when State == "error"
    FirstSeq     uint64            // lowest seq still retained (≥1); 0 when empty
    LastSeq      uint64
    Cancelled    bool
    CreatedAt    time.Time
    LastActivity time.Time         // last append or heartbeat touch
    Meta         map[string]string
}
```

### 3.1 Semantics

- `Start` returns as soon as the stream is created in the store. The producer runs with
  `ctx = context.WithoutCancel(callerCtx)` plus `WithTimeout(MaxDuration)` plus cancellation on
  `Cancel`/`Close`. **The request context must never cancel the producer** — that is the whole point.
- `produce` returning `nil` ⇒ terminal `done` with data `{}`.
  Returning an error ⇒ terminal `error` with code `producer_failed`. The error message is **not**
  forwarded to clients unless `WithExposeProducerErrors(true)`; it is always logged.
  A panic in `produce` is recovered ⇒ `error` with code `producer_panic`.
- `Writer` methods return `ErrClosed` once the stream is terminal or cancelled; the producer should
  return promptly when that happens (its `ctx` is also cancelled). Each event is stored exactly once:
  the `Writer` passes its expected sequence number and retries transient store errors for at most
  `StaleAfter / 2` (ADR 0005). Beyond `MaxEvents` events the error also matches `ErrTooManyEvents`
  and the stream ends with `error{code:"max_events_exceeded"}` (ADR 0004).
- A producer that has not returned one `HeartbeatInterval` after its `ctx` was cancelled is
  abandoned: the Hub writes the terminal for the cause and stops heartbeating (ADR 0006).
- `Subscribe(id, afterSeq)`: `afterSeq == 0` means from the beginning. `afterSeq > LastSeq` ⇒
  `ErrInvalidCursor`. Unknown or malformed id ⇒ `ErrNotFound`. Running streams are never trimmed;
  only a store that trims a terminal stream during retention can make `afterSeq < FirstSeq-1`, which
  is `ErrGap` and final for that reader (ADR 0004).
- `Next` blocks until an event is available or `ctx` is done. After delivering the terminal event
  it returns `io.EOF`, and only then: store errors never match `io.EOF` (ADR 0005).
- `Cancel`: idempotent. Marks the stream cancelled; the producer's ctx is cancelled within
  `HeartbeatInterval`; the Hub writes terminal `error{code:"cancelled"}` (G7). `ErrNotFound` for unknown ids; no error if already terminal.
- `Close(ctx)`: refuses new `Start`s, waits for local producers until `ctx` is done, then cancels the
  remaining ones and writes their `server_shutdown` terminals concurrently, each bounded by 1 s.
  Producers that ignore their context keep their goroutine until they return (ADR 0006).

### 3.2 Options (defaults)

| Option | Default | Meaning |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | structured logging |
| `WithClock(Clock)` | real clock | injectable time for tests |
| `WithIDGenerator(func() string)` | 16 random bytes, base64url | stream ids |
| `WithMaxDuration(d)` | 10 min | hard cap on a producer; exceeded ⇒ `error{timeout}` |
| `WithHeartbeatInterval(d)` | 2 s | producer liveness touch + cancel check |
| `WithStaleAfter(d)` | 15 s | no append/touch observed by a reader for this long while running ⇒ `error{producer_lost}` |
| `WithRetention(d)` | 10 min | how long a terminal stream stays readable |
| `WithIdleTTL(d)` | `MaxDuration + StaleAfter + 1 min` | TTL refreshed on every append and heartbeat while running; must be ≥ `StaleAfter + HeartbeatInterval` (ADR 0008) |
| `WithMaxEvents(n)` | 10 000 | hard cap on non-terminal events per stream; exceeded ⇒ `error{max_events_exceeded}` (ADR 0004) |
| `WithExposeProducerErrors(bool)` | false | include producer error text in the `error` payload |
| `WithHooks(Hooks)` | none | `OnStreamStart/End`, `OnSubscribe/Unsubscribe`, `OnEvent` for metrics |

Start options: `WithMeta(map[string]string)` (stored, returned in `Status`, passed to `Authorize`),
`WithStreamID(string)`, `WithStreamMaxDuration(d)`.

### 3.3 Errors (sentinels, matched with `errors.Is`)

`ErrNotFound`, `ErrExists`, `ErrClosed`, `ErrCancelled`, `ErrGap`, `ErrTooManyEvents` (ADR 0004),
`ErrConflict` (ADR 0005), `ErrInvalidCursor`, `ErrInvalidKind`, `ErrTooLarge` (event data >
`MaxEventBytes`, default 64 KiB), `ErrHubClosed`.

## 4. HTTP contract (`httpx`, `ginx`)

### 4.1 `GET /streams/{id}` — subscribe

Request:
- `Last-Event-ID: <seq>` header (standard) **or** `?last_event_id=<seq>` query (for tools that
  cannot set headers). Header wins if both present. Absent ⇒ from the beginning.
- `Accept: text/event-stream` recommended, not required.

Responses:

| Status | When | Body |
|---|---|---|
| `200` | stream exists | SSE body (§5) |
| `204` | stream is terminal **and** the cursor already includes the terminal event | empty — client must stop |
| `400` | cursor not a non-negative integer, or `> LastSeq` | `{"error":"invalid_last_event_id"}` |
| `404` | unknown or expired id | `{"error":"stream_not_found"}` |
| `410` | cursor older than retained events (`ErrGap`; only after a store trimmed a terminal stream) | `{"error":"gap","first_seq":n}` — final: the client stops and reports `gap` (ADR 0004) |
| `401`/`403` | `Authorize` hook rejected | `{"error":"unauthorized"}` / `{"error":"forbidden"}` |

SSE response headers: `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control: no-cache, no-transform`,
`Connection: keep-alive`, `X-Accel-Buffering: no`. The handler flushes after every event.

### 4.2 `POST /streams/{id}/cancel` — cancel

`202 {"status":"cancelling"}` (also when already terminal, idempotent); `404` unknown; `401/403` per `Authorize`.

### 4.3 Starting a stream

Starting is application-specific (it needs the prompt, the user, the model). The library provides
two shapes and the example app uses both:

- **Two-step** (recommended for chats): `POST /chat` → app calls `hub.Start(...)` → responds
  `201 {"stream_id":"…","url":"/streams/…"}` → client subscribes with the TypeScript client.
- **One-step**: the app calls `hub.Start(...)` then `handler.ServeStream(w, r, id)` in the same
  request. Convenient, but the client must still learn the id (`X-Stream-ID` response header is
  set) to resume later.

### 4.4 Handler API

```go
package httpx
func NewHandler(h *rebound.Hub, opts ...HandlerOption) *Handler
func (x *Handler) ServeStream(w http.ResponseWriter, r *http.Request, streamID string)
func (x *Handler) ServeCancel(w http.ResponseWriter, r *http.Request, streamID string)
func (x *Handler) Stream(idFrom func(*http.Request) string) http.Handler  // e.g. httpx.PathValue("id")
func (x *Handler) Cancel(idFrom func(*http.Request) string) http.Handler
// Handler options
func WithAuthorize(func(ctx context.Context, r *http.Request, st rebound.Status) error) HandlerOption // return ErrUnauthorized / ErrForbidden
func WithPingInterval(d time.Duration) HandlerOption  // default 15s; SSE comment ": ping"
func WithReconnectDelay(d time.Duration) HandlerOption // default 1s; sent once as "retry: 1000"
func WithWriteTimeout(d time.Duration) HandlerOption   // default 10s per flush
func WithLogger(l *slog.Logger) HandlerOption          // default slog.Default(); 503s and stream failures (ADR 0008)
```

```go
package ginx
func Stream(x *httpx.Handler, param string) gin.HandlerFunc // reads c.Param(param)
func Cancel(x *httpx.Handler, param string) gin.HandlerFunc
```

## 5. Wire format (SSE)

On connect, once:

```
retry: 1000

```

Then each event:

```
id: 42
event: data
data: {"text":"Bon"}

```

- `id` is the decimal `Seq`. **Only stored events carry an `id`.**
- `event` is the `Kind`. For `data` the `event:` line is still emitted (explicit beats implicit).
- `data` is the event payload. Multi-line payloads are split into several `data:` lines per the SSE spec. Payloads are UTF-8 text with LF line endings (§1, ADR 0007), so the client gets back exactly the stored bytes; the example app uses JSON.
- Keep-alive pings are **comments**, never events: `: ping\n\n` every `PingInterval`. They have no `id` and do not advance the cursor.
- Terminal events:

```
id: 57
event: done
data: {}

```
```
id: 57
event: error
data: {"code":"cancelled","message":"cancelled by client"}

```

After the terminal event the server closes the response.

### 5.1 Error codes

| code | Written by | Meaning |
|---|---|---|
| `cancelled` | Hub | `Cancel` was called |
| `producer_failed` | Hub | producer returned an error |
| `producer_panic` | Hub | producer panicked |
| `timeout` | Hub | `MaxDuration` exceeded |
| `producer_lost` | any reader (G7) | no activity observed for `StaleAfter` while running |
| `server_shutdown` | Hub | `Close` deadline reached with producer still running |
| `max_events_exceeded` | Hub | the producer tried to write more than `MaxEvents` events (ADR 0004) |

## 6. Store contract

```go
package store

type Store interface {
    Create(ctx context.Context, id string, m Meta) error                                   // ErrExists
    Append(ctx context.Context, id string, ev AppendRequest) (seq uint64, err error)       // ErrNotFound, ErrClosed, ErrCancelled (unless Force), ErrTooManyEvents, ErrConflict
    Read(ctx context.Context, id string, afterSeq uint64, limit int, block time.Duration) ([]rebound.Event, error) // may return (nil, nil) on block timeout
    Status(ctx context.Context, id string) (rebound.Status, error)
    Cancel(ctx context.Context, id string) error
    Touch(ctx context.Context, id string, at time.Time) error
    Close() error
}

type Meta struct { CreatedAt time.Time; IdleTTL, Retention time.Duration; MaxEvents int; Attrs map[string]string }
type AppendRequest struct { Kind string; Data []byte; At time.Time; Force bool; Seq uint64 } // Force: allowed even if cancelled, for terminal events; Seq: expected seq, 0 = next (ADR 0005)
```

Package layout note (ADR 0002): `Event`, `State`, `Status`, `IsTerminal`, `Clock` and the store
sentinels (`ErrNotFound`, `ErrExists`, `ErrClosed`, `ErrCancelled`, `ErrGap`, `ErrTooManyEvents`,
`ErrConflict`) are declared in
`store` and re-exported by `rebound` as aliases, so `rebound.Event` and `store.Event` are the same
type and the signatures above compile as written without an import cycle.

Rules every store must satisfy (verified by `store/storetest`):

- `Append` assigns `seq = LastSeq+1` **atomically** with the write, updates `LastActivity`,
  refreshes TTL, and — when `Kind` is terminal — flips `State` and switches the TTL to `Retention`.
  A second terminal append returns `ErrClosed`.
- `MaxEvents` is a hard cap (ADR 0004): a non-terminal append on a stream that holds `MaxEvents`
  events returns `ErrTooManyEvents`; the terminal append is always accepted. A running stream is
  never trimmed; a store may trim a terminal stream during retention.
- With `Seq > 0` (ADR 0005), checked first and atomically: if event `Seq` is already stored with the
  same kind and data, `Append` returns `Seq` and changes nothing; if `Seq ≤ LastSeq` otherwise, it
  returns `ErrClosed` on a terminal stream, else `ErrConflict`; `Seq > LastSeq+1` is `ErrConflict`.
- `Append` of a non-terminal event on a cancelled stream returns `ErrCancelled`; with `Force: true`
  it succeeds (only the Hub uses Force, for terminal events).
- `Read` returns events with `Seq > afterSeq` in order, at most `limit`, **from the log itself**
  (never from a separate pub/sub channel), blocking up to `block` when none are available yet.
  Returns `ErrGap` if `afterSeq+1 < FirstSeq`, `ErrNotFound` for unknown ids. It must return
  promptly when `ctx` is cancelled.
- No method returns an error matching `io.EOF`: to readers that value means "end of stream" (ADR 0005).
- `Status` is cheap and consistent with the last completed `Append`.

## 7. Non-goals for v0.1

WebSockets · Postgres store · built-in authentication · metrics exporters (hooks only) ·
resuming a *producer* after its process dies (the stream fails with `producer_lost`; the app may
start a new one) · message history beyond `Retention` · HTTP/2 push.

## 8. Compatibility

Go 1.26+, i.e. the two supported Go releases (ADR 0003; was 1.23). Redis 6.2+. Redis Cluster supported via
`{id}` hash tags. Browsers: anything with `fetch` + `ReadableStream`.
