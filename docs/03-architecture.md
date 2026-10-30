# 03 — Architecture

## 1. The core idea in one picture

```
                 Start()                         Subscribe(after=n)
 app handler ─────────▶ Hub ◀─────────────────────── httpx / ginx ◀──── GET /streams/{id}
                         │                                  ▲            Last-Event-ID: n
            spawns       │ producer goroutine               │ reader loop (one per connection)
                         ▼                                  │
                  ┌─────────────┐   Append(seq++)    ┌──────┴──────┐
   LLM ───────▶   │  Writer     │ ─────────────────▶ │   Store     │  ◀── Read(after=n, block)
                  └─────────────┘                    │ log + meta  │
                                                     └─────────────┘
                                                      memory │ redis
```

Three roles, one log:

1. **Producer** appends events to the log. It never knows who is listening.
2. **Store** owns the ordered, numbered log and the stream's metadata (state, liveness, TTL).
3. **Readers** read *from the log* starting after a cursor. Replay and live are the same operation,
   which is what makes G2 (no gap, no duplicate) a property of the design rather than of careful
   locking.

## 2. Repository layout

```
rebound/
├── rebound.go             # public API: Hub, Subscription, Writer, Stream, types, sentinel errors
├── options.go             # public API: Option / StartOption, every With… option, Hooks
├── example_test.go        # runnable examples (pkg.go.dev); rebound_test.go tests the facade
├── internal/core/         # the implementation of package rebound, with its unit tests (ADR 0009)
│   ├── rebound.go         # Hub, Start, Subscribe, Cancel, Status, Close
│   ├── producer.go        # producer goroutine: detached ctx, heartbeat, timeout, terminal write
│   ├── subscription.go    # Subscription.Next: read loop, stale detection, EOF
│   ├── event.go           # Kind validation, terminal helpers
│   ├── options.go         # Option / StartOption, defaults
│   ├── errors.go          # sentinel errors
│   ├── clock.go           # Clock alias of store.Clock (ADR 0002)
│   └── hooks.go           # Hooks struct
├── store/
│   ├── store.go           # Store interface, Meta, AppendRequest
│   ├── types.go           # Event, State, Status, IsTerminal, Clock, store sentinels (ADR 0002)
│   ├── storetest/         # reusable conformance suite for any Store implementation
│   ├── memory/            # in-memory store (dev, tests, single instance)
│   └── redis/             # Redis Streams store (+ append.lua embedded with go:embed)
├── sse/                   # SSE encoder (events, comments, retry), Last-Event-ID parsing
├── httpx/                 # net/http handler: ServeStream, ServeCancel, Authorize, ping
├── ginx/                  # thin Gin adapters over httpx
├── internal/testutil/     # fake clock, helpers (never imported by public packages' API)
├── internal/sseclient/    # SSE client parser shared by httpx tests and the conformance tester
├── internal/conformance/  # conformance contract: test producer (server side) and chaos tester engine
├── cmd/rebound-conformance/# chaos tester (docs/06)
├── examples/chat/         # Gin chat app + fake/OpenAI-compatible LLM + web UI (docs/08)
├── examples/deploy/       # docker-compose for redis, 2 app instances, nginx round-robin
├── examples/playground/   # the same on a real LLM
├── clients/typescript/    # @bailalayoub/rebound-client: fetch-based resumable SSE client
├── docs/                  # these documents and the ADRs
├── Makefile, .golangci.yml, .github/workflows/ci.yml, go.mod, LICENSE, README.md, CHANGELOG.md
```

Rules: no `pkg/` directory; `internal/` for anything not part of the API; the root package has no
dependency on `httpx`, `ginx`, `gin` or `go-redis` (they depend on it, not the reverse). The root
package only forwards to `internal/core`: behaviour, invariants and their tests belong in
`internal/core` (ADR 0009).

## 3. Data model

### Event

`Seq` (uint64, dense from 1) · `Kind` (string) · `Data` ([]byte, ≤ 64 KiB).

### Stream metadata

`State` running|done|error · `ErrorCode` · `Seq` (last assigned) · `Cancelled` · `CreatedAt` ·
`LastActivity` · `IdleTTL` · `Retention` · `MaxEvents` · `Attrs`.

### Redis layout (hash tag keeps both keys on one cluster slot)

```
rebound:{<id>}:meta   HASH    state, seq, cancelled, created_ms, last_activity_ms, idle_ttl_ms, retention_ms, max_events, error_code, attrs (JSON)
rebound:{<id>}:log    STREAM  entry id "<seq>-0", fields: kind, data
```

`append.lua` (atomic):

```
if not exists meta         → return "NOTFOUND"
if expected seq given and expected ≤ meta.seq:          (ADR 0005, checked first)
    entry expected-0 has the same kind and data → return expected (replay, nothing written)
    state != running → return "CLOSED"; else → return "CONFLICT"
if expected seq given and expected > meta.seq + 1 → return "CONFLICT"
if state != running        → return "CLOSED"
if cancelled and not force → return "CANCELLED"
if not terminal and meta.seq ≥ max_events → return "TOO_MANY"   (ADR 0004: hard cap, no trimming)
seq = HINCRBY meta seq 1
XADD log <seq>-0 kind <kind> data <data>
HSET meta last_activity_ms <now>
if kind is terminal: HSET meta state <done|error> error_code <code>; PEXPIRE both retention_ms
else:                PEXPIRE both idle_ttl_ms
return seq
```

Reads:
- `read.lua`: existence, gap check (`XRANGE log - + COUNT 1` gives `FirstSeq`) and
  `XRANGE log <after+1>-0 + COUNT <limit>` in one atomic step.
- wait: `XREAD COUNT 1 BLOCK <≤250 ms> STREAMS log <after>-0` is only a wake-up signal; at most
  `WithMaxBlockingReads` reads hold a pooled connection this way, the others poll every 250 ms
  (ADR 0008), so waiting readers cannot starve appends.

Because both history and live reads come from the stream itself, there is no replay/live seam.

### Memory layout

`map[streamID]*stream` guarded by a `sync.RWMutex`; each stream holds `events []Event` (at most
`MaxEvents` plus the terminal, never trimmed), metadata, and a `notify chan struct{}` that is closed
and replaced on every append (broadcast). `Read` with `block > 0` snapshots `events` under lock, and
if empty waits on `notify`, `ctx.Done()` or a timer, then re-reads. TTLs are enforced lazily on the
injected clock, with a sweep in `Create`.

## 4. Sequences

### 4.1 Start and produce

```
Start(ctx, produce):
  id   = idgen()
  store.Create(id, meta)                       # ErrExists → retry with new id (max 3) unless custom id
  go runProducer(id, produce)
  return Stream{id}

runProducer:
  pctx = WithoutCancel(ctx) + WithTimeout(MaxDuration) + cancel registered in hub.cancels[id]
  ticker(HeartbeatInterval): store.Touch(now); st = store.Status(); if st.Cancelled → cancel pctx
  err = call produce(pctx, writer) with recover, in its own goroutine
  if pctx is cancelled and produce has not returned after HeartbeatInterval → abandon it (ADR 0006)
  stop the heartbeat
  terminal (Append with Force, retried on transient errors for ≤ StaleAfter/2):
    produce returned nil                      → Append(done, {} , Force)
    ctx deadline exceeded                     → Append(error timeout, Force)
    st.Cancelled or ErrCancelled seen         → Append(error cancelled, Force)
    hub closing                               → Append(error server_shutdown, Force)
    ErrTooManyEvents seen                     → Append(error max_events_exceeded, Force)
    panic                                     → Append(error producer_panic, Force)
    other error                               → Append(error producer_failed, Force)
  ignore ErrClosed on the terminal append (someone else already closed it: G7)
  hooks.OnStreamEnd with the stored terminal (read back from Status when another writer won)
```

The `Writer` normalizes the payload (UTF-8, LF; ADR 0007) and wraps `store.Append` with `Force: false` and its expected `Seq`, under a mutex, retrying transient errors for at most `StaleAfter/2` (ADR 0005); on `ErrCancelled`/`ErrClosed`/`ErrTooManyEvents` it also cancels `pctx` so a producer that ignores its context still stops at the next write.

### 4.2 Subscribe and read

```
Subscribe(ctx, id, after):
  st = store.Status(id)            # ErrNotFound
  if after > st.LastSeq            → ErrInvalidCursor
  if st.terminal && after == st.LastSeq → return Subscription that yields io.EOF immediately (httpx maps to 204)
  return &Subscription{cursor: after}

Next(ctx):
  loop:
    evs, err = store.Read(ctx, id, cursor, batch=64, block=min(StaleAfter/3, 5s))
    ErrGap → return ErrGap; any store error matching io.EOF is converted (ADR 0005)
    if len(evs) > 0: buffer them; cursor = last.Seq; pop first; if it is terminal mark eof-after
    else:
      st = store.Status(id)
      if st.State != running: continue              # terminal exists, next Read returns it
      if (st.LastSeq, st.LastActivity) changed: remember them and the local time; continue
      if local now - time of the last observed change > StaleAfter:   # own clock only (ADR 0006)
        store.Append(error producer_lost, Force)     # ignore ErrClosed (G7)
        continue
```

The producer-lost write is performed by readers because the producer's process may be dead; the
conditional terminal append makes it safe when several readers race.

### 4.3 HTTP reader loop (`httpx.ServeStream`)

```
parse cursor (header, then query) → 400 on error
authorize(ctx, r, status) → 401/403
sub, err = hub.Subscribe(ctx, id, cursor) → 404 / 400 / 410 / 204 mapping
write headers, "retry: <ms>", flush
events := goroutine pumping sub.Next into a channel
select loop:
  event   → sse.WriteEvent; flush; if terminal → return
  ping tick → sse.WriteComment("ping"); flush
  r.Context().Done() → sub.Close(); return   # client went away; producer unaffected
  write error / timeout → sub.Close(); return
```

The per-flush `WriteTimeout` uses `http.ResponseController.SetWriteDeadline` so one stuck client
cannot pin a goroutine forever.

### 4.4 Cancel

```
POST /streams/{id}/cancel → hub.Cancel(id) → store.Cancel(id) (sets flag) → 202
producer (same or other instance) notices within HeartbeatInterval → pctx cancelled →
produce returns → Append(error cancelled, Force) → all readers receive it → close.
```

Fast path on the same instance: `hub.Cancel` also cancels `pctx` directly if the producer is local.

## 5. Concurrency and lifecycle

- One goroutine per producer, one per HTTP reader (plus its pump). No shared mutable state
  outside the Store and the Hub's `cancels` map (mutex-guarded).
- `Hub.Close(ctx)`: stop accepting `Start` (`ErrHubClosed`), let local producers finish until the
  `ctx` deadline (spec §3), then cancel the ones still running and force-append `server_shutdown`
  for them.
- Backpressure: a slow reader slows only its own connection; the producer appends to the store at
  its own pace. `MaxEvents` bounds memory; `MaxEventBytes` bounds a single event.

## 6. Failure matrix

| Failure | Detected by | Outcome |
|---|---|---|
| Client disconnects | reader loop (`r.Context()`) | reader exits; producer continues; client resumes with `Last-Event-ID` |
| Client reconnects on another instance | nothing to detect | works by construction (shared log) |
| Producer returns error / panics | hub | terminal `error{producer_failed|producer_panic}` |
| Producer exceeds `MaxDuration` | pctx deadline | terminal `error{timeout}` |
| Producer process dies | readers observing no change of `LastSeq`/`LastActivity` | terminal `error{producer_lost}` after `StaleAfter` of observation |
| Producer ignores its context | Hub, one `HeartbeatInterval` after cancellation | producer abandoned, terminal for the cause (ADR 0006) |
| Cancel from any instance | heartbeat tick / local fast path | terminal `error{cancelled}` ≤ `HeartbeatInterval` |
| Redis unavailable | store errors | `Start` fails; `Next` returns the error; readers get HTTP 503 before headers are sent, or the connection closes after (client retries with backoff) |
| Producer exceeds `MaxEvents` | store (`ErrTooManyEvents`) | terminal `error{max_events_exceeded}`; running streams are never trimmed (ADR 0004) |
| A store trimmed a terminal stream | `Read` | `ErrGap` → HTTP 410 → client stops with `gap` |
| Store reply lost (timeout, dropped connection) | `Writer` | same request retried; the expected `Seq` makes it idempotent (ADR 0005) |
| Server shutdown | `Hub.Close` | terminal `error{server_shutdown}` for producers that could not finish |

## 7. Observability

`Hooks` callbacks receive stream id, kind and durations; `slog` logs at `Info` for lifecycle
(start/end with reason, duration, event count) and `Debug` for per-event details. No metrics
library in v0.1; the example app shows how to wire Prometheus counters in 20 lines through hooks.

## 8. Security notes

- Stream ids are 128-bit random; knowing an id is the only capability needed to read, so apps
  that need stricter control use `WithAuthorize` with the `Meta` set at `Start` (e.g. `user_id`).
- Producer error messages are hidden by default (`producer_failed` without text).
- Event size and count are capped; `Last-Event-ID` is parsed strictly as a decimal uint64.
- No eval of user input; the Lua script is embedded and loaded with `EVALSHA` + fallback `EVAL`.

## 9. Testing architecture

- `store/storetest`: one suite, run against `memory`, `miniredis` (unit) and real Redis
  (`//go:build integration`). Covers every rule in spec §6 including the atomicity of terminal appends under 50 concurrent writers.
- Root package: table tests + a **randomized resume test** (`TestResume_NoGapNoDup`): N streams,
  random event counts and sizes, readers that drop at random cursors and resume, seeded and logged
  so any failure is reproducible with `-run TestResume -seed=…`.
- `httpx`: `httptest` end-to-end with real SSE parsing; 204/400/404/410 matrix; ping timing with
  the fake clock; write-deadline behaviour.
- `cmd/rebound-conformance`: black-box, over real TCP, both stores, and the 2-instance compose.
- `clients/typescript`: unit tests for the SSE parser and the reconnect state machine against an
  in-process Node server that drops connections on purpose.
