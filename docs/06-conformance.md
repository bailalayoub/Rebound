# 06 — Conformance tester

`cmd/rebound-conformance` is a black-box chaos client. It proves the guarantees of spec §2 over real
TCP against any server that implements the small contract below — Rebound itself, or a Python or
Node library someone wants to compare. It is also the demo: "drop the connection 30% of the time,
the text is still identical, byte for byte".

## 1. Server contract

The server under test exposes three endpoints. Rebound's `examples/chat` implements them when
started with `CONFORMANCE=1` (the start endpoint is test-only and off by default); so can anyone
else.

### `POST /conformance/streams`

Starts a deterministic test producer.

```json
{
  "text": "Lorem ipsum …",      // required; the full expected payload
  "chunk": 3,                   // characters per event (default 4)
  "delay_ms": 10,               // pause between events (default 10)
  "fail_after": -1,             // if ≥ 0: producer returns an error after this many events
  "kinds": false                // if true: every 10th event uses kind "meta" with data {"i":n}
}
```

Precisely: the event with seq `n` is `meta {"i":n}` when `kinds` is true and `n % 10 == 0`;
otherwise it is the next chunk of `text`. `fail_after = k` stops after `k` data events (meta
events do not count) and the stream ends with `error{producer_failed}`. The tester computes this
exact sequence (`internal/conformance.ExpectedEvents`) and compares kinds and payloads event by
event, not only the reassembled text.

Response `201 {"stream_id":"…","url":"/streams/<id>"}`.

Each `data` event's payload is the raw chunk (no JSON wrapping) so the tester can reassemble the
text by concatenating `data` payloads in `Seq` order. `done` carries `{}`; `error` carries
`{"code":…}` as in spec §5.

### `GET /streams/{id}` and `POST /streams/{id}/cancel`

Exactly spec §4.

## 2. Scenarios

| ID | Name | What it does | Pass condition |
|---|---|---|---|
| S1 | Happy path | one connection, no drops | text identical; seqs 1..n dense; terminal `done` |
| S2 | Random drops + resume | close the connection with probability `drop-rate` after each event; reconnect with `Last-Event-ID` after a random 0–200 ms | identical text; no dup/gap across all connections; terminal received exactly once |
| S3 | Restart from zero | drop 1–3 times, then reconnect **without** `Last-Event-ID` and read to the end (drops are capped: each reconnection starts over at seq 1) | each connection's events start at 1; final assembled text (from the last connection) identical |
| S4 | Cancel | cancel at a random point via POST | terminal `error{cancelled}` within 2 s; no `data` event after it; text is a strict prefix |
| S5 | Producer failure | `fail_after = k` | terminal `error{producer_failed}` after exactly k data events |
| S6 | Concurrent readers | `concurrency` readers on the same stream, each with its own drop pattern | all assemble the identical text |
| S7 | Invalid cursor | `Last-Event-ID = LastSeq+1000` after done | HTTP 400 |
| S8 | Unknown stream | random id | HTTP 404 |
| S9 | Already complete | after `done`, reconnect with `Last-Event-ID = LastSeq` | HTTP 204, no body |
| S10 | Parallel streams | `parallel` streams at once, each with S2 behaviour | all pass; p95 time-to-first-event reported |
| S11 | Custom kinds | `kinds = true` | `meta` events interleaved at the right positions; text (data only) identical |
| S12 | Late joiner | connect only after the producer finished | full text replayed; terminal present; a second connect with `Last-Event-ID = LastSeq` → 204 |

Default run: S1–S12, `runs = 200` for S2 (the core), 20 for the others.

## 3. CLI

```
rebound-conformance \
  --base-url http://localhost:8080 \
  --runs 200 --parallel 20 --concurrency 3 \
  --drop-rate 0.3 --text-size 2000 --chunk 4 --delay-ms 5 \
  --seed 0            # 0 = random; the chosen seed is always printed
  --scenarios S1,S2   # default: all
  --json              # machine-readable report
  --timeout 60s
```

Exit code 0 only when every scenario passes. The table output shows, per scenario: runs, failures,
reconnections, p50/p95 time-to-first-event, p95 time-to-terminal, and the seed. On failure it
prints the stream id, the sequence of connections (cursor → events received → how it ended) and
the first divergence between expected and assembled text.

## 4. Implementation notes

- Single Go binary in `cmd/rebound-conformance`, standard library only (plus `testify` is not
  needed). Its SSE parser lives in `internal/sseclient` and is shared with `httpx` tests.
- Drops are implemented by cancelling the request context, which closes the TCP connection
  mid-body — the same thing a dying mobile network does from the server's point of view.
- Everything random derives from one `math/rand/v2` source seeded by `--seed`; print the seed first.
- The tester must itself be free of races (`go test -race` on its internal packages) and must not
  leak goroutines between runs (checked with `goleak`-style counting in tests; no new dependency —
  use `runtime.NumGoroutine` deltas with a generous bound).

## 5. Make targets

```
make conformance          # memory: starts examples/chat in-process on a free port, runs the tester
make conformance-redis    # docker compose up redis + 2 app instances + nginx, runs the tester against nginx
```

Both run in CI (the Redis variant with the `services:` Redis and a single instance; the
2-instance compose is run locally).
