# ADR 0001 — Numeric sequence numbers and a single log (no pub/sub)

Status: accepted · Date: 2026-10-11

## Context

Resumable SSE needs (a) a cursor the client can echo back and (b) a server-side way to deliver
"everything after the cursor", both for the past and for the future, with no gap and no duplicate
at the boundary. Common designs use Redis Pub/Sub for live delivery plus a buffer for replay; they
race at the seam and need careful double-reading. Redis stream ids (`ms-seq`) are strings and
differ from an in-memory implementation's natural counter.

## Decision

1. Every event carries a dense, per-stream `uint64` sequence number assigned **atomically by the
   store at append time** (Lua script in Redis, mutex in memory). The SSE `id:` is that number.
2. Readers only ever read **from the log** (`XRANGE`/`XREAD BLOCK` in Redis; the slice plus a
   broadcast channel in memory). There is no separate publish channel. Replay and live are the same
   call with a different cursor.
3. Terminal events are ordinary log entries written with a conditional, idempotent append, so any
   actor (producer, hub, reader detecting a lost producer) may close a stream and exactly one
   terminal entry exists.

## Consequences

- G2 (no gap, no duplicate) follows from the data structure instead of from locking discipline.
- Memory and Redis stores behave identically and share one test suite.
- Live latency is bounded by `XREAD BLOCK` wake-up (sub-millisecond in practice), not by polling.
- Trimming (`MAXLEN ~`) can drop old entries; resuming before `FirstSeq` yields `ErrGap`/HTTP 410,
  which the client handles by restarting from 0. Accepted trade-off for bounded memory.
- Redis ≥ 6.2 required for exclusive range starts.
