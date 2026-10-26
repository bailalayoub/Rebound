# ADR 0008 — API adjustments before the v0.1.0 release

Status: accepted · Date: 2026-10-11

## Decision

1. `New` rejects `WithIdleTTL(d)` when `d < StaleAfter + HeartbeatInterval`: a shorter TTL lets a
   live, silent producer's stream expire between heartbeats, or a dead producer's stream vanish
   before a reader can write `producer_lost`. The derived default is unchanged.
2. `Hub.Status`, `Subscribe` and `Cancel` check the id syntax (`[A-Za-z0-9._~-]{1,128}`) and answer
   `ErrNotFound` for anything else without calling the store.
3. `httpx.WithLogger(*slog.Logger)` (default `slog.Default()`): 503 answers, mid-stream store
   failures and write failures are logged with `stream_id`.
4. `store/redis`: `WithReadBatch` is removed (the Hub always passes its own limit, so it had no
   effect). `WithMaxBlockingReads(n)` bounds how many reads hold a pooled connection in
   `XREAD BLOCK` at once (default: half the pool of a `*redis.Client`, else 16). Other waiting
   readers poll every 250 ms without holding a connection, so readers can no longer starve appends
   and heartbeats (G6).

## Consequences

Pre-1.0 and unreleased: the removal is listed in the changelog. Spec §3.2 and §4.4 are updated.
