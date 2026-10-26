# ADR 0005 — Idempotent appends with an expected sequence number; no io.EOF from stores

Status: accepted · Date: 2026-10-11

## Context

`Store.Append` assigned `LastSeq+1` blindly. When its reply was lost (timeout, connection dropped
after the request), the event was stored but `Writer.Write` returned an error: retrying duplicated
the token, not retrying ended the generation. A 2 s Redis restart killed a 60-event stream after 18
events. Separately, go-redis reports a closed connection as `io.EOF`, which `Subscription.Next`
wrapped with `%w`, so callers saw the end-of-stream signal before the terminal event.

## Decision

1. `store.AppendRequest` gains `Seq uint64`, the expected sequence number (0: assign the next one,
   used for terminal writes by the Hub and by readers). With `Seq > 0`, atomically:
   - `Seq ≤ LastSeq` and the stored event `Seq` has the same kind and data: return `Seq` and change
     nothing (the replay of a write whose reply was lost). This check comes first.
   - `Seq ≤ LastSeq` otherwise: `ErrClosed` if the stream is terminal, else `ErrConflict`.
   - `Seq > LastSeq + 1`: `ErrConflict`.
   - `Seq == LastSeq + 1`: the usual rules.
2. The `Writer` owns the next sequence number of its stream and serializes its appends with a mutex
   held across the store call. It is the only way to keep concurrent `Write` calls dense and
   ordered; the lock is local to one producer and guards no shared Hub state. On any error other
   than the stream's end (`ErrClosed`, `ErrCancelled`, `ErrNotFound`, `ErrTooManyEvents`,
   `ErrConflict`) it retries the same request with backoff (50 ms doubling to 1 s, on the Hub
   clock) for at most `StaleAfter / 2`. Terminal writes are retried the same way; a retry that finds
   a terminal already stored (`ErrClosed`) is success by G7.
3. Stores must not return errors matching `io.EOF` (spec §6). The Redis store reports a closed
   connection without wrapping `io.EOF`, and `Subscription.Next` converts such errors from any
   store, so `io.EOF` from `Next` always means "terminal event delivered".

## Consequences

- One `Write` stores its event exactly once, whatever the network does; go-redis's own retries are
  safe too. Short store outages no longer end generations.
- New store sentinel `ErrConflict` (re-exported by `rebound`); `storetest` covers the replay rules.
  Third-party stores must implement the `Seq` check atomically.
- A producer blocked in a retrying `Write` gets its answer at most `StaleAfter / 2` later, before
  readers could declare it lost.
