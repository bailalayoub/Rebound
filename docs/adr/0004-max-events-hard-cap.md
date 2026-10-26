# ADR 0004 — MaxEvents is a hard cap; running streams are never trimmed

Status: accepted · Date: 2026-10-11

## Context

Spec §3.2 made `MaxEvents` an approximate cap enforced by trimming the oldest events, and §4.1 told
clients to "restart from 0" after a 410. Once a log is trimmed, cursor 0 is itself older than
`FirstSeq-1`, so the restart gets 410 again: a stream longer than `MaxEvents` could never be read
from the start, and the documented recovery could not succeed. Redis `MAXLEN ~` also trimmed far
less precisely than the memory store (50 events kept for a cap of 10).

## Decision

1. `MaxEvents` is a hard cap on the non-terminal events of a stream. A non-terminal `Append` on a
   stream that already holds `MaxEvents` events fails with the store sentinel `ErrTooManyEvents`.
   The terminal append is always accepted, so a stream holds at most `MaxEvents + 1` entries.
2. The Hub ends such a stream with terminal `error{code:"max_events_exceeded"}`. `Writer` methods
   return an error matching both `ErrClosed` and `ErrTooManyEvents`.
3. A running stream is never trimmed. A store may trim a terminal stream during retention; the
   built-in stores do not. `ErrGap` / HTTP 410 remain for that case and for third-party stores.
4. 410 is final. The TypeScript client reports `onError({code:"gap", fatal:true})` and stops; it no
   longer restarts from 0.

## Consequences

- Every reader, including a late joiner with cursor 0, can read a complete stream: G2 holds
  without exception for the built-in stores.
- Memory per stream is bounded by `MaxEvents × MaxEventBytes`, as before; trimming no longer hides it.
- A producer that needs more than `MaxEvents` events fails visibly instead of silently losing
  history; applications with long outputs raise `MaxEvents` (default 10 000).
- Memory and Redis behave identically again (ADR 0001); `MAXLEN ~` is gone.
