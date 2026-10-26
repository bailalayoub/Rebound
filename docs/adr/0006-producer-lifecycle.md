# ADR 0006 — Producer lifecycle: abandon stuck producers, observe staleness locally, bounded Close

Status: accepted · Date: 2026-10-11

## Context

The `timeout` and `cancelled` terminals were written only after `produce` returned, and heartbeats
continued meanwhile: a producer that ignored its context kept its stream running forever (G5).
Readers compared `LastActivity`, written with the producer host's clock, with their own clock: a
skewed or stepping clock ended healthy streams. `Close` wrote the `server_shutdown` terminals one by
one without a deadline. `OnStreamEnd` reported the producer's outcome, not the stored terminal.

## Decision

1. When a producer's context is cancelled (Cancel, MaxDuration, Close, MaxEvents, stream ended
   elsewhere), the Hub waits one `HeartbeatInterval` for `produce` to return. Then it abandons it:
   it writes the terminal for the cause, stops heartbeating and reports the end. The producer's
   goroutine runs until `produce` returns; its writes fail with `ErrClosed`.
2. A reader declares `producer_lost` when neither `LastSeq` nor `LastActivity` has changed for
   `StaleAfter`, measured on its own clock since it started observing. Timestamps written by other
   hosts are only compared for equality. A late reader therefore observes for up to `StaleAfter`
   before ending an orphaned stream.
3. At its deadline, `Close` cancels the remaining producers and writes their `server_shutdown`
   terminals concurrently, each bounded by 1 s, then returns. Producers that ignore their context
   are abandoned as in 1; `Close` does not wait for their goroutines.
4. `OnStreamEnd` and the "stream ended" log report the stored terminal: when another writer won,
   the reason is read back from `Status` (`done` or the stored error code).

## Consequences

- G5 holds for misbehaving producers: terminal within `MaxDuration + HeartbeatInterval` of creation
  while the producer's process lives, and within `StaleAfter` of a reader observing the silence
  when it dies.
- Clock skew between instances no longer matters; only each host's own clock does.
- `Close(ctx)` returns within about 1 s of its deadline whatever the store does.
