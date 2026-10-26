# ADR 0002 — Shared vocabulary lives in `store`, re-exported by `rebound`

Status: accepted · Date: 2026-10-11

## Context

Spec §6 declares the `store.Store` interface in terms of `rebound.Event`, `rebound.Status` and the
`rebound` sentinel errors, while spec §3 has `rebound.New(s store.Store, …)`. Taken literally, the
root package imports `store` and `store` imports the root package: an import cycle, which Go
rejects. The same knot appears for time: the memory store needs the injected `Clock` (TTL janitor,
blocking reads), the fake clock in `internal/testutil` must implement it, and root-package tests
need both the fake clock and the memory store.

Third-party stores must be able to implement `store.Store` and return the right sentinels, so the
shared types cannot live under `internal/`.

## Decision

1. Package `store` is the leaf package of the module. It defines the vocabulary every store and the
   Hub share: `Event`, `State` (+ constants), `Status`, `IsTerminal(kind)`, the kind constants,
   `Clock`/`Timer`/`Ticker` + `RealClock`, and the sentinels a store returns: `ErrNotFound`,
   `ErrExists`, `ErrClosed`, `ErrCancelled`, `ErrGap`.
2. Package `rebound` re-exports them with **type aliases** (`type Event = store.Event`) and
   **sentinel aliases** (`var ErrNotFound = store.ErrNotFound`). Hub-only sentinels
   (`ErrInvalidCursor`, `ErrInvalidKind`, `ErrTooLarge`, `ErrHubClosed`) and user-kind validation
   stay in `rebound`.
3. Spec §3 and §6 keep their wording: with aliases, `rebound.Event` *is* `store.Event`, so
   `errors.Is(err, rebound.ErrNotFound)` and every signature in the spec compile unchanged.

## Consequences

- No import cycle; `store`, `store/memory`, `store/redis` and `internal/testutil` never import the
  root package, so root-package white-box tests can use the memory store and the fake clock.
- The public API seen by users is source-identical to the spec. `go doc` shows the alias
  (`type Event = store.Event`), which is the only visible difference.
- Sentinel error texts are prefixed `rebound:` even though they are declared in `store`, because
  they are what users see.
