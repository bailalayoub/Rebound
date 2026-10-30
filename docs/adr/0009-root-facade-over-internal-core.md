# ADR 0009 — The root package is a facade over `internal/core`

Status: accepted · Date: 2026-10-30

## Context

Package `rebound` lives at the module root, so its files are the first thing a visitor sees on
GitHub. In v0.1.0 that was 9 source files and 13 test files next to the folders, and readers
reported that they did not know where to start. The tests cannot leave the package's folder: most
of them are white-box tests (`package rebound`) of unexported functions such as `writeTerminal`,
`checkLiveness` or `validateStreamID`, and Go requires those to sit with the code. Moving the
package itself to a subfolder would change the import path `github.com/bailalayoub/rebound` and
break every user.

## Decision

1. The implementation of package `rebound`, its white-box tests and their helpers move unchanged to
   `internal/core` (package `core`).
2. The root keeps the public API only, in `rebound.go` and `options.go`, with the same names,
   signatures and doc comments as v0.1.0:
   - `Hub` and `Subscription` are structs wrapping their `core` counterparts; every method
     forwards.
   - `Writer`, `ProducerFunc`, `Stream` and `Hooks` are declared again at the root, identical to
     `core`'s, so that pkg.go.dev shows their methods and fields. `Start` wraps the producer;
     `WithHooks` converts the struct.
   - `Option` and `StartOption` are aliases of `core`'s: they are opaque, and the `With…`
     functions that build them are declared at the root with their documentation.
   - Sentinel errors are the same values (`var ErrHubClosed = core.ErrHubClosed`), so `errors.Is`
     matches either.
3. The root holds no logic. `rebound_test.go` checks that every forward is wired; behaviour is
   tested in `internal/core`, with its own coverage threshold.

## Consequences

- The import path, the API and the behaviour are unchanged; users have nothing to do.
- The root shows four Go files: the API, the options, the examples and the facade test.
- A new public function or option is declared twice: the implementation in `internal/core`, the
  forward and its doc comment at the root. The compiler catches a missing forward only when it is
  used, so `rebound_test.go` must call every exported function.
- Stack traces and profiles show `internal/core` frames instead of `rebound` ones.
