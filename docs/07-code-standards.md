# 07 — Code standards

## Go

### Style and structure
- `gofumpt` formatting; `goimports` grouping (std, third-party, module).
- Package names: short, lowercase, no underscores (`httpx`, `ginx`, `storetest`).
- No `pkg/`, no `utils`, no `common`. `internal/` for anything not meant to be imported.
- Context is always the first parameter and is never stored in a struct.
- Functional options (`Option func(*config) error`) for `New`; validate in `New`, return errors, never panic.
- No package-level mutable state. No `init()` except for `go:embed`-free constants.
- Exported types and functions have doc comments that start with the identifier's name.
- Keep functions short enough to read without scrolling; prefer a named helper to a comment that explains a block.

### Errors
- Sentinels in `errors.go`; wrap with `fmt.Errorf("rebound: append %s: %w", id, err)`; match with `errors.Is`.
- Every error message starts with the package name (`rebound:`, `redis store:`) and says what was attempted.
- Library code never logs *and* returns the same error; it returns. The Hub logs lifecycle events once, at the boundary.
- Never ignore an error without a comment stating why it is safe (`// ErrClosed here means another writer won the terminal race (G7).`).

### Concurrency
- Every goroutine has a clear owner and a way to stop (`ctx` or a close channel). Document who closes what.
- Channels: the sender closes. Prefer `select` with `ctx.Done()` over unbuffered sends.
- Protect shared maps with a mutex; keep critical sections tiny; never call out to the store or the network while holding a lock.
- `-race` on every test run. Any race report is a blocker-level bug.

### Logging
- `log/slog`, injected via `WithLogger`. Keys: `stream_id`, `seq`, `kind`, `reason`, `duration_ms`, `store`.
- `Info`: stream start/end, cancel, hub close. `Debug`: per-event, per-read. `Warn`: producer lost, gap, write timeouts. `Error`: store failures.
- Never log event payloads or `Meta` values (they may contain user data).

### Testing
- Table-driven tests; subtests named for the case; `t.Parallel()` wherever state allows.
- `testify/require` for fatal assertions, `assert` for the rest; keep messages meaningful.
- Fake clock (`internal/testutil.Clock`) for anything time-dependent. **No `time.Sleep` in unit
  tests**; `make check` greps for it under non-integration test files.
- Polling helper `testutil.Eventually(t, cond, timeout)` with 10 ms steps when a goroutine hand-off cannot be observed otherwise.
- Golden files for wire formats (`sse/testdata/*.golden`), updated with `-update`.
- Randomized tests take a `-seed` flag, print the seed on failure, default to a time-based seed.
- Integration tests carry `//go:build integration` and read `REDIS_ADDR` (default `localhost:6379`).
- Coverage thresholds enforced by `make cover`: `rebound` 85 %, `store/memory` 85 %, `store/redis` 85 %, `sse` 90 %, `httpx` 85 %, `ginx` 80 %. Tool packages (`cmd/`, `examples/`) are excluded.
- Example tests (`Example*`) for every public entry point users will copy: `New`, `Start`, `httpx`, `ginx`.

### Lint
`.golangci.yml` enables: `errcheck`, `govet`, `staticcheck`, `revive`, `gocritic`, `gosec`,
`misspell`, `unparam`, `ineffassign`, `unused`, `bodyclose`, `noctx`, `errorlint`, `gofumpt`,
`goimports`, `prealloc`, `nilerr`, `contextcheck`, `forcetypeassert`. `//nolint:<linter> // reason`
only, never bare.

### Security
- `crypto/rand` for ids. Strict parsing of anything from the network (`strconv.ParseUint(s, 10, 64)`).
- Caps on event size, events per stream, header length (`Last-Event-ID` ≤ 20 chars).
- `govulncheck ./...` in CI.
- No reflection-based decoding of untrusted input beyond `encoding/json` into fixed structs with `DisallowUnknownFields` where the schema is ours.

## TypeScript (clients/typescript)

- `strict: true`, ESM output + `.d.ts`, target ES2020, no runtime dependencies.
- Public surface documented with TSDoc; types exported.
- Tests with `vitest`; an in-process `node:http` server emulates drops; no network, no timers longer than 50 ms (use fake timers for backoff).
- `eslint` with `@typescript-eslint/recommended`; `prettier` defaults.

## Documentation
- README is for users: problem → 60-second quick start → guarantees → HTTP contract → options → FAQ. No internals.
- `docs/` is for contributors. Keep spec and architecture in sync with code in the same commit as the change.
- Every ADR ≤ 1 page.

## Repository hygiene
- `.editorconfig`, `.gitignore` (binaries, coverage, `dist/`, `node_modules/`).
- `CHANGELOG.md` in Keep a Changelog format; entries written with the feature, not at release time.
- Semantic versioning. Pre-1.0: breaking changes allowed in minor versions, always listed in the changelog.
- MIT license header not required in files; `LICENSE` at root.
