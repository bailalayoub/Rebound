# Contributing to Rebound

Thanks for taking the time. Bug reports, fixes, new stores, adapters and documentation are all
welcome. This page says what you need, how to run the checks, and the rules a pull request must
follow.

## Prerequisites

- **Go 1.26 or newer** (the two supported Go releases, [ADR 0003](../docs/adr/0003-minimum-go-version.md)),
  with a C compiler for `-race` (gcc on Linux, Xcode tools on macOS).
- **GNU make** and **bash**.
- **Docker** for Redis (`make redis-up`), the multi-instance demo and the CI-parity
  container.
- **Node.js 20.19+ or 22.12+** for the TypeScript client.
- The linters at pinned versions: `make tools` installs gofumpt, goimports, golangci-lint and
  govulncheck.

No local Go toolchain, or on Windows? `scripts/in-docker.sh <target>` runs any make target in a
container with Go 1.26, gcc and the pinned linters (default target: `check`).

## Commands

| Command | What it does |
|---|---|
| `make check` | fmt check, `go vet`, golangci-lint, the `time.Sleep` check, `go test -race`. **Must be green before every commit.** |
| `make test` | unit tests, without the race detector (quick loop) |
| `make cover` | coverage with per-package thresholds (`scripts/coverage-gate.sh`) |
| `make test-integration` | tests tagged `integration`, against a real Redis (`REDIS_ADDR`, default `localhost:6379`) and a cluster-mode Redis (`REDIS_CLUSTER_ADDR`) |
| `make conformance` | builds the example server and the chaos tester, runs it with the memory store |
| `make conformance-redis` | the same against two instances + Redis + nginx (docker compose) |
| `make readme-check` | the README quick start is `examples/quickstart/main.go` and builds in a fresh module |
| `make client-test` | TypeScript client tests and an `npm pack` check |
| `make demo` | the example chat on http://localhost:8080 (memory store, fake model) |

CI runs `check`, `cover`, `readme-check` and `govulncheck` on Go 1.26 and the latest stable, plus
the integration tests, the conformance run and the client tests.

## Rules

The full conventions are in [docs/project/code-standards.md](../docs/project/code-standards.md). The ones a
review will check first:

1. **Tests first.** Write or extend the test that shows the bug or the missing behaviour, watch it
   fail, then make it pass. A fix without a test is not finished.
2. **Always `-race`.** The library is concurrent by nature; a race report is a blocking bug.
3. **No flaky tests.** A test that fails once in 20 runs is a bug. Use the injected clock
   (`internal/testutil.Clock`); no `time.Sleep` in unit tests (`make check` rejects it). Polling
   with `testutil.Eventually` in 10 ms steps is fine.
4. **Never weaken a test to make it pass**: no `t.Skip`, no deleted assertions, no lowered coverage
   thresholds, no `//nolint` without a one-line reason.
5. **Library code never panics, never logs secrets or payloads, never uses global state.** Errors
   are wrapped with `%w` and matched with `errors.Is`.
6. **The spec rules the code.** [docs/02-spec.md](../docs/02-spec.md) defines the public API, the wire
   protocol and the guarantees. A change to any of them starts with an ADR (below), then the spec,
   then the code, in the same pull request.
7. **Dependencies are allowlisted.** Runtime: `github.com/redis/go-redis/v9`, and
   `github.com/gin-gonic/gin` only in `ginx/` and the example. Tests: `github.com/alicebob/miniredis/v2`,
   `github.com/stretchr/testify`. TypeScript client: no runtime dependency; `typescript` and `vitest`
   for development. Anything else needs an ADR.
8. **Small commits, [Conventional Commits](https://www.conventionalcommits.org/).** One logical
   change per commit, scoped by package: `feat(store/redis): …`, `fix(httpx): …`, `docs: …`,
   `test(core): …`. For any user-visible change, add a line under `## [Unreleased]` at the top of
   [CHANGELOG.md](../CHANGELOG.md) (create the heading if it is not there).

## Proposing an ADR

Architecture Decision Records live in [docs/adr/](../docs/adr). Write one when a change touches the
public API, the wire protocol, the store contract, the guarantees or the dependency list.

1. Copy the shape of an existing ADR (for example [0005](../docs/adr/0005-idempotent-append.md)) into
   `docs/adr/NNNN-short-title.md`, with the next free number.
2. Keep it to one page: **Status** (`proposed`), **Context** (the problem, with evidence: a failing
   test, a benchmark, a link), **Decision**, **Consequences** (what breaks, what it costs, what is
   left out), and the alternatives you rejected.
3. Open a pull request with the ADR alone, or with the ADR, the spec change and the code together.
   The ADR is accepted when the pull request is merged; its status then becomes `accepted`.

For a small idea, open a feature request issue first: it is cheaper to agree on the direction
before writing the ADR.

## Pull requests

- Fork, branch from `main`, keep the pull request focused on one change.
- `make check` green locally; mention in the description which other targets you ran
  (`test-integration`, `conformance`, `client-test`) when your change can affect them.
- Fill in the pull request template. Link the issue or the ADR.
- Security issues: do not open a pull request or an issue; follow [SECURITY.md](SECURITY.md).

By contributing you agree that your contributions are licensed under the [MIT License](../LICENSE),
and you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
