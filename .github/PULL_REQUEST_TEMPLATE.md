<!-- Thanks! Keep the pull request to one change. Security fixes: see SECURITY.md first. -->

## What and why

What this changes, and the problem it solves. Link the issue (`Fixes #…`) or the ADR.

## How it is tested

The test that failed before this change and passes after it, or the new tests.

## Checklist

- [ ] `make check` is green (fmt, vet, lint, `go test -race`).
- [ ] I ran the other targets my change can affect: `make cover` / `make test-integration` /
      `make conformance` / `make client-test` / `make readme-check`.
- [ ] No test was weakened: no `t.Skip`, no removed assertion, no lowered threshold, no bare `//nolint`.
- [ ] No `time.Sleep` in unit tests; time-dependent code uses the injected clock.
- [ ] Public API, wire protocol or dependency change: an ADR is included and `docs/02-spec.md` is updated.
- [ ] User-visible change: a line under `## [Unreleased]` in `CHANGELOG.md`.
- [ ] Commits follow Conventional Commits (`feat(httpx): …`, `fix(store/redis): …`).
