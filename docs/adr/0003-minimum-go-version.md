# ADR 0003 — Minimum Go version: from 1.23 to the oldest supported release (1.26)

Status: accepted · Date: 2026-10-11

## Context

Spec §8, the contributor guide and roadmap M0 fix the floor at Go 1.23. As of 2026-10 the supported Go releases
are 1.27 (go1.27.2) and 1.26; Go 1.23 stopped receiving security fixes in August 2025.

Once `httpx` serves HTTP, `govulncheck ./...` built with go1.23.12 reports 29 reachable
vulnerabilities (M2, 2026-10-11):

- 26 in the Go 1.23 standard library (`net/http`, `crypto/tls`, `crypto/x509`, `net/url`, …), fixed
  only in Go 1.24+ toolchains. A library cannot fix these; users get them from their toolchain.
- 3 in `golang.org/x/net` v0.25.0 (GO-2026-6603, -6611, -6617), pulled in by gin v1.10.1, fixed in
  x/net v0.60.0. The last x/net release that builds with Go 1.23 is v0.38.0.

Allowlisted dependencies have moved on: gin's latest (v1.12.0) requires Go 1.25, go-redis's latest
(v9.23.0, needed for M3) requires Go 1.26. M5 requires `govulncheck` clean. With a 1.23 floor
and gin in the module, the documents contradict each other.

## Options

1. **Floor = oldest supported Go release (1.26)** — recommended. `go 1.26.0` in go.mod; gin v1.12.0,
   x/net ≥ v0.60.0, go-redis v9.23.0; CI tests on 1.26.x and stable; govulncheck runs on stable.
   Matches common library policy ("the two supported Go releases"). Users on 1.23–1.25 must upgrade
   Go, which they need anyway for security fixes.
2. **Keep 1.23, pin old dependencies.** gin v1.10.1, x/net ≤ v0.38.0, an old go-redis. Known
   vulnerabilities stay in the dependency floor; the M5 goal (`govulncheck` clean) cannot be met
   unless the check runs on a newer toolchain and ignores the x/net findings.
3. **Split `ginx` (and the example) into their own modules.** The core module could stay on a lower
   floor without gin, but stdlib findings still require a patched toolchain, go-redis still pushes
   the floor up, and every release then needs several tags. More release machinery for little gain.

## Decision

Option 1, applied in one change: go.mod `go 1.26.0`; gin v1.12.0, golang.org/x/net v0.61.0 and
quic-go v0.59.1 (gin's HTTP/3 dependency, GO-2026-5676 fixed in v0.59.1); `golang:1.26` dev image
with golangci-lint v2 (v1 cannot lint Go 1.26 code; config migrated to the v2 format); CI matrix
[1.26.x, stable] running check, coverage and govulncheck on both; spec §8, the contributor guide and the roadmap
updated to "Go 1.26+ (the two supported Go releases)".

## Consequences

- `govulncheck` clean on a supported toolchain; current gin and go-redis usable.
- Users on Go < 1.26 cannot use v0.1.0. Pre-1.0, documented in the changelog.
- The floor moves with Go's release cycle; each bump is listed in the changelog.
