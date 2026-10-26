---
name: Bug report
about: Something does not behave as the spec or the docs say
title: ""
labels: bug
assignees: ""
---

<!-- Security problem? Do not open an issue: see SECURITY.md. -->

**What happened**

A clear description of the problem. Paste the error, the HTTP status or the SSE frames you got.

**What you expected**

What should have happened instead. If it contradicts a guarantee of `docs/02-spec.md`, quote it.

**How to reproduce**

The smallest program, test or `curl` sequence that shows it. A failing test is ideal.

```go

```

**Environment**

- Rebound version (`go list -m github.com/bailalayoub/rebound`):
- Go version (`go version`):
- Store: memory / redis (Redis version, standalone / Sentinel / Cluster):
- HTTP layer: net/http (`httpx`) / Gin (`ginx`) / other:
- Client: `@bailalayoub/rebound-client` version / EventSource / other:
- OS, and proxy or load balancer in front of the server if any:

**Anything else**

Logs (without secrets or payloads), conformance tester output (`rebound-conformance` with its seed),
screenshots.
