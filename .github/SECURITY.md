# Security policy

## Supported versions

Rebound is pre-1.0. Security fixes go into the latest minor release only.

| Version | Supported |
|---|---|
| 0.1.x | yes |

## Reporting a vulnerability

**Please do not open a public issue, discussion or pull request for a security problem.**

Report it privately through GitHub Security Advisories:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability**
   ([direct link](https://github.com/bailalayoub/rebound/security/advisories/new)).
3. Describe the problem, the affected versions and packages, and how to reproduce it. A failing test
   or a minimal program is the most useful proof.

The report is visible only to you and the maintainers. You can expect an acknowledgement within
7 days and a first assessment within 14 days. Once a fix is ready, it is released and the advisory
is published with credit to you, unless you prefer to stay anonymous.

## Scope

In scope: the Go library (root package and `internal/core`, `store/...`, `sse`, `httpx`, `ginx`), the conformance
tester and the TypeScript client `@bailalayoub/rebound-client`. Examples of what matters: a reader
receiving events of another stream, a crafted `Last-Event-ID` or stream id that crashes the server
or exhausts its memory, a payload that breaks the SSE framing, secrets or payloads written to logs.

`examples/chat`, `examples/deploy/` and `examples/playground/` are local demos without authentication, bound to
127.0.0.1. Missing authentication there is by design, but a way to leak the playground's API key is
in scope.
