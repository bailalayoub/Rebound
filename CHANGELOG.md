# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Repository layout** (no API change): CONTRIBUTING, SECURITY and CODE_OF_CONDUCT moved to
  `.github/`, the dev Redis compose file to `scripts/dev-redis.yml` (`make redis-up`), the README
  quick start fixture to `examples/quickstart`, the stress campaign to `docs/benchmarks/v0.1.0/`
  and the maintainer notes to `docs/project/`. The README is now a short overview with a map of
  the repository; the integration details moved to `docs/usage.md`, with an index in
  `docs/README.md`.

## [0.1.0] - 2026-10-26

First public release: resumable Server-Sent Events streams for Go.

### Added

- **Hub.** `rebound.New(store)` and `Hub.Start` run a producer detached from the request that
  started it: only `Cancel`, `MaxDuration` or `Close` stop it. `Subscribe` delivers every event
  after a cursor, replayed or live, in order, with no gap and no duplicate.
- **Exactly one terminal event per stream**: `done`, or `error` with one of `cancelled`,
  `producer_failed`, `producer_panic`, `timeout`, `producer_lost`, `server_shutdown`,
  `max_events_exceeded`. It is written even when the producer hangs (it is abandoned one heartbeat
  after the cause) or its process dies (readers notice the silence and end the stream).
- **Each event stored once.** The writer retries transient store errors with its expected sequence
  number, so a short Redis outage neither duplicates a token nor ends the generation.
- **Stores.** `store/memory` for one process and tests; `store/redis` on Redis Streams and Lua
  (Redis 6.2+, standalone, Sentinel or Cluster) for several instances, without sticky sessions;
  `store/storetest`, the suite any custom store must pass.
- **HTTP.** `httpx` (net/http) and `ginx` (Gin): resume from `Last-Event-ID` (header or
  `?last_event_id=`), status mapping (200, 204, 400, 404, 410, 401/403 via `WithAuthorize`),
  keep-alive pings, per-write deadlines, a cancel endpoint. `sse` holds the encoder and a strict
  cursor parser.
- **Options**: max duration, heartbeat interval, stale detection, retention, idle TTL, a hard cap
  on events per stream, exposed producer errors, logger, clock, id generator and metrics hooks;
  per stream: metadata, id and max duration.
- **TypeScript client** `@bailalayoub/rebound-client`: zero dependencies, `fetch`-based (custom
  headers), automatic resume with `Last-Event-ID`, backoff with jitter, honours `retry:` and
  `Retry-After`, never delivers an event twice.
- **`rebound-conformance`**, a black-box chaos tester (scenarios S1–S12, seeded connection drops)
  for any server that implements the HTTP contract, in any language.
- **Examples.** `examples/chat`, a chat UI that survives reloads, with a built-in fake model or any
  OpenAI-compatible API (OpenAI, Groq, OpenRouter, Gemini, Ollama); `deploy/`, two instances behind
  nginx round-robin on one Redis; `playground/`, the same on a real model with one
  `docker compose` command. All are local demos without authentication, bound to 127.0.0.1.
- **Docs**: the specification, the architecture, the conformance contract, eight ADRs, and
  [How it works, with diagrams](docs/how-it-works.md).

### Requirements

- Go 1.26 or newer (the two supported Go releases). Redis 6.2 or newer for `store/redis`.
  Node 18 or newer for the TypeScript client.

### Known limits

- A producer runs on the instance that started it; if that process dies, the stream ends with
  `producer_lost` and is not resumed elsewhere.
- Redis Cluster is tested on a single-node cluster.

[0.1.0]: https://github.com/bailalayoub/rebound/releases/tag/v0.1.0
