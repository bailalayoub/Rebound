# Roadmap

v0.1.0 was built in six milestones. The spec, the ADRs and a few test comments refer to them by
name (M0 to M5), so this page keeps what each one delivered, followed by what could come next.

## Delivered in v0.1.0

| Milestone | What it delivered |
|---|---|
| **M0 — Skeleton and tooling** | Module layout, Makefile, lint config, CI, error sentinels, event kinds and their validation, the fake clock used by every timing test. |
| **M1 — Core** | The `store.Store` interface, the memory store, the reusable `storetest` suite, the Hub (producers, subscriptions, heartbeats, `MaxDuration`, stale detection, `Close`) and the SSE encoder. `TestResume_NoGapNoDup` runs 500 seeded iterations under `-race`. |
| **M2 — HTTP** | `httpx.Handler` (cursor parsing, `Authorize`, the status table of spec §4.1, pings, write timeouts, clean exit on disconnect) and the Gin adapter `ginx`. |
| **M3 — Redis store** | The Redis Streams store: Lua scripts, gap detection, TTL switching, hash-tagged keys. `storetest` passes on miniredis and on a real Redis; two Hubs sharing one Redis behave as one. |
| **M4 — Conformance and multi-instance** | `examples/chat` as the conformance target, the chaos tester `cmd/rebound-conformance` (scenarios S1–S12, seeded drops) and the 2-instance compose behind nginx without sticky sessions. |
| **M5 — Client, example, release** | The zero-dependency TypeScript client, the chat UI with fake and OpenAI-compatible LLMs, the README quick start checked by `make readme-check`, godoc on every exported symbol. |

## Next

None of these is scheduled. Each one that changes the public API or the wire protocol starts with an
ADR (see [CONTRIBUTING.md](../../.github/CONTRIBUTING.md)).

- **Producer hand-off.** A producer runs on the instance that started it. If that instance dies,
  readers get `producer_lost`; nothing resumes the generation elsewhere.
- **Postgres store**, for teams that already run Postgres and not Redis.
- **Metrics adapter** (Prometheus or OpenTelemetry) on top of the existing `Hooks`.
- **Redis Cluster on several nodes.** Keys are hash-tagged and the integration tests run on a
  single-node cluster; a multi-node, multi-shard run is not part of CI yet.
- **More HTTP adapters**: chi, echo, fiber.
- **Other server languages** (Python, Node) passing the same conformance tester.
- **WebSocket transport**, if there is demand.
