# 08 — Example app and TypeScript client

## 1. `examples/chat` — a resumable chat in one file set

Purpose: (a) the conformance target (docs/06 §1), (b) the demo, (c) the copy-paste reference for
users. It must stay small and readable; it is not a product.

### Endpoints

| Method | Path | Role |
|---|---|---|
| `POST` | `/chat` | `{"message": "…"}` → starts a producer → `201 {"stream_id","url"}` |
| `GET` | `/streams/:id` | `ginx.Stream` |
| `POST` | `/streams/:id/cancel` | `ginx.Cancel` |
| `POST` | `/conformance/streams` | deterministic producer (docs/06), only with `CONFORMANCE=1` |
| `POST` | `/loadtest/streams` | timestamped producer of the stress harness (docs/benchmarks/v0.1.0/README.md), only with `CONFORMANCE=1` |
| `GET` | `/` | static UI (`web/index.html`, `web/app.js`, built client) |
| `GET` | `/healthz` | `{"store":"memory|redis","instance":"<hostname>"}` |

### Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | |
| `REBOUND_STORE` | `memory` | `memory` or `redis` |
| `REDIS_ADDR` | `localhost:6379` | |
| `LLM_PROVIDER` | `fake` | `fake` or `openai` (any OpenAI-compatible chat completions API) |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | with `openai`: the API base including its version, as in the provider's docs; `/chat/completions` is appended |
| `LLM_API_KEY`, `LLM_MODEL` | — | required with `openai` (startup fails with a readable message otherwise); the key is never logged or displayed |
| `LLM_STREAM_USAGE` | `1` | `0` omits `stream_options.include_usage`, for providers that do not document it |
| `INSTANCE_NAME` | hostname | shown in the UI so multi-instance demos are visible |
| `CONFORMANCE` | off | `1` mounts the test-only `/conformance/streams` and `/loadtest/streams`; set by `make conformance` and `examples/deploy/` |
| `DEBUG_ADDR` | off | loopback address for expvar and pprof (`/debug/vars`, `/debug/pprof/`); a non-loopback address is refused |

### Structure

```
examples/chat/
├── main.go            # wiring: store, hub, gin routes, graceful shutdown (Hub.Close with 30s)
├── llm/
│   ├── provider.go    # type Provider interface { Stream(ctx, prompt string) (<-chan Chunk, error) }
│   ├── fake.go        # deterministic: echoes a canned paragraph word by word with a configurable delay; supports fail/hang markers in the prompt for tests ("!fail", "!hang")
│   └── openai.go      # plain net/http SSE client for chat.completions stream=true; no SDK
├── handlers.go        # /chat, /conformance/streams
├── web/               # index.html, app.js, style.css; vendor/rebound/ = the built client, copied by
│                      # `make client-build` and committed so go:embed and Docker builds need no Node
└── README.md          # how to run, how to point it at a real model
```

Event payloads produced by `/chat`: a first `meta` event `{"instance","provider","model"}`;
`data` events `{"text":"…"}`; a final `data` event `{"usage":{…}}` when the provider reports it;
`done {}` from the Hub. When the model fails (bad key, unknown model, provider unreachable), an
`llm_error` event `{"status","message","hint"}` precedes the terminal `error{producer_failed}`, so
the UI can say why instead of stopping silently. `/healthz` also names the provider and model.

`examples/playground/` runs this example on a real model (OpenAI, Groq, OpenRouter, Gemini, Ollama) with two
instances behind nginx; see `examples/playground/README.md`.

### UI behaviour (vanilla JS, no framework)

- Send message → `POST /chat` → store `stream_id` in `sessionStorage` under the conversation →
  `subscribe(url)`.
- Append text as events arrive; show the instance name and the model from the custom `meta` event
  the server sends first, so switching instances and the real model are visible in the demo; show
  an `llm_error` as a readable error.
- A "Long answer" button asks for about 800 words, so a fast real model streams long enough to
  reload or drop the connection.
- `?stream=<id>` follows an existing stream from its first event (the "second tab" test).
- On page load, if `sessionStorage` has an unfinished stream, resume it with the saved
  `lastEventId` — this is the "reload mid-answer" demo.
- Cancel button → `POST /streams/:id/cancel`; render the `error{cancelled}` terminal as "Stopped".
- A "simulate network drop" toggle calls `sub.dropConnection()` (test hook in the client) every
  N events so the demo works without DevTools.

## 2. `clients/typescript` — `@bailalayoub/rebound-client`

Fetch-based because `EventSource` cannot set headers reliably everywhere and cannot do POST;
also gives full control of reconnection.

### API

```ts
import { subscribe } from "@bailalayoub/rebound-client";

const sub = subscribe("/streams/abc123", {
  lastEventId: 0,                      // resume cursor; default 0
  headers: { Authorization: "Bearer …" },
  fetch: globalThis.fetch,             // injectable (tests, Node < 18 polyfills)
  retry: { baseMs: 500, maxMs: 10_000, factor: 2, jitter: 0.2, maxAttempts: Infinity },
  onEvent(e: ReboundEvent) {},          // { id: number, kind: string, data: string }
  onDone() {},                         // terminal "done"
  onError(err: ReboundError) {},        // terminal "error" (code, message) OR fatal transport error
  onReconnect(info: { attempt: number; lastEventId: number; delayMs: number }) {},
  onOpen(info: { status: number }) {},
});

sub.lastEventId;   // number, always current
sub.state;         // "connecting" | "open" | "reconnecting" | "closed"
sub.close();       // stop for good (does NOT cancel the server-side stream)
sub.dropConnection(); // test hook: abort the current fetch to force a reconnect
```

### Rules

- Parse SSE per the WHATWG spec: `id:`, `event:`, `data:` (multi-line join with `\n`), `retry:`,
  comments (`:`), blank line dispatches. Ignore unknown fields. Handle `\r\n` and `\n`.
- Update `lastEventId` **only** when an event with an `id:` field is dispatched.
- Honour `retry:` as the new `baseMs`.
- Reconnect (with `Last-Event-ID` header) on: network error, response body ending **without** a
  terminal event, HTTP 5xx, HTTP 429 (respect `Retry-After` if present).
- Stop permanently on: terminal event (`done`/`error`), HTTP 204, 400, 401, 403, 404, 410. A 410
  is reported as `onError({code:"gap", fatal:true})`: the server no longer holds the events after
  the cursor, and restarting from 0 cannot help (ADR 0004).
- Exponential backoff with jitter; `maxAttempts` reached ⇒ `onError({code:"max_attempts"})`.
- Never deliver an event twice: if the server replays an id ≤ `lastEventId` (should not happen), drop it and report via `onError` with `code: "protocol_violation"`, non-fatal.
- Zero dependencies; works in browsers and Node 18+.

### Tests (vitest)

- Parser: fixture strings → events (multi-line data, CRLF, comments, missing `event:`).
- State machine against an in-process `node:http` server that: streams N events then closes
  without terminal (expect reconnect with correct header); returns 204 (expect stop); returns 410
  (expect a fatal `gap` error and no further request); sends a terminal `error` (expect `onError`
  and no reconnect); honours `retry:`.
- Fake timers for backoff timing.

### Build and publish

`npm run build` → `dist/index.js` (ESM) + `dist/index.d.ts`. `package.json` `files: ["dist"]`,
plus `README.md` and `LICENSE`; a `prepack` script builds, so `npm pack` / `npm publish` from a
clean checkout always ship `dist/` (CI checks it). Publishing to npm is a manual step done by a
maintainer; CI only runs a dry-run (`npm pack --dry-run`).

## 3. Multi-instance deployment for the demo (`examples/deploy/`)

```
docker-compose.yml: redis:7, app-1, app-2 (same image, INSTANCE_NAME differs), nginx round-robin
                    without ip_hash on :8080 → app-1/app-2
```

Expected demo: send a message, drop the connection several times; the UI shows the instance name
alternating between `app-1` and `app-2` while the text continues seamlessly.

## 4. Demo script (to record a GIF)

1. `make demo-redis` (compose up, UI at http://localhost:8080).
2. Type a long prompt; the fake LLM streams ~60 s of text.
3. Toggle "simulate drops"; watch the reconnect counter and the instance badge alternate.
4. Reload the page: the answer resumes where it was.
5. Click Cancel: "Stopped" appears within a second.
6. In a terminal: `rebound-conformance --base-url http://localhost:8080 --runs 200` → all green.

Keep the GIF under 20 s: steps 3 and 4 are the money shots.
