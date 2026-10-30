# examples/chat

A resumable chat on Rebound: Gin routes, a fake or OpenAI-compatible model, and a small UI that
survives page reloads and dropped connections. It is a demo and the conformance target, not a
product: **it has no authentication and no rate limit.** Keep it on localhost, and never expose it
with `LLM_PROVIDER=openai`: anyone could spend your API credits.

```sh
go run ./examples/chat                                 # memory store, fake model, http://localhost:8080
REBOUND_STORE=redis REDIS_ADDR=localhost:6379 go run ./examples/chat
LLM_PROVIDER=openai LLM_API_KEY=sk-… LLM_MODEL=gpt-6-luna go run ./examples/chat
```

| Variable | Default | |
|---|---|---|
| `PORT` | `8080` | |
| `REBOUND_STORE` | `memory` | `memory` or `redis` |
| `REDIS_ADDR` | `localhost:6379` | |
| `LLM_PROVIDER` | `fake` | `fake` or `openai` (any OpenAI-compatible chat completions API) |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | with `openai`: API base including its version; `/chat/completions` is appended |
| `LLM_API_KEY`, `LLM_MODEL` | — | required with `openai` (any key value for a local Ollama) |
| `LLM_STREAM_USAGE` | `1` | `0` omits `stream_options.include_usage` |
| `INSTANCE_NAME` | hostname | shown in the UI and `/healthz`, to make multi-instance demos visible |
| `FAKE_DELAY_MS`, `FAKE_REPEAT` | `120`, `2` | pace and length of the fake answer |
| `CONFORMANCE` | off | `1` mounts `POST /conformance/streams`, the test-only producer of `rebound-conformance`, and `POST /loadtest/streams`, the timestamped producer of `rebound-load` |
| `DEBUG_ADDR` | off | a loopback address (e.g. `127.0.0.1:6060`) serving expvar (`/debug/vars`: goroutines, RSS, open fds, memstats) and pprof; refused if not loopback |

Two instances behind nginx round-robin on one Redis: `docker compose -f examples/deploy/docker-compose.yml up --build`
(listens on 127.0.0.1:8080). The same on a real model, with ready-made settings for OpenAI, Groq,
OpenRouter, Gemini and Ollama and a list of manual tests: [`examples/playground/`](../playground).
