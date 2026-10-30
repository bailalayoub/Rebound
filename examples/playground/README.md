# Playground: the chat UI on a real LLM

Two instances of `examples/chat` behind nginx round-robin, sharing one Redis, answering with the
model you choose in `examples/playground/.env`: OpenAI, Groq, OpenRouter, Google Gemini, Ollama on your PC,
or the built-in fake model. Use it to see with a real model what Rebound promises: reload the page,
drop the connection, lose an instance, and the answer still arrives complete, once, in order.

It is a local demo: no authentication, no rate limit, published on `127.0.0.1` only. Anyone who
could reach it could spend your API credits.

## Start (Windows PowerShell)

Prerequisite: Docker Desktop running. All commands run from the repository root.

1. Create your settings file (gitignored, never committed):

   ```powershell
   Copy-Item examples\playground\.env.example examples\playground\.env
   notepad examples\playground\.env
   ```

2. In `examples\playground\.env`, comment out the fake block (`LLM_PROVIDER=fake`), uncomment **one** provider
   block and paste your key after `LLM_API_KEY=`. Leave `DEMO_PORT=8090` unless that port is taken.

3. Start (the first build takes a minute or two):

   ```powershell
   docker compose -f examples/playground/docker-compose.yml --env-file examples/playground/.env up --build
   ```

   With `LLM_PROVIDER=openai` and an empty `LLM_API_KEY` or `LLM_MODEL`, both app instances stop at
   once with a message such as
   `chat: LLM_PROVIDER=openai needs LLM_API_KEY (any value for a local Ollama) and LLM_MODEL`.

4. Open <http://localhost:8090>. The `model:` badge shows the provider and model that answer, for
   example `api.groq.com · openai/gpt-oss-20b`.

5. Stop: `Ctrl+C` in that window, then remove the containers:

   ```powershell
   docker compose -f examples/playground/docker-compose.yml --env-file examples/playground/.env down
   ```

To run it in the background instead, add `-d` to `up`, and follow the logs with
`docker compose -f examples/playground/docker-compose.yml --env-file examples/playground/.env logs -f app-1 app-2`.
The logs never contain the key. `docker compose config` does print it: do not share that output.

After editing `examples\playground\.env` (another provider, another model), run the `up` command again: the
app containers are recreated with the new settings.

On Linux or macOS: `make playground` and `make playground-down`.

## Manual tests

Use **Long answer** for most of them: it asks for about 800 words, which gives you time to act.
The badges above the answer show the model, the instance that generates (`instance: app-1` or
`app-2`), the connection state, the reconnection count and the last event received.

| # | Test | What to do | What to expect |
|---|---|---|---|
| 1 | Normal answer | Type a question, **Send**. | Text streams in; `model:` names your provider and model; the answer ends with `Done.` and a token count (when the provider reports usage). |
| 2 | Reload mid-answer | **Long answer**, then `F5` while it streams. | The page comes back with the text so far, says `Resuming after reload…`, and continues from the last event: no repeated or missing words. The reconnection counter restarts at 0 (it is per page). |
| 3 | Simulated drops | Tick **Simulate network drops**, then **Long answer**. | The connection is cut every 5 events; `reconnections` keeps climbing; the text is identical to an answer without drops. |
| 4 | Offline 5 s | **Long answer**; `F12` → Network → throttling **Offline**; wait 5 s; back to **No throttling**. | State goes `reconnecting`; retries back off (about 1 s, 2 s, 4 s…). Back online, the next retry resumes: everything generated meanwhile appears at once, then the text continues live. |
| 5 | Cancel | **Long answer**, then **Cancel**. | `Stopped.` within about a second, even when the other instance generates (it notices at its next heartbeat, every second); no text after it. |
| 6 | Second tab on the same answer | During an answer, click **Follow this answer in another tab**. | The new tab replays the answer from the first word, then follows live; both tabs show the same text and finish together. **Cancel** in one tab stops both. |
| 7a | Stop app-1 mid-answer | **Long answer**, then in a second PowerShell: `docker compose -f examples/playground/docker-compose.yml --env-file examples/playground/.env stop app-1` | If `instance: app-2` generates: the connection served by app-1 closes, the client reconnects through nginx to app-2 and the text simply continues. If `instance: app-1` generates: app-1 shuts down gracefully, finishing the running generation first (up to 30 s; beyond that the answer ends with `Error: server_shutdown`) while the client keeps reading through either instance. Restart it with `... start app-1`. |
| 7b | Crash app-1 mid-answer | As 7a, with `kill app-1` instead of `stop app-1`, while `instance: app-1` generates. | The generation dies with the process. About 15 s later (`StaleAfter`) the reader, now on app-2, ends the stream: `Error: producer_lost`. The text received so far stays. |
| 8 | Wrong key | Put a wrong `LLM_API_KEY` in `.env`, run `up` again, **Send**. | A red message such as `Model error (HTTP 401): authentication failed (the provider's message is not shown: it may quote the key) — the provider rejected the API key: check LLM_API_KEY`. Gemini answers HTTP 400 with its own message instead. Neither the key nor a fragment of it is ever displayed. |
| 9 | Wrong model | Put a model name that does not exist in `LLM_MODEL`, run `up` again, **Send**. | A red `Model error (HTTP 400 or 404): …` with the provider's message and a hint to check `LLM_MODEL`. |

The fake model works for every test except 8 and 9; it ignores the question and always streams the
same text, about a minute long.
