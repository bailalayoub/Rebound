# @bailalayoub/rebound-client

Resumable Server-Sent Events client for [Rebound](https://github.com/bailalayoub/rebound) streams.
`fetch`-based (custom headers, any method of starting the stream), zero dependencies, browsers and
Node 18+.

```ts
import { subscribe } from "@bailalayoub/rebound-client";

const sub = subscribe("/streams/abc123", {
  lastEventId: 0,                              // resume cursor, e.g. restored after a reload
  headers: { Authorization: "Bearer …" },
  onEvent: (e) => render(e.id, e.kind, e.data),
  onDone: () => console.log("complete"),
  onError: (err) => console.warn(err.code, err.message, err.fatal),
  onReconnect: ({ attempt, lastEventId, delayMs }) => console.log("reconnecting", attempt),
});

sub.lastEventId; // always current: persist it to resume after a page reload
sub.close();     // stop for good (does not cancel the server-side stream)
```

## Behaviour

- Reconnects with `Last-Event-ID` on network errors, on a response that ends without a terminal
  event, on HTTP 5xx and 429 (`Retry-After` honoured), with exponential backoff and jitter
  (`retry: { baseMs: 500, maxMs: 10000, factor: 2, jitter: 0.2, maxAttempts: Infinity }`).
- Honours the server's `retry:` field as the new base delay.
- Stops on the terminal event (`done` → `onDone`, `error` → fatal `onError` with the server's code),
  on HTTP 204, 400, 401, 403, 404 and 410. A 410 means the server no longer holds the events after
  the cursor; it is reported as a fatal `onError` with code `gap` (a restart from 0 could not bring
  them back).
- Never delivers an event twice: an id at or below `lastEventId` is dropped and reported as a
  non-fatal `protocol_violation`.
- `dropConnection()` aborts the current connection to exercise reconnection in demos and tests.

## Development

```sh
npm ci && npm test    # type-check + vitest (Node 20.19+ or 22.12+ for the dev tools)
npm run build         # dist/index.js (ESM) + .d.ts; npm pack and npm publish build first (prepack)
```
