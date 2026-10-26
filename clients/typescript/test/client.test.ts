import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it, vi } from "vitest";

import { subscribe, type ReconnectInfo, type ReboundError, type ReboundEvent, type SubscribeOptions } from "../src/client.js";

type Handler = (req: IncomingMessage, res: ServerResponse) => void;

interface TestServer {
  url: string;
  requests: IncomingMessage[];
  close: () => Promise<void>;
}

const servers: TestServer[] = [];

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

/** Serves handlers[i] for the i-th request; the last handler repeats. */
async function serve(...handlers: Handler[]): Promise<TestServer> {
  const requests: IncomingMessage[] = [];
  const server: Server = createServer((req, res) => {
    requests.push(req);
    handlers[Math.min(requests.length, handlers.length) - 1]!(req, res);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  const ts: TestServer = {
    url: `http://127.0.0.1:${port}/streams/s1`,
    requests,
    close: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
  servers.push(ts);
  return ts;
}

function event(id: number, kind: string, data: string): string {
  return `id: ${id}\nevent: ${kind}\n${data.split("\n").map((l) => `data: ${l}`).join("\n")}\n\n`;
}

/** Streams the given events, then ends the response (with or without a terminal event). */
function stream(...chunks: string[]): Handler {
  return (_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.write("retry: 20\n\n");
    for (const c of chunks) {
      res.write(c);
    }
    res.end();
  };
}

function status(code: number, body = "", headers: Record<string, string> = {}): Handler {
  return (_req, res) => {
    res.writeHead(code, { "Content-Type": "application/json", ...headers });
    res.end(body);
  };
}

interface Recorded {
  events: ReboundEvent[];
  errors: ReboundError[];
  reconnects: ReconnectInfo[];
  done: number;
  opens: number;
}

/** Subscribes with fast, jitter-free retries and records every callback. */
function record(url: string, extra: Partial<SubscribeOptions> = {}) {
  const rec: Recorded = { events: [], errors: [], reconnects: [], done: 0, opens: 0 };
  const sub = subscribe(url, {
    retry: { baseMs: 1, maxMs: 5, jitter: 0 },
    onEvent: (e) => rec.events.push(e),
    onError: (e) => rec.errors.push(e),
    onReconnect: (i) => rec.reconnects.push(i),
    onDone: () => rec.done++,
    onOpen: () => rec.opens++,
    ...extra,
  });
  return { sub, rec };
}

describe("subscribe", () => {
  it("resumes with Last-Event-ID when the body ends without a terminal event", async () => {
    const srv = await serve(
      stream(event(1, "data", "Bon"), event(2, "data", "jo")),
      stream(event(3, "data", "ur"), event(4, "done", "{}")),
    );
    const { sub, rec } = record(srv.url, { retry: { baseMs: 1, maxMs: 50, jitter: 0 } });
    await sub.closed;

    expect(rec.events.map((e) => [e.id, e.kind, e.data])).toEqual([
      [1, "data", "Bon"],
      [2, "data", "jo"],
      [3, "data", "ur"],
      [4, "done", "{}"],
    ]);
    expect(rec.done).toBe(1);
    expect(rec.errors).toEqual([]);
    expect(srv.requests[0]!.headers["last-event-id"]).toBeUndefined();
    expect(srv.requests[1]!.headers["last-event-id"]).toBe("2");
    expect(srv.requests[1]!.headers.accept).toBe("text/event-stream");
    expect(rec.reconnects).toEqual([{ attempt: 1, lastEventId: 2, delayMs: 20 }]); // retry: 20 honoured
    expect(sub.state).toBe("closed");
    expect(sub.lastEventId).toBe(4);
  });

  it("honours retry: as the new backoff base", async () => {
    const srv = await serve(
      (_req, res) => {
        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.end("retry: 7\n\n" + event(1, "data", "x"));
      },
      stream(event(2, "done", "{}")),
    );
    const { sub, rec } = record(srv.url);
    await sub.closed;
    expect(rec.reconnects[0]!.delayMs).toBe(5); // min(maxMs 5, retry 7)
  });

  it("starts from the given lastEventId and sends custom headers", async () => {
    const srv = await serve(stream(event(8, "done", "{}")));
    const { sub, rec } = record(srv.url, { lastEventId: 7, headers: { Authorization: "Bearer t" } });
    await sub.closed;
    expect(srv.requests[0]!.headers["last-event-id"]).toBe("7");
    expect(srv.requests[0]!.headers.authorization).toBe("Bearer t");
    expect(rec.events).toEqual([{ id: 8, kind: "done", data: "{}" }]);
  });

  it("stops on 204 without reconnecting", async () => {
    const srv = await serve(status(204));
    const { sub, rec } = record(srv.url, { lastEventId: 4 });
    await sub.closed;
    expect(srv.requests).toHaveLength(1);
    expect(rec.errors).toEqual([]);
    expect(rec.reconnects).toEqual([]);
  });

  it.each([
    [400, '{"error":"invalid_last_event_id"}', "invalid_last_event_id"],
    [401, '{"error":"unauthorized"}', "unauthorized"],
    [403, '{"error":"forbidden"}', "forbidden"],
    [404, '{"error":"stream_not_found"}', "stream_not_found"],
    [404, "not json", "http_404"],
    [418, "", "unexpected_status_418"],
  ])("stops on HTTP %i with a fatal error", async (code, body, want) => {
    const srv = await serve(status(code, body));
    const { sub, rec } = record(srv.url);
    await sub.closed;
    expect(srv.requests).toHaveLength(1);
    expect(rec.errors).toEqual([{ code: want, message: `HTTP ${code}`, fatal: true, status: code }]);
  });

  // ADR 0004: 410 means the server no longer holds the events after the cursor, and a
  // restart from 0 cannot bring them back. The client stops with a fatal "gap", once.
  it("stops on HTTP 410 with a fatal gap error and never restarts from 0", async () => {
    const srv = await serve(status(410, '{"error":"gap","first_seq":17}'), stream(event(1, "data", "a"), event(2, "done", "{}")));
    const { sub, rec } = record(srv.url, { lastEventId: 5 });
    await sub.closed;
    expect(srv.requests).toHaveLength(1);
    expect(srv.requests[0]!.headers["last-event-id"]).toBe("5");
    expect(rec.reconnects).toEqual([]);
    expect(rec.events).toEqual([]);
    expect(rec.errors).toEqual([{ code: "gap", message: "HTTP 410", fatal: true, status: 410 }]);
    expect(sub.lastEventId).toBe(5);
  });

  it("reports a terminal error event and does not reconnect", async () => {
    const srv = await serve(stream(event(1, "data", "a"), event(2, "error", '{"code":"cancelled","message":"cancelled by client"}')));
    const { sub, rec } = record(srv.url);
    await sub.closed;
    expect(rec.errors).toEqual([{ code: "cancelled", message: "cancelled by client", fatal: true }]);
    expect(rec.done).toBe(0);
    expect(srv.requests).toHaveLength(1);
  });

  it("reconnects on 5xx and on 429, honouring Retry-After", async () => {
    const srv = await serve(
      status(503),
      status(429, "", { "Retry-After": "0" }),
      stream(event(1, "done", "{}")),
    );
    const { sub, rec } = record(srv.url);
    await sub.closed;
    expect(srv.requests).toHaveLength(3);
    expect(rec.reconnects.map((r) => [r.attempt, r.delayMs])).toEqual([
      [1, 1],
      [2, 0],
    ]);
    expect(rec.done).toBe(1);
  });

  it("drops replayed ids with a non-fatal protocol_violation", async () => {
    const srv = await serve(stream(event(1, "data", "a"), event(1, "data", "dup"), event(2, "data", "b"), event(3, "done", "{}")));
    const { sub, rec } = record(srv.url);
    await sub.closed;
    expect(rec.events.map((e) => e.data)).toEqual(["a", "b", "{}"]);
    expect(rec.errors).toEqual([{ code: "protocol_violation", message: "event 1 replayed after 1", fatal: false }]);
  });

  it("gives up after maxAttempts consecutive failures", async () => {
    const srv = await serve(status(500));
    const { sub, rec } = record(srv.url, { retry: { baseMs: 1, maxMs: 1, jitter: 0, maxAttempts: 3 } });
    await sub.closed;
    expect(srv.requests).toHaveLength(4);
    expect(rec.errors).toEqual([{ code: "max_attempts", message: "gave up after 3 attempts", fatal: true }]);
  });

  it("reconnects after dropConnection and closes for good on close()", async () => {
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    const srv = await serve(
      (_req, res) => {
        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.write(event(1, "data", "a"));
        void held.then(() => res.end());
      },
      (_req, res) => {
        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.write(event(2, "data", "b")); // stays open until the client closes
      },
    );
    const { sub, rec } = record(srv.url);
    await vi.waitFor(() => expect(rec.events).toHaveLength(1));
    expect(sub.state).toBe("open");
    sub.dropConnection();
    await vi.waitFor(() => expect(rec.events).toHaveLength(2));
    expect(srv.requests[1]!.headers["last-event-id"]).toBe("1");
    sub.close();
    await sub.closed;
    expect(sub.state).toBe("closed");
    release();
    expect(srv.requests).toHaveLength(2);
  });

  it("treats a non-JSON terminal error payload as code 'error'", async () => {
    const srv = await serve(stream(event(1, "error", "boom")));
    const { sub, rec } = record(srv.url);
    await sub.closed;
    expect(rec.errors).toEqual([{ code: "error", message: "boom", fatal: true }]);
  });
});

describe("backoff", () => {
  it("grows exponentially with jitter, capped at maxMs, and resets after a successful open", async () => {
    vi.useFakeTimers();
    const delays: number[] = [];
    let calls = 0;
    const fetchStub = (async () => {
      calls++;
      throw new TypeError("network down");
    }) as unknown as typeof fetch;
    const sub = subscribe("http://example.invalid/streams/x", {
      fetch: fetchStub,
      random: () => 1, // maximum positive jitter
      retry: { baseMs: 100, maxMs: 1000, factor: 2, jitter: 0.2, maxAttempts: 6 },
      onReconnect: (i) => delays.push(i.delayMs),
    });
    await vi.runAllTimersAsync();
    await sub.closed;
    expect(delays).toEqual([120, 240, 480, 960, 1200, 1200]);
    expect(calls).toBe(7);
  });

  it("closing while waiting cancels the pending reconnect", async () => {
    vi.useFakeTimers();
    let calls = 0;
    const sub = subscribe("http://example.invalid/streams/x", {
      fetch: (async () => {
        calls++;
        throw new TypeError("network down");
      }) as unknown as typeof fetch,
      retry: { baseMs: 10_000, jitter: 0 },
    });
    await vi.advanceTimersByTimeAsync(1);
    expect(sub.state).toBe("reconnecting");
    sub.close();
    await sub.closed;
    await vi.advanceTimersByTimeAsync(60_000);
    expect(calls).toBe(1);
  });
});
