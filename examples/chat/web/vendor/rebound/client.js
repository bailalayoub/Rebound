import { SSEParser } from "./parser.js";
const DEFAULT_RETRY = { baseMs: 500, maxMs: 10000, factor: 2, jitter: 0.2, maxAttempts: Infinity };
/** Statuses after which reconnecting cannot help. 410: the events after the cursor are gone. */
const FATAL_STATUSES = new Set([400, 401, 403, 404, 410]);
/**
 * Subscribes to a Rebound stream URL (e.g. "/streams/abc"). The client reconnects on network
 * errors, on a response that ends without a terminal event, on HTTP 5xx and 429, sending
 * `Last-Event-ID` so it receives exactly the events it missed. It stops on the terminal event and
 * on HTTP 204/400/401/403/404/410; a 410 (the events after the cursor are gone) is reported as a
 * fatal `gap` error.
 */
export function subscribe(url, options = {}) {
    const doFetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    const retry = { ...DEFAULT_RETRY, ...options.retry };
    const random = options.random ?? Math.random;
    let lastEventId = options.lastEventId ?? 0;
    let state = "connecting";
    let isClosed = false;
    let current;
    let wake;
    let resolveClosed;
    const closed = new Promise((resolve) => {
        resolveClosed = resolve;
    });
    const report = (error) => options.onError?.(error);
    const finish = () => {
        if (state === "closed") {
            return;
        }
        isClosed = true;
        state = "closed";
        current?.abort();
        wake?.();
        resolveClosed();
    };
    /** Handles one parsed frame; returns true when it was the terminal event. */
    const handleFrame = (frame) => {
        if (frame.retry !== undefined) {
            retry.baseMs = frame.retry;
        }
        if (!frame.hasData) {
            return false; // comments (pings) and retry-only frames
        }
        let id = lastEventId;
        if (frame.id !== undefined) {
            id = Number(frame.id);
            if (!Number.isSafeInteger(id) || id < 0) {
                report({ code: "protocol_violation", message: `invalid event id ${JSON.stringify(frame.id)}`, fatal: false });
                return false;
            }
            if (id <= lastEventId) {
                report({ code: "protocol_violation", message: `event ${id} replayed after ${lastEventId}`, fatal: false });
                return false;
            }
            lastEventId = id;
        }
        const event = { id, kind: frame.event || "message", data: frame.data };
        options.onEvent?.(event);
        if (event.kind === "done") {
            options.onDone?.();
            return true;
        }
        if (event.kind === "error") {
            const { code, message } = parseErrorPayload(event.data);
            report({ code, message, fatal: true });
            return true;
        }
        return false;
    };
    /** Reads a 200 response to its end. */
    const consume = async (body) => {
        const reader = body.getReader();
        const decoder = new TextDecoder();
        let terminal = false;
        const parser = new SSEParser((frame) => {
            if (!terminal && handleFrame(frame)) {
                terminal = true;
            }
        });
        try {
            while (!terminal) {
                const { done, value } = await reader.read();
                if (done) {
                    break;
                }
                parser.feed(decoder.decode(value, { stream: true }));
            }
        }
        finally {
            reader.releaseLock();
        }
        return terminal ? { kind: "terminal" } : { kind: "retry" };
    };
    /** One connection attempt. */
    const connect = async () => {
        const controller = new AbortController();
        current = controller;
        const headers = { Accept: "text/event-stream", ...options.headers };
        if (lastEventId > 0) {
            headers["Last-Event-ID"] = String(lastEventId);
        }
        const response = await doFetch(url, { headers, signal: controller.signal, cache: "no-store" });
        const status = response.status;
        if (status === 200 && response.body) {
            state = "open";
            options.onOpen?.({ status });
            const outcome = await consume(response.body);
            controller.abort(); // release the connection after the terminal event
            return outcome;
        }
        const text = await response.text().catch(() => "");
        if (status === 204) {
            return { kind: "stop" };
        }
        if (status >= 500 || status === 429) {
            const after = retryAfterMs(response.headers.get("Retry-After"));
            return after === undefined ? { kind: "retry" } : { kind: "retry", delayMs: after };
        }
        const code = errorCodeFromBody(text) ?? (FATAL_STATUSES.has(status) ? `http_${status}` : `unexpected_status_${status}`);
        report({ code, message: `HTTP ${status}`, fatal: true, status });
        return { kind: "stop" };
    };
    const sleep = (ms) => new Promise((resolve) => {
        const timer = setTimeout(() => {
            wake = undefined;
            resolve();
        }, ms);
        wake = () => {
            clearTimeout(timer);
            wake = undefined;
            resolve();
        };
    });
    const backoff = (attempt) => {
        const raw = Math.min(retry.maxMs, retry.baseMs * Math.pow(retry.factor, attempt - 1));
        const spread = raw * retry.jitter * (random() * 2 - 1);
        return Math.max(0, Math.round(raw + spread));
    };
    const run = async () => {
        let attempt = 0;
        while (!isClosed) {
            let outcome;
            try {
                outcome = await connect();
            }
            catch {
                if (isClosed) {
                    break;
                }
                outcome = { kind: "retry" }; // network error, or dropConnection()
            }
            if (isClosed) {
                break;
            }
            if (outcome.kind === "terminal" || outcome.kind === "stop") {
                break;
            }
            if (state === "open") {
                attempt = 0; // the previous connection worked: start the backoff over
            }
            attempt++;
            if (attempt > retry.maxAttempts) {
                report({ code: "max_attempts", message: `gave up after ${retry.maxAttempts} attempts`, fatal: true });
                break;
            }
            const delayMs = outcome.delayMs ?? backoff(attempt);
            state = "reconnecting";
            options.onReconnect?.({ attempt, lastEventId, delayMs });
            await sleep(delayMs);
        }
        finish();
    };
    void run();
    return {
        get lastEventId() {
            return lastEventId;
        },
        get state() {
            return state;
        },
        closed,
        close: finish,
        dropConnection() {
            current?.abort();
        },
    };
}
function parseErrorPayload(data) {
    try {
        const parsed = JSON.parse(data);
        if (parsed && typeof parsed === "object") {
            const { code, message } = parsed;
            return {
                code: typeof code === "string" && code ? code : "error",
                message: typeof message === "string" ? message : "",
            };
        }
    }
    catch {
        // Not JSON: keep the raw text as the message.
    }
    return { code: "error", message: data };
}
function errorCodeFromBody(text) {
    try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === "object" && typeof parsed.error === "string") {
            return parsed.error;
        }
    }
    catch {
        // Not a JSON error body.
    }
    return undefined;
}
/** Parses Retry-After (seconds or an HTTP date) into milliseconds. */
function retryAfterMs(value) {
    if (!value) {
        return undefined;
    }
    if (/^[0-9]+$/.test(value.trim())) {
        return Number(value.trim()) * 1000;
    }
    const date = Date.parse(value);
    return Number.isNaN(date) ? undefined : Math.max(0, date - Date.now());
}
//# sourceMappingURL=client.js.map