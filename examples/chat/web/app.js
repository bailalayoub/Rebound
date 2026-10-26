// Example UI: POST /chat starts a generation, @bailalayoub/rebound-client follows it. The stream id,
// the cursor and the text so far live in sessionStorage, so reloading the page resumes the answer.
// "?stream=<id>" follows an existing stream from its first event (a second tab on the same answer).
import { subscribe } from "./vendor/rebound/index.js";

const KEY = "rebound-chat";
// A prompt that makes a fast real model write for long enough to reload and drop connections.
const LONG_PROMPT =
  "Write a detailed answer of about 800 words, in several paragraphs, explaining how a web page " +
  "travels from a server to a browser: DNS resolution, TCP, TLS, HTTP, caching and rendering. " +
  "Do not use bullet points.";

const $ = (id) => document.getElementById(id);
const ui = {
  form: $("form"), message: $("message"), send: $("send"), long: $("long"), cancel: $("cancel"),
  drops: $("drops"), text: $("text"), status: $("status"), state: $("state"), model: $("model"),
  instance: $("instance"), reconnects: $("reconnects"), cursor: $("cursor"), share: $("share"),
  shareLink: $("share-link"),
};

let sub = null;
let saved = null; // { streamId, lastEventId, text, finished, final, model, instance, llmError }
let reconnections = 0;
let sinceDrop = 0;

function persist() {
  sessionStorage.setItem(KEY, JSON.stringify(saved));
}

function setStatus(message, isError = false) {
  ui.status.textContent = message;
  ui.status.classList.toggle("error", isError);
}

function setBusy(busy) {
  ui.send.disabled = busy;
  ui.long.disabled = busy;
  ui.cancel.disabled = !busy;
}

function render() {
  ui.text.textContent = saved?.text ?? "";
  ui.cursor.textContent = `last event: ${saved?.lastEventId ?? 0}`;
  ui.reconnects.textContent = `reconnections: ${reconnections}`;
  if (saved?.model) ui.model.textContent = `model: ${saved.model}`;
  if (saved?.instance) ui.instance.textContent = `instance: ${saved.instance}`;
  if (saved?.streamId) {
    ui.share.hidden = false;
    ui.shareLink.href = `?stream=${encodeURIComponent(saved.streamId)}`;
  }
}

// describeModel turns {provider, model} into "provider · model".
function describeModel({ provider, model }) {
  return [provider, model].filter(Boolean).join(" · ");
}

// describeLLMError explains why the model did not answer, from the server's "llm_error" event.
function describeLLMError({ status, message, hint }) {
  return `Model error${status ? ` (HTTP ${status})` : ""}: ${message}${hint ? ` — ${hint}` : ""}`;
}

function follow() {
  setBusy(true);
  sinceDrop = 0;
  sub = subscribe(`/streams/${encodeURIComponent(saved.streamId)}`, {
    lastEventId: saved.lastEventId,
    onOpen() {
      ui.state.textContent = "open";
      ui.state.classList.add("open");
    },
    onEvent(e) {
      saved.lastEventId = e.id;
      if (e.kind === "meta") {
        const meta = JSON.parse(e.data);
        saved.instance = meta.instance;
        if (meta.model) saved.model = describeModel(meta);
      } else if (e.kind === "llm_error") {
        saved.llmError = JSON.parse(e.data);
        setStatus(describeLLMError(saved.llmError), true);
      } else if (e.kind === "data") {
        const payload = JSON.parse(e.data);
        if (payload.text !== undefined) saved.text += payload.text;
        if (payload.usage) setStatus(`${payload.usage.completion_tokens} tokens`);
      }
      persist();
      render();
      if (ui.drops.checked && e.kind !== "done" && e.kind !== "error" && ++sinceDrop >= 5) {
        sinceDrop = 0;
        sub.dropConnection(); // the demo's "network drop": the client resumes from lastEventId
      }
    },
    onReconnect() {
      reconnections++;
      ui.state.textContent = "reconnecting";
      ui.state.classList.remove("open");
      render();
    },
    onDone() {
      finish("Done.");
    },
    onError(err) {
      if (!err.fatal) return;
      if (err.code === "cancelled") {
        finish("Stopped.");
      } else if (saved.llmError) {
        finish(describeLLMError(saved.llmError), true);
      } else {
        finish(`Error: ${err.code}${err.message ? ` (${err.message})` : ""}`, true);
      }
    },
  });
}

function finish(message, isError = false) {
  saved.finished = true;
  saved.final = { message, isError };
  persist();
  setStatus(message, isError);
  setBusy(false);
  ui.state.textContent = "closed";
  ui.state.classList.remove("open");
}

async function start(message) {
  sub?.close();
  setStatus("");
  reconnections = 0;
  setBusy(true);
  let response;
  try {
    response = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message }),
    });
  } catch {
    setBusy(false);
    setStatus("Could not reach the server.", true);
    return;
  }
  if (response.status !== 201) {
    setBusy(false);
    setStatus(`Could not start: HTTP ${response.status}`, true);
    return;
  }
  const { stream_id: streamId } = await response.json();
  saved = { streamId, lastEventId: 0, text: "", finished: false, model: saved?.model ?? "", instance: "" };
  persist();
  render();
  follow();
}

ui.form.addEventListener("submit", (event) => {
  event.preventDefault();
  void start(ui.message.value);
});

ui.long.addEventListener("click", () => {
  ui.message.value = LONG_PROMPT;
  void start(LONG_PROMPT);
});

ui.cancel.addEventListener("click", async () => {
  if (!saved) return;
  ui.cancel.disabled = true;
  await fetch(`/streams/${encodeURIComponent(saved.streamId)}/cancel`, { method: "POST" });
});

// Which model this deployment uses, before anything is sent.
fetch("/healthz")
  .then((r) => r.json())
  .then((h) => {
    if (!saved?.model && h.model) ui.model.textContent = `model: ${describeModel(h)}`;
  })
  .catch(() => {});

// Resume an unfinished answer after a reload, or follow the stream named in the URL.
try {
  saved = JSON.parse(sessionStorage.getItem(KEY) ?? "null");
} catch {
  saved = null;
}
const shared = new URLSearchParams(location.search).get("stream");
if (shared && saved?.streamId !== shared) {
  saved = { streamId: shared, lastEventId: 0, text: "", finished: false };
  persist();
}
if (saved) {
  render();
  if (saved.finished) {
    setStatus(saved.final?.message ?? "Done.", saved.final?.isError ?? false);
  } else {
    setStatus(shared ? "Following the shared answer…" : "Resuming after reload…");
    follow();
  }
}
