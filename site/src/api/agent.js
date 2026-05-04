// SSE-over-fetch helper for the agent chat. Browser EventSource only
// supports GET, but our endpoint takes a JSON body with conversation
// history, so we POST and parse the body stream manually.
//
// Calls onEvent({event, data}) for each parsed SSE record. Returns a
// promise that resolves when the stream ends and rejects on transport
// errors. Pass an AbortSignal to cancel mid-stream (caller-controlled
// cleanup when the component unmounts).

import { getAdminKey, setAdminKey } from "./client.js";

async function postWithAdminKey(body, signal) {
  const send = (key) =>
    fetch("/api/agent/chat", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "text/event-stream",
        ...(key ? { "X-PSAT-Admin-Key": key } : {}),
      },
      body: JSON.stringify(body),
      signal,
    });

  let res = await send(getAdminKey());
  // Mirror api/client.js: on 401, prompt for the admin key and retry
  // once. The streaming endpoint runs auth before the response generator
  // starts, so 401 here is identical to a plain JSON 401 — safe to retry
  // the whole POST.
  if (res.status === 401) {
    const entered = window.prompt(
      "Admin key required for the agent chat.\nPaste your PSAT admin key:",
      getAdminKey(),
    );
    if (!entered) {
      throw new Error("agent chat failed: 401 (admin key required)");
    }
    setAdminKey(entered);
    res = await send(entered);
  }
  return res;
}

export async function streamAgentChat(body, onEvent, { signal } = {}) {
  const res = await postWithAdminKey(body, signal);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`agent chat failed: ${res.status} ${text}`);
  }
  if (!res.body) throw new Error("no response body");

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  // SSE record framing: events are separated by "\n\n". Within a record,
  // each line is "field: value". We only care about `event:` and `data:`.
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let sep;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const raw = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      const evt = parseSSERecord(raw);
      if (evt) onEvent(evt);
    }
  }
}

function parseSSERecord(raw) {
  if (!raw.trim()) return null;
  let event = "message";
  const dataLines = [];
  for (const line of raw.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
  }
  if (!dataLines.length) return null;
  let data = null;
  try { data = JSON.parse(dataLines.join("\n")); }
  catch { data = dataLines.join("\n"); }
  return { event, data };
}
