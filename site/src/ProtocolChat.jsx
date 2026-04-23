import React, { useCallback, useEffect, useRef, useState } from "react";

// Prompt suggestions shown when the chat is empty. Picked to highlight
// the kind of multi-source reasoning the backend actually supports —
// "what happens if X compromised" type questions that cross contract
// ownership, timelocks, and audit coverage.
const PROMPT_IDEAS = [
  "What is the worst thing that can happen if one EOA is compromised?",
  "Which contracts hold the most TVL and who can upgrade them?",
  "Are there unverified contracts I should worry about?",
  "Summarize the audit coverage — what's audited vs unaudited?",
];

// Build a short human-readable argument hint for a tool call. The
// backend sends the raw args dict; we surface the most informative
// field so the progress log reads like "list_contracts(company='ether
// fi')" rather than the full JSON.
function summarizeToolArgs(name, args) {
  if (!args || typeof args !== "object") return "";
  const keys = ["address", "company", "query", "auditor", "path_substring", "direction"];
  for (const k of keys) {
    if (args[k]) return `${k}=${String(args[k]).slice(0, 40)}`;
  }
  return Object.keys(args).slice(0, 2).join(", ");
}

// Parse one "data: {...}\n\n" chunk into a JSON event. Returns null if
// the chunk isn't a complete SSE event — caller keeps buffering.
function parseSseChunk(chunk) {
  const trimmed = chunk.trim();
  if (!trimmed.startsWith("data:")) return null;
  try {
    return JSON.parse(trimmed.slice(5).trim());
  } catch {
    return null;
  }
}

export default function ProtocolChat({ companyName }) {
  // Each message is either:
  //   {role: "user", content: "..."}
  //   {role: "assistant", content: "...", events: [{kind, name, summary}, ...]}
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  // The assistant turn in flight — rendered live as events arrive, then
  // committed to messages[] when the final answer event lands.
  const [liveEvents, setLiveEvents] = useState([]);
  const abortRef = useRef(null);
  const listRef = useRef(null);

  useEffect(() => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, busy, liveEvents]);

  const send = useCallback(
    async (questionText) => {
      const q = (questionText || input).trim();
      if (!q || busy) return;
      const next = [...messages, { role: "user", content: q }];
      setMessages(next);
      setInput("");
      setBusy(true);
      setError(null);
      setLiveEvents([]);

      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const resp = await fetch(`/api/company/${encodeURIComponent(companyName)}/ask`, {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
          body: JSON.stringify({
            question: q,
            history: next.slice(0, -1).map((m) => ({ role: m.role, content: m.content })),
          }),
          signal: controller.signal,
        });
        if (!resp.ok || !resp.body) {
          throw new Error(`HTTP ${resp.status}`);
        }

        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let finalAnswer = "";
        const collected = [];

        // SSE frames are separated by a blank line. We buffer partial
        // chunks across reads so a frame that arrives split across two
        // TCP packets still parses cleanly.
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const frames = buffer.split("\n\n");
          buffer = frames.pop() ?? ""; // last chunk may be incomplete

          for (const frame of frames) {
            const evt = parseSseChunk(frame);
            if (!evt) continue;
            if (evt.event === "tool_call") {
              const entry = {
                kind: "tool_call",
                name: evt.name,
                args: summarizeToolArgs(evt.name, evt.args),
              };
              collected.push(entry);
              setLiveEvents([...collected]);
            } else if (evt.event === "tool_result") {
              // Attach the result summary to the matching in-flight tool_call.
              for (let i = collected.length - 1; i >= 0; i--) {
                if (collected[i].kind === "tool_call" && collected[i].name === evt.name && !collected[i].summary) {
                  collected[i].summary = evt.summary;
                  break;
                }
              }
              setLiveEvents([...collected]);
            } else if (evt.event === "iteration") {
              // Silent bookkeeping — rendered implicitly by the tool_call
              // entries that follow. Kept in the protocol in case a
              // future UI wants to show "round 1 of 3" pacing.
            } else if (evt.event === "answer") {
              finalAnswer = evt.text || "";
            } else if (evt.event === "error") {
              throw new Error(evt.message || "assistant error");
            }
          }
        }

        setMessages([
          ...next,
          {
            role: "assistant",
            content: finalAnswer || "(no response)",
            events: collected,
          },
        ]);
      } catch (e) {
        if (e?.name === "AbortError") {
          setError("Cancelled.");
        } else {
          setError(e?.message || String(e));
        }
      } finally {
        abortRef.current = null;
        setBusy(false);
        setLiveEvents([]);
      }
    },
    [busy, companyName, input, messages],
  );

  const cancel = () => {
    if (abortRef.current) abortRef.current.abort();
  };

  const onKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  return (
    <div className="protocol-chat">
      <div className="protocol-chat-header">
        <div className="protocol-chat-title">
          <span className="protocol-chat-avatar">◆</span>
          <span>Ask about {companyName}</span>
        </div>
        {messages.length > 0 && (
          <button
            type="button"
            className="protocol-chat-clear"
            onClick={() => {
              setMessages([]);
              setError(null);
            }}
            title="Clear conversation"
          >
            Clear
          </button>
        )}
      </div>

      <div className="protocol-chat-body" ref={listRef}>
        {messages.length === 0 && (
          <div className="protocol-chat-empty">
            <p className="protocol-chat-empty-hint">
              Ask the assistant anything about this protocol's contracts,
              ownership, audit coverage, or risks.
            </p>
            <div className="protocol-chat-suggestions">
              {PROMPT_IDEAS.map((p) => (
                <button
                  key={p}
                  type="button"
                  className="protocol-chat-suggestion"
                  onClick={() => send(p)}
                >
                  {p}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div key={i} className={`protocol-chat-msg protocol-chat-msg--${m.role}`}>
            <div className="protocol-chat-msg-role">{m.role === "user" ? "You" : "Assistant"}</div>
            {m.role === "assistant" && m.events && m.events.length > 0 && (
              <ToolTrace events={m.events} collapsed />
            )}
            <div className="protocol-chat-msg-body">{m.content}</div>
          </div>
        ))}

        {busy && (
          <div className="protocol-chat-msg protocol-chat-msg--assistant">
            <div className="protocol-chat-msg-role">Assistant</div>
            {liveEvents.length > 0 ? (
              <ToolTrace events={liveEvents} live />
            ) : (
              <div className="protocol-chat-msg-body protocol-chat-msg-body--loading">
                Thinking…
              </div>
            )}
          </div>
        )}

        {error && (
          <div className="protocol-chat-error">
            <strong>Error:</strong> {error}
          </div>
        )}
      </div>

      <div className="protocol-chat-input-row">
        <textarea
          className="protocol-chat-input"
          rows={2}
          placeholder="Ask a question… (Enter to send, Shift+Enter for newline)"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={onKeyDown}
          disabled={busy}
        />
        {busy ? (
          <button
            type="button"
            className="protocol-chat-send protocol-chat-cancel"
            onClick={cancel}
            title="Cancel the in-flight assistant turn"
          >
            Cancel
          </button>
        ) : (
          <button
            type="button"
            className="protocol-chat-send"
            onClick={() => send()}
            disabled={!input.trim()}
          >
            Send
          </button>
        )}
      </div>
    </div>
  );
}

// Renders the list of tool calls a turn made. Shown live during an
// in-flight request (expanded, with a spinner on the current call) and
// collapsed by default once the answer lands (click to expand).
function ToolTrace({ events, live = false, collapsed = false }) {
  const [open, setOpen] = useState(!collapsed || live);
  const hasPending = events.some((e) => e.kind === "tool_call" && !e.summary);

  if (!open) {
    return (
      <button
        type="button"
        className="protocol-chat-trace-toggle"
        onClick={() => setOpen(true)}
      >
        {events.length} tool call{events.length === 1 ? "" : "s"}
      </button>
    );
  }

  return (
    <div className="protocol-chat-trace">
      <div className="protocol-chat-trace-header">
        <span>{live ? (hasPending ? "Working…" : "Ready") : `${events.length} tool call${events.length === 1 ? "" : "s"}`}</span>
        {!live && (
          <button
            type="button"
            className="protocol-chat-trace-collapse"
            onClick={() => setOpen(false)}
            title="Hide tool trace"
          >
            ×
          </button>
        )}
      </div>
      <ul className="protocol-chat-trace-list">
        {events.map((evt, idx) => (
          <li key={idx} className="protocol-chat-trace-item">
            <span className="protocol-chat-trace-status">
              {evt.summary ? "✓" : "⟳"}
            </span>
            <span className="protocol-chat-trace-name">{evt.name}</span>
            {evt.args && (
              <span className="protocol-chat-trace-args">({evt.args})</span>
            )}
            {evt.summary && (
              <span className="protocol-chat-trace-summary">→ {evt.summary}</span>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
