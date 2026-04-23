import React, { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api/client.js";

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

export default function ProtocolChat({ companyName }) {
  const [messages, setMessages] = useState([]); // [{role, content}]
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const listRef = useRef(null);

  // Scroll to bottom on new messages so the latest answer is visible
  // without the user having to hunt for it.
  useEffect(() => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, busy]);

  const send = useCallback(
    async (questionText) => {
      const q = (questionText || input).trim();
      if (!q || busy) return;
      const next = [...messages, { role: "user", content: q }];
      setMessages(next);
      setInput("");
      setBusy(true);
      setError(null);
      try {
        const resp = await api(`/api/company/${encodeURIComponent(companyName)}/ask`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            question: q,
            // Send prior turns so multi-turn follow-ups ("show me the list
            // of those contracts") land in the right context.
            history: next.slice(0, -1).map((m) => ({ role: m.role, content: m.content })),
          }),
        });
        setMessages([...next, { role: "assistant", content: resp.answer || "(no response)" }]);
      } catch (e) {
        setError(e?.message || String(e));
      } finally {
        setBusy(false);
      }
    },
    [busy, companyName, input, messages],
  );

  const onKeyDown = (e) => {
    // Enter to send, Shift+Enter for newline — standard chat affordance.
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
          <div
            key={i}
            className={`protocol-chat-msg protocol-chat-msg--${m.role}`}
          >
            <div className="protocol-chat-msg-role">
              {m.role === "user" ? "You" : "Assistant"}
            </div>
            <div className="protocol-chat-msg-body">{m.content}</div>
          </div>
        ))}
        {busy && (
          <div className="protocol-chat-msg protocol-chat-msg--assistant">
            <div className="protocol-chat-msg-role">Assistant</div>
            <div className="protocol-chat-msg-body protocol-chat-msg-body--loading">
              Thinking…
            </div>
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
        <button
          type="button"
          className="protocol-chat-send"
          onClick={() => send()}
          disabled={busy || !input.trim()}
        >
          Send
        </button>
      </div>
    </div>
  );
}
