import { useEffect, useMemo, useState } from "react";

import { api } from "../api/client.js";
import { shortenAddress } from "../graph.js";

const CONTRACT_TYPE_COLORS = {
  proxy: "#2563eb",
  safe: "#7c3aed",
  timelock: "#d97706",
  pausable: "#ea580c",
  access_control: "#0d9488",
  regular: "#64748b",
};

const CONTRACT_TYPE_ORDER = ["proxy", "safe", "timelock", "pausable", "access_control", "regular"];

const ALL_EVENT_TYPES = [
  "upgraded", "admin_changed", "beacon_upgraded", "ownership_transferred",
  "paused", "unpaused", "role_granted", "role_revoked",
  "signer_added", "signer_removed", "threshold_changed",
  "safe_tx_executed", "safe_tx_failed",
  "safe_module_executed", "safe_module_failed",
  "timelock_scheduled", "timelock_executed", "delay_changed",
  "state_changed_poll",
];

const EVENT_TYPE_COLORS = {
  ownership_transferred: "#ef4444",
  paused: "#ef4444",
  unpaused: "#ef4444",
  upgraded: "#f59e0b",
  admin_changed: "#f59e0b",
  beacon_upgraded: "#f59e0b",
  timelock_executed: "#f59e0b",
  timelock_scheduled: "#3b82f6",
  signer_added: "#3b82f6",
  signer_removed: "#3b82f6",
  safe_tx_executed: "#22c55e",
  safe_tx_failed: "#ef4444",
  safe_module_executed: "#22c55e",
  safe_module_failed: "#ef4444",
  role_granted: "#f59e0b",
  role_revoked: "#f59e0b",
  threshold_changed: "#f59e0b",
  delay_changed: "#f59e0b",
  state_changed_poll: "#8b5cf6",
};

export default function ProtocolMonitoringPage({ companyName }) {
  const [protocolId, setProtocolId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [noProtocol, setNoProtocol] = useState(false);
  const [contracts, setContracts] = useState([]);
  const [subscriptions, setSubscriptions] = useState([]);
  const [events, setEvents] = useState([]);
  const [webhookUrl, setWebhookUrl] = useState("");
  const [webhookLabel, setWebhookLabel] = useState("");
  const [webhookEventTypes, setWebhookEventTypes] = useState([]);
  const [showEventPicker, setShowEventPicker] = useState(false);
  const [addingWebhook, setAddingWebhook] = useState(false);
  const [error, setError] = useState(null);
  const [reEnrolling, setReEnrolling] = useState(false);

  // Fetch protocol_id from company overview
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api(`/api/company/${encodeURIComponent(companyName)}`);
        if (cancelled) return;
        if (data.protocol_id) {
          setProtocolId(data.protocol_id);
        } else {
          setNoProtocol(true);
        }
      } catch {
        if (!cancelled) setNoProtocol(true);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [companyName]);

  // Fetch monitoring data once protocolId is known + auto-refresh
  const refresh = useMemo(() => {
    if (!protocolId) return null;
    return async () => {
      try {
        const [c, s, e] = await Promise.all([
          api(`/api/protocols/${protocolId}/monitoring`),
          api(`/api/protocols/${protocolId}/subscriptions`),
          api(`/api/protocols/${protocolId}/events?limit=100`),
        ]);
        setContracts(c);
        setSubscriptions(s);
        setEvents(e);
      } catch (err) {
        console.error("Failed to load monitoring data:", err);
      }
    };
  }, [protocolId]);

  useEffect(() => {
    if (!refresh) return;
    refresh();
    const timer = setInterval(refresh, 10000);
    return () => clearInterval(timer);
  }, [refresh]);

  async function addWebhook(e) {
    e.preventDefault();
    if (!webhookUrl.trim() || !protocolId) return;
    setAddingWebhook(true);
    setError(null);
    try {
      const body = {
        discord_webhook_url: webhookUrl.trim(),
        label: webhookLabel.trim() || null,
      };
      if (webhookEventTypes.length > 0) {
        body.event_filter = { event_types: webhookEventTypes };
      }
      await api(`/api/protocols/${protocolId}/subscribe`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      setWebhookUrl("");
      setWebhookLabel("");
      setWebhookEventTypes([]);
      setShowEventPicker(false);
      if (refresh) refresh();
    } catch (err) {
      setError(String(err.message || err));
    } finally {
      setAddingWebhook(false);
    }
  }

  async function reEnroll() {
    if (!protocolId) return;
    setReEnrolling(true);
    setError(null);
    try {
      await api(`/api/protocols/${protocolId}/re-enroll`, { method: "POST" });
      if (refresh) refresh();
    } catch (err) {
      setError(String(err.message || err));
    } finally {
      setReEnrolling(false);
    }
  }

  async function toggleContractActive(contractId, currentActive) {
    try {
      await api(`/api/monitored-contracts/${contractId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_active: !currentActive }),
      });
      if (refresh) refresh();
    } catch (err) {
      console.error("Failed to toggle contract:", err);
    }
  }

  async function removeSubscription(subId) {
    try {
      await api(`/api/protocol-subscriptions/${subId}`, { method: "DELETE" });
      if (refresh) refresh();
    } catch (err) {
      console.error("Failed to remove subscription:", err);
    }
  }

  if (loading) {
    return (
      <div className="page">
        <section className="panel">
          <p style={{ textAlign: "center", padding: "2rem 0", color: "#64748b" }}>Loading protocol monitoring...</p>
        </section>
      </div>
    );
  }

  if (noProtocol) {
    return (
      <div className="page">
        <section className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Protocol Monitoring</p>
              <h2>Not Available</h2>
            </div>
          </div>
          <p className="empty">No protocol monitoring available for this company. Run a protocol analysis first.</p>
        </section>
      </div>
    );
  }

  // Sort contracts by type priority
  const sortedContracts = [...contracts].sort((a, b) => {
    const ai = CONTRACT_TYPE_ORDER.indexOf(a.contract_type);
    const bi = CONTRACT_TYPE_ORDER.indexOf(b.contract_type);
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });

  // Helper: extract monitoring config flags as chips
  function monitoringChips(config) {
    if (!config || typeof config !== "object") return null;
    const flags = [];
    if (config.watch_upgrades) flags.push("upgrades");
    if (config.watch_ownership) flags.push("ownership");
    if (config.watch_pause) flags.push("pause");
    if (config.watch_roles) flags.push("roles");
    // Read both the new and legacy keys so auto-enrolled rows (which
    // only set watch_safe_signers) and old user data (which set the
    // historical watch_signers) both light up the chip.
    if (config.watch_safe_signers || config.watch_signers) flags.push("safe activity");
    if (config.watch_timelock) flags.push("timelock");
    if (config.watch_state) flags.push("state");
    if (flags.length === 0) return <span style={{ color: "#475569" }}>none</span>;
    return flags.map((f) => (
      <span key={f} className="chip" style={{ fontSize: 11, marginRight: 4, marginBottom: 2 }}>{f}</span>
    ));
  }

  // Helper: render event details from data object
  function renderEventDetails(data) {
    if (!data || typeof data !== "object") return "-";
    const entries = Object.entries(data).filter(([k]) => !["contract_address", "contract_type", "chain"].includes(k));
    if (entries.length === 0) return "-";
    return entries.map(([k, v]) => (
      <span key={k} style={{ marginRight: 8, fontSize: 12 }}>
        <span style={{ color: "#64748b" }}>{k.replace(/_/g, " ")}:</span>{" "}
        <span className="mono" style={{ color: "#e2e8f0" }}>{typeof v === "string" && v.startsWith("0x") ? shortenAddress(v) : String(v)}</span>
      </span>
    ));
  }

  return (
    <div className="page">
      {/* Section 1: Discord Notifications */}
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Discord Notifications</p>
            <h2>Webhook Subscriptions ({subscriptions.length})</h2>
          </div>
        </div>

        <form onSubmit={addWebhook} style={{ marginBottom: 16 }}>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <input
              value={webhookUrl}
              onChange={(e) => setWebhookUrl(e.target.value)}
              placeholder="Discord webhook URL"
              required
              style={{ flex: "1 1 300px", fontFamily: "monospace", fontSize: 13, padding: "8px 12px", borderRadius: 6, border: "1px solid #334155", background: "#0f172a", color: "#e2e8f0" }}
            />
            <input
              value={webhookLabel}
              onChange={(e) => setWebhookLabel(e.target.value)}
              placeholder="Label (optional)"
              style={{ flex: "0 1 200px", fontSize: 13, padding: "8px 12px", borderRadius: 6, border: "1px solid #334155", background: "#0f172a", color: "#e2e8f0" }}
            />
            <button type="submit" disabled={addingWebhook} style={{ padding: "8px 16px", borderRadius: 6, background: "#2563eb", color: "#fff", border: "none", cursor: "pointer", fontWeight: 600, fontSize: 13 }}>
              {addingWebhook ? "Adding..." : "Add Webhook"}
            </button>
          </div>
          <div style={{ marginTop: 8 }}>
            <button
              type="button"
              onClick={() => setShowEventPicker(!showEventPicker)}
              style={{ background: "none", border: "none", color: "#94a3b8", cursor: "pointer", fontSize: 12, padding: 0 }}
            >
              {showEventPicker ? "- Hide event filter" : "+ Filter by event type"}
              {webhookEventTypes.length > 0 && ` (${webhookEventTypes.length} selected)`}
            </button>
            {showEventPicker && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 8, padding: 8, borderRadius: 6, border: "1px solid #334155", background: "#0f172a" }}>
                {ALL_EVENT_TYPES.map((et) => {
                  const selected = webhookEventTypes.includes(et);
                  const evtColor = EVENT_TYPE_COLORS[et] || "#94a3b8";
                  return (
                    <button
                      key={et}
                      type="button"
                      onClick={() => {
                        setWebhookEventTypes((prev) =>
                          selected ? prev.filter((t) => t !== et) : [...prev, et]
                        );
                      }}
                      style={{
                        padding: "3px 8px",
                        borderRadius: 4,
                        fontSize: 11,
                        fontWeight: 600,
                        cursor: "pointer",
                        border: selected ? `1px solid ${evtColor}` : "1px solid #334155",
                        background: selected ? evtColor + "22" : "transparent",
                        color: selected ? evtColor : "#64748b",
                      }}
                    >
                      {et.replace(/_/g, " ")}
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </form>
        {error && <p style={{ color: "#ef4444", fontSize: 13, marginBottom: 12 }}>{error}</p>}

        {subscriptions.length === 0 ? (
          <p className="empty">No webhook subscriptions. Add one above to receive Discord notifications for governance events.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #334155", textAlign: "left" }}>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Label</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Webhook URL</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Event Filter</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Created</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}></th>
                </tr>
              </thead>
              <tbody>
                {subscriptions.map((s) => (
                  <tr key={s.id} style={{ borderBottom: "1px solid #1e293b" }}>
                    <td style={{ padding: "8px 12px" }}>{s.label || <span style={{ color: "#475569" }}>-</span>}</td>
                    <td style={{ padding: "8px 12px" }}>
                      <span className="mono" style={{ fontSize: 12 }}>
                        {s.discord_webhook_url ? s.discord_webhook_url.slice(0, 60) + (s.discord_webhook_url.length > 60 ? "..." : "") : "-"}
                      </span>
                    </td>
                    <td style={{ padding: "8px 12px" }}>
                      {s.event_filter && Array.isArray(s.event_filter.event_types) && s.event_filter.event_types.length > 0 ? (
                        s.event_filter.event_types.map((f) => (
                          <span key={f} className="chip" style={{ fontSize: 11, marginRight: 4 }}>{f.replace(/_/g, " ")}</span>
                        ))
                      ) : <span style={{ color: "#475569" }}>all events</span>}
                    </td>
                    <td style={{ padding: "8px 12px", whiteSpace: "nowrap", color: "#94a3b8", fontSize: 12 }}>
                      {s.created_at ? new Date(s.created_at).toLocaleDateString() : "-"}
                    </td>
                    <td style={{ padding: "8px 12px" }}>
                      <button onClick={() => removeSubscription(s.id)} style={{ background: "none", border: "none", color: "#ef4444", cursor: "pointer", fontSize: 12 }}>remove</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Section 2: Monitored Contracts */}
      <section className="panel" style={{ marginTop: 16 }}>
        <div className="panel-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
          <div>
            <p className="eyebrow">Monitored Contracts</p>
            <h2>Contracts ({sortedContracts.length})</h2>
          </div>
          <button
            onClick={reEnroll}
            disabled={reEnrolling}
            style={{ padding: "6px 14px", borderRadius: 6, background: "#1e293b", color: "#94a3b8", border: "1px solid #334155", cursor: "pointer", fontSize: 12, fontWeight: 600, whiteSpace: "nowrap" }}
          >
            {reEnrolling ? "Re-enrolling..." : "Re-enroll Contracts"}
          </button>
        </div>
        {sortedContracts.length === 0 ? (
          <p className="empty">No contracts being monitored for this protocol.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #334155", textAlign: "left" }}>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Address</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Type</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Watching</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Polling</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Last Block</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Active</th>
                </tr>
              </thead>
              <tbody>
                {sortedContracts.map((c) => {
                  const typeColor = CONTRACT_TYPE_COLORS[c.contract_type] || CONTRACT_TYPE_COLORS.regular;
                  return (
                    <tr key={c.id} style={{ borderBottom: "1px solid #1e293b", opacity: c.is_active ? 1 : 0.5 }}>
                      <td style={{ padding: "8px 12px" }}><span className="mono">{shortenAddress(c.address)}</span></td>
                      <td style={{ padding: "8px 12px" }}>
                        <span style={{ display: "inline-block", padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, background: typeColor + "22", color: typeColor }}>
                          {c.contract_type || "regular"}
                        </span>
                      </td>
                      <td style={{ padding: "8px 12px" }}>{monitoringChips(c.monitoring_config)}</td>
                      <td style={{ padding: "8px 12px" }}>{c.needs_polling ? <span className="chip warn">polling</span> : <span className="chip">events</span>}</td>
                      <td style={{ padding: "8px 12px" }}>{c.last_scanned_block ? c.last_scanned_block.toLocaleString() : "-"}</td>
                      <td style={{ padding: "8px 12px" }}>
                        <button
                          onClick={() => toggleContractActive(c.id, c.is_active)}
                          style={{
                            padding: "2px 10px", borderRadius: 4, fontSize: 11, fontWeight: 600, cursor: "pointer",
                            border: "1px solid " + (c.is_active ? "#22c55e" : "#475569"),
                            background: c.is_active ? "#22c55e22" : "transparent",
                            color: c.is_active ? "#22c55e" : "#475569",
                          }}
                        >
                          {c.is_active ? "on" : "off"}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Section 3: Detected Events */}
      <section className="panel" style={{ marginTop: 16 }}>
        <div className="panel-header">
          <div>
            <p className="eyebrow">Detected Events</p>
            <h2>Governance Events ({events.length})</h2>
          </div>
        </div>
        {events.length === 0 ? (
          <p className="empty">No governance events detected yet. Events will appear here as they are detected by the monitoring system.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #334155", textAlign: "left" }}>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Time</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Contract</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Event</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Details</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Block</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Tx</th>
                </tr>
              </thead>
              <tbody>
                {events.map((evt) => {
                  const evtColor = EVENT_TYPE_COLORS[evt.event_type] || "#94a3b8";
                  const contractAddr = evt.data?.contract_address || "";
                  return (
                    <tr key={evt.id} style={{ borderBottom: "1px solid #1e293b" }}>
                      <td style={{ padding: "8px 12px", whiteSpace: "nowrap" }}>{evt.detected_at ? new Date(evt.detected_at).toLocaleString() : "-"}</td>
                      <td style={{ padding: "8px 12px" }}><span className="mono">{shortenAddress(contractAddr)}</span></td>
                      <td style={{ padding: "8px 12px" }}>
                        <span style={{ display: "inline-block", padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, background: evtColor + "22", color: evtColor }}>
                          {(evt.event_type || "unknown").replace(/_/g, " ")}
                        </span>
                      </td>
                      <td style={{ padding: "8px 12px" }}>{renderEventDetails(evt.data)}</td>
                      <td style={{ padding: "8px 12px" }}>{evt.block_number || "-"}</td>
                      <td style={{ padding: "8px 12px" }}><span className="mono">{evt.tx_hash ? shortenAddress(evt.tx_hash) : "-"}</span></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
