import { useEffect, useState } from "react";

import { api } from "../api/client.js";
import { shortenAddress } from "../graph.js";

export default function ProxyWatcherPage() {
  const [proxies, setProxies] = useState([]);
  const [events, setEvents] = useState([]);
  const [loaded, setLoaded] = useState(false);
  const [address, setAddress] = useState("");
  const [label, setLabel] = useState("");
  const [discordWebhook, setDiscordWebhook] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);
  const [subscriptions, setSubscriptions] = useState({});
  const [expandedProxy, setExpandedProxy] = useState(null);
  const [newWebhook, setNewWebhook] = useState("");
  const [monitoredCount, setMonitoredCount] = useState(0);

  useEffect(() => {
    (async () => {
      try {
        const mc = await api("/api/monitored-contracts");
        setMonitoredCount(Array.isArray(mc) ? mc.length : 0);
      } catch { /* ignore */ }
    })();
  }, []);

  async function refresh() {
    try {
      const [p, e] = await Promise.all([
        api("/api/watched-proxies"),
        api("/api/proxy-events?limit=100"),
      ]);
      setProxies(p);
      setEvents(e);
      // Fetch subscriptions for all proxies
      const subMap = {};
      await Promise.all(
        p.map(async (proxy) => {
          try {
            subMap[proxy.id] = await api(`/api/watched-proxies/${proxy.id}/subscriptions`);
          } catch {
            subMap[proxy.id] = [];
          }
        })
      );
      setSubscriptions(subMap);
      setLoaded(true);
    } catch (err) {
      console.error("Failed to load proxy data:", err);
    }
  }

  async function addSubscription(proxyId) {
    if (!newWebhook.trim()) return;
    try {
      await api(`/api/watched-proxies/${proxyId}/subscriptions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ discord_webhook_url: newWebhook.trim() }),
      });
      setNewWebhook("");
      refresh();
    } catch (err) {
      setError(String(err.message || err));
    }
  }

  async function removeSubscription(subId) {
    try {
      await api(`/api/subscriptions/${subId}`, { method: "DELETE" });
      refresh();
    } catch (err) {
      console.error("Failed to remove subscription:", err);
    }
  }

  useEffect(() => {
    refresh();
    const timer = setInterval(refresh, 10000);
    return () => clearInterval(timer);
  }, []);

  async function addProxy(e) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await api("/api/watched-proxies", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address: address.trim(), label: label.trim() || null, discord_webhook_url: discordWebhook.trim() || null }),
      });
      setAddress("");
      setLabel("");
      setDiscordWebhook("");
      refresh();
    } catch (err) {
      setError(String(err.message || err));
    } finally {
      setSubmitting(false);
    }
  }

  async function removeProxy(id) {
    try {
      await api(`/api/watched-proxies/${id}`, { method: "DELETE" });
      refresh();
    } catch (err) {
      console.error("Failed to remove proxy:", err);
    }
  }

  if (!loaded) {
    return (
      <div className="page">
        <section className="panel">
          <p style={{ textAlign: "center", padding: "2rem 0", color: "#64748b" }}>Loading proxy watcher...</p>
        </section>
      </div>
    );
  }

  return (
    <div className="page">
      {monitoredCount > 0 && (
        <div style={{ marginBottom: 16, padding: "10px 16px", borderRadius: 8, background: "#1e293b", border: "1px solid #334155", display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
          <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#22c55e" }} />
          <span style={{ color: "#e2e8f0" }}>Protocol monitoring active &mdash; <strong>{monitoredCount}</strong> contract{monitoredCount !== 1 ? "s" : ""} monitored</span>
        </div>
      )}
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Proxy Watcher</p>
            <h2>Watched Proxies ({proxies.length})</h2>
          </div>
          <span className="chip" style={{ background: "#fef3c7", color: "#92400e", fontSize: 11 }}>Work in Progress</span>
        </div>

        <form onSubmit={addProxy} style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <input
            value={address}
            onChange={(e) => setAddress(e.target.value)}
            placeholder="Proxy address (0x...)"
            required
            style={{ flex: "1 1 300px", fontFamily: "monospace", fontSize: 13, padding: "8px 12px", borderRadius: 6, border: "1px solid #334155", background: "#0f172a", color: "#e2e8f0" }}
          />
          <input
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            placeholder="Label (optional)"
            style={{ flex: "0 1 200px", fontSize: 13, padding: "8px 12px", borderRadius: 6, border: "1px solid #334155", background: "#0f172a", color: "#e2e8f0" }}
          />
          <input
            value={discordWebhook}
            onChange={(e) => setDiscordWebhook(e.target.value)}
            placeholder="Discord webhook URL (optional)"
            style={{ flex: "1 1 300px", fontSize: 13, padding: "8px 12px", borderRadius: 6, border: "1px solid #334155", background: "#0f172a", color: "#e2e8f0" }}
          />
          <button type="submit" disabled={submitting} style={{ padding: "8px 16px", borderRadius: 6, background: "#2563eb", color: "#fff", border: "none", cursor: "pointer", fontWeight: 600, fontSize: 13 }}>
            {submitting ? "Adding..." : "Watch Proxy"}
          </button>
        </form>
        {error && <p style={{ color: "#ef4444", fontSize: 13, marginBottom: 12 }}>{error}</p>}

        {proxies.length === 0 ? (
          <p className="empty">No proxies being watched. Add one above to start monitoring for upgrades.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #334155", textAlign: "left" }}>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Label</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Address</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Type</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Implementation</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Polling</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Notifications</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Last Block</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}></th>
                </tr>
              </thead>
              <tbody>
                {proxies.map((p) => {
                  const subs = subscriptions[p.id] || [];
                  const isExpanded = expandedProxy === p.id;
                  return (
                    <React.Fragment key={p.id}>
                      <tr style={{ borderBottom: isExpanded ? "none" : "1px solid #1e293b" }}>
                        <td style={{ padding: "8px 12px" }}>{p.label || <span style={{ color: "#475569" }}>-</span>}</td>
                        <td style={{ padding: "8px 12px" }}><span className="mono">{shortenAddress(p.proxy_address)}</span></td>
                        <td style={{ padding: "8px 12px" }}>{p.proxy_type ? <span className="chip alt">{p.proxy_type}</span> : <span style={{ color: "#475569" }}>unknown</span>}</td>
                        <td style={{ padding: "8px 12px" }}><span className="mono">{p.last_known_implementation ? shortenAddress(p.last_known_implementation) : "-"}</span></td>
                        <td style={{ padding: "8px 12px" }}>{p.needs_polling ? <span className="chip warn">polling</span> : <span className="chip">events</span>}</td>
                        <td style={{ padding: "8px 12px" }}>
                          <button
                            onClick={() => setExpandedProxy(isExpanded ? null : p.id)}
                            style={{ background: "none", border: "none", color: subs.length > 0 ? "#22c55e" : "#64748b", cursor: "pointer", fontSize: 12 }}
                          >
                            {subs.length > 0 ? `${subs.length} webhook${subs.length > 1 ? "s" : ""}` : "none"} {isExpanded ? "\u25B2" : "\u25BC"}
                          </button>
                        </td>
                        <td style={{ padding: "8px 12px" }}>{p.last_scanned_block ? p.last_scanned_block.toLocaleString() : "-"}</td>
                        <td style={{ padding: "8px 12px" }}>
                          <button onClick={() => removeProxy(p.id)} style={{ background: "none", border: "none", color: "#ef4444", cursor: "pointer", fontSize: 12 }}>remove</button>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr style={{ borderBottom: "1px solid #1e293b" }}>
                          <td colSpan={8} style={{ padding: "8px 12px 16px 24px", background: "#0c1222" }}>
                            <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 8 }}>Discord Subscriptions</div>
                            {subs.map((s) => (
                              <div key={s.id} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                                <span className="mono" style={{ fontSize: 12, color: "#cbd5e1" }}>{s.discord_webhook_url ? s.discord_webhook_url.slice(0, 60) + (s.discord_webhook_url.length > 60 ? "..." : "") : "-"}</span>
                                {s.label && <span style={{ fontSize: 11, color: "#64748b" }}>({s.label})</span>}
                                <button onClick={() => removeSubscription(s.id)} style={{ background: "none", border: "none", color: "#ef4444", cursor: "pointer", fontSize: 11, marginLeft: "auto" }}>remove</button>
                              </div>
                            ))}
                            <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
                              <input
                                value={newWebhook}
                                onChange={(e) => setNewWebhook(e.target.value)}
                                placeholder="Discord webhook URL"
                                style={{ flex: 1, fontSize: 12, padding: "6px 10px", borderRadius: 4, border: "1px solid #334155", background: "#0f172a", color: "#e2e8f0" }}
                                onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addSubscription(p.id); } }}
                              />
                              <button onClick={() => addSubscription(p.id)} style={{ padding: "6px 12px", borderRadius: 4, background: "#7c3aed", color: "#fff", border: "none", cursor: "pointer", fontWeight: 600, fontSize: 12 }}>Add</button>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="panel" style={{ marginTop: 16 }}>
        <div className="panel-header">
          <div>
            <p className="eyebrow">Detected Events</p>
            <h2>Upgrade Events ({events.length})</h2>
          </div>
        </div>
        {events.length === 0 ? (
          <p className="empty">No upgrade events detected yet.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #334155", textAlign: "left" }}>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Time</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Proxy</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Type</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Old Impl</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>New Impl</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Block</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Tx</th>
                </tr>
              </thead>
              <tbody>
                {events.map((evt) => {
                  const proxy = proxies.find((p) => p.id === evt.watched_proxy_id);
                  return (
                    <tr key={evt.id} style={{ borderBottom: "1px solid #1e293b" }}>
                      <td style={{ padding: "8px 12px", whiteSpace: "nowrap" }}>{new Date(evt.detected_at).toLocaleString()}</td>
                      <td style={{ padding: "8px 12px" }}>{proxy?.label || <span className="mono">{shortenAddress(proxy?.proxy_address || "")}</span>}</td>
                      <td style={{ padding: "8px 12px" }}><span className="chip alt">{evt.event_type}</span></td>
                      <td style={{ padding: "8px 12px" }}><span className="mono">{evt.old_implementation ? shortenAddress(evt.old_implementation) : "-"}</span></td>
                      <td style={{ padding: "8px 12px" }}><span className="mono">{shortenAddress(evt.new_implementation)}</span></td>
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
