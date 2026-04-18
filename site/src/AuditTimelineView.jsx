import { useEffect, useState } from "react";

import { getTimeline } from "./api/audits.js";

const CHIP_GREEN = { background: "#dcfce7", color: "#166534", borderColor: "#bbf7d0" };
const CHIP_AMBER = { background: "#fef3c7", color: "#92400e", borderColor: "#fde68a" };
const CHIP_RED = { background: "#fee2e2", color: "#991b1b", borderColor: "#fecaca" };
const CHIP_GRAY = { background: "#f1f5f9", color: "#475569", borderColor: "#e2e8f0" };

// Status → (label, color) for the top-of-section chip. Server-side semantics
// are in api.py:contract_audit_timeline._current_status — keep labels in sync
// with what the user actually wants to see, not the raw enum.
function statusChip(status) {
  switch (status) {
    case "audited":
      return { label: "Current impl audited", style: CHIP_GREEN };
    case "non_proxy_audited":
      return { label: "Audited", style: CHIP_GREEN };
    case "unaudited_since_upgrade":
      return { label: "Unaudited since last upgrade", style: CHIP_AMBER };
    case "never_audited":
      return { label: "Never audited", style: CHIP_RED };
    case "non_proxy_unaudited":
      return { label: "Unaudited", style: CHIP_RED };
    default:
      return { label: status || "unknown", style: CHIP_GRAY };
  }
}

function matchStyle(conf) {
  if (conf === "high") return CHIP_GREEN;
  if (conf === "medium") return CHIP_AMBER;
  if (conf === "low") return CHIP_RED;
  return CHIP_GRAY;
}

function formatAuditDate(date) {
  if (!date) return "—";
  const parsed = new Date(date);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
  }
  return String(date);
}

function parseAuditTs(date) {
  if (!date) return null;
  const t = Date.parse(date);
  return Number.isNaN(t) ? null : t;
}

// Null block range → fall back to audit date vs window timestamps, else
// the same audit shows under every era.
export function coverageMatchesWindow(coverage, window) {
  const covFrom = coverage.covered_from_block;
  const covTo = coverage.covered_to_block;
  const hasBlocks = covFrom != null || covTo != null;
  if (hasBlocks) {
    const eraFrom = window?.from_block ?? -Infinity;
    const eraTo = window?.to_block ?? Infinity;
    const cFrom = covFrom ?? -Infinity;
    const cTo = covTo ?? Infinity;
    return cFrom < eraTo && cTo > eraFrom;
  }
  const auditTs = parseAuditTs(coverage.date);
  if (auditTs == null) return false;
  const eraFromTs = window?.from_ts != null ? parseAuditTs(window.from_ts) ?? -Infinity : -Infinity;
  const eraToTs = window?.to_ts != null ? parseAuditTs(window.to_ts) ?? Infinity : Infinity;
  return auditTs >= eraFromTs && auditTs < eraToTs;
}

// Fallback windows when the audit_timeline endpoint has none yet —
// artifact-level data from /api/analyses fills the gap until the
// resolution worker projects UpgradeEvent rows.
function derivedWindowsFromArtifact(upgradeHistory, contractAddress) {
  if (!upgradeHistory?.proxies || !contractAddress) return [];
  const needle = contractAddress.toLowerCase();
  const proxyEntry = Object.values(upgradeHistory.proxies).find(
    (p) => (p?.proxy_address || "").toLowerCase() === needle,
  );
  if (!proxyEntry) return [];
  const impls = proxyEntry.implementations || [];
  // Normalize artifact unix seconds to the endpoint's ISO string shape.
  return impls.map((impl) => ({
    impl_address: impl.address,
    from_block: impl.block_introduced ?? null,
    to_block: impl.block_replaced ?? null,
    from_ts: impl.timestamp_introduced != null ? new Date(impl.timestamp_introduced * 1000).toISOString() : null,
    to_ts: impl.timestamp_replaced != null ? new Date(impl.timestamp_replaced * 1000).toISOString() : null,
  }));
}

export function AuditTimelinePanel({ timeline, loading, error, companyName, upgradeHistory }) {
  if (loading) return <p className="empty">Loading audit timeline…</p>;
  if (error) return <p className="empty" style={{ color: "#991b1b" }}>Audit timeline error: {error}</p>;
  if (!timeline) return null;

  const coverage = timeline.coverage || [];

  let windows = timeline.impl_windows || [];
  if (windows.length === 0 && upgradeHistory && timeline.contract?.is_proxy) {
    windows = derivedWindowsFromArtifact(upgradeHistory, timeline.contract.address);
  }

  const eraCards = windows.map((w, idx) => {
    const matches = coverage.filter((c) => coverageMatchesWindow(c, w));
    return { window: w, matches, idx };
  });

  const placedAuditIds = new Set();
  for (const card of eraCards) {
    for (const m of card.matches) placedAuditIds.add(m.audit_id);
  }
  const unwindowed = windows.length
    ? coverage.filter((c) => !placedAuditIds.has(c.audit_id))
    : coverage;

  // Backend is block-range strict; relax to "audited" when date alignment
  // on the current era has evidence the backend couldn't see.
  let displayStatus = timeline.current_status;
  if (
    displayStatus === "unaudited_since_upgrade"
    && timeline.contract?.is_proxy
    && eraCards.length > 0
  ) {
    const currentCard = eraCards[eraCards.length - 1];
    if (currentCard?.matches?.length) displayStatus = "audited";
  }
  const chip = statusChip(displayStatus);

  const placedCount = placedAuditIds.size + (windows.length ? 0 : unwindowed.length);

  return (
    <div className="stack">
      <div className="chips">
        <span className="chip" style={chip.style}>{chip.label}</span>
        {timeline.contract?.is_proxy ? (
          <span className="chip" style={CHIP_GRAY}>proxy · {placedCount} audit{placedCount === 1 ? "" : "s"} matched</span>
        ) : (
          <span className="chip" style={CHIP_GRAY}>{placedCount} audit{placedCount === 1 ? "" : "s"} matched</span>
        )}
      </div>

      {eraCards.length > 0 ? (
        <div className="timeline">
          {eraCards.map(({ window: w, matches, idx }) => {
            const isCurrent = w.to_block == null;
            return (
              <div className={`timeline-entry ${isCurrent ? "current" : "past"}`} key={`${w.impl_address}-${w.from_block ?? idx}`}>
                <div className="timeline-marker" />
                <div className="timeline-content">
                  <div className="timeline-header">
                    <strong className="mono">{w.impl_address}</strong>
                    {isCurrent ? <span className="chip" style={CHIP_GREEN}>current</span> : <span className="chip" style={CHIP_GRAY}>replaced</span>}
                  </div>
                  <div className="kv-grid compact small" style={{ marginTop: 4 }}>
                    <div className="kv-row">
                      <span className="key">Active</span>
                      <span>
                        {w.from_ts ? formatAuditDate(w.from_ts) : `block ${w.from_block?.toLocaleString?.() ?? "?"}`}
                        {"  →  "}
                        {w.to_ts ? formatAuditDate(w.to_ts) : w.to_block != null ? `block ${w.to_block.toLocaleString()}` : "current"}
                      </span>
                    </div>
                  </div>
                  <div className="chips" style={{ marginTop: 6 }}>
                    {matches.length === 0 ? (
                      <span className="chip" style={CHIP_RED}>no audit coverage</span>
                    ) : (
                      matches.map((m) => {
                        const hint = companyName
                          ? `/company/${encodeURIComponent(companyName)}/audits?audit=${m.audit_id}`
                          : null;
                        const label = (
                          <>
                            {m.auditor || "Unknown"}
                            <span style={{ marginLeft: 6, opacity: 0.8 }}>
                              {formatAuditDate(m.date)}
                            </span>
                          </>
                        );
                        const baseStyle = matchStyle(m.match_confidence);
                        return hint ? (
                          <a
                            key={m.audit_id}
                            href={hint}
                            className="chip"
                            style={{ ...baseStyle, textDecoration: "none" }}
                            title={`${m.match_type} · ${m.match_confidence}${m.title ? ` — ${m.title}` : ""}`}
                          >
                            {label}
                          </a>
                        ) : (
                          <span
                            key={m.audit_id}
                            className="chip"
                            style={baseStyle}
                            title={`${m.match_type} · ${m.match_confidence}${m.title ? ` — ${m.title}` : ""}`}
                          >
                            {label}
                          </span>
                        );
                      })
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      ) : null}

      {unwindowed.length ? (
        <div className="card">
          <div className="subsection-title">
            {windows.length ? "Audits outside any known impl era" : "Audits"}
          </div>
          <div className="chips" style={{ marginTop: 6 }}>
            {unwindowed.map((m) => {
              const baseStyle = matchStyle(m.match_confidence);
              const hint = companyName
                ? `/company/${encodeURIComponent(companyName)}/audits?audit=${m.audit_id}`
                : null;
              const body = (
                <>
                  {m.auditor || "Unknown"}
                  <span style={{ marginLeft: 6, opacity: 0.8 }}>{formatAuditDate(m.date)}</span>
                </>
              );
              return hint ? (
                <a
                  key={m.audit_id}
                  href={hint}
                  className="chip"
                  style={{ ...baseStyle, textDecoration: "none" }}
                  title={`${m.match_type} · ${m.match_confidence}${m.title ? ` — ${m.title}` : ""}`}
                >
                  {body}
                </a>
              ) : (
                <span
                  key={m.audit_id}
                  className="chip"
                  style={baseStyle}
                  title={`${m.match_type} · ${m.match_confidence}${m.title ? ` — ${m.title}` : ""}`}
                >
                  {body}
                </span>
              );
            })}
          </div>
        </div>
      ) : null}

      {!eraCards.length && !unwindowed.length ? (
        <p className="empty">No audit coverage recorded yet.</p>
      ) : null}
    </div>
  );
}

export default function AuditTimelineView({ contractId, companyName, upgradeHistory }) {
  const [timeline, setTimeline] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!contractId) {
      setTimeline(null);
      setLoading(false);
      setError("No contract_id on this analysis — audits unavailable.");
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    getTimeline(contractId)
      .then((d) => { if (!cancelled) setTimeline(d); })
      .catch((err) => { if (!cancelled) setError(err.message || String(err)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [contractId]);

  return (
    <AuditTimelinePanel
      timeline={timeline}
      loading={loading}
      error={error}
      companyName={companyName}
      upgradeHistory={upgradeHistory}
    />
  );
}
