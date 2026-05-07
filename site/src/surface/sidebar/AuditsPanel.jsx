import { useEffect, useState } from "react";

import { getTimeline } from "../../api/audits.js";
import {
  AUDIT_STATUS_META,
  DRIFT_FALSE_META,
  DRIFT_TRUE_META,
  EQUIVALENCE_META,
  formatAuditDate,
  MATCH_TYPE_META,
  MetaBadge,
  proofKindTitle,
  PROOF_KIND_META,
  SEVERITY_META,
  STATUS_LABELS,
} from "../../auditUi.jsx";

function LiveFindingsSection({ coverage }) {
  // Collect every live finding across covering audits, tagged with its
  // originating auditor/title so the user can see which audit raised it.
  const entries = [];
  for (const a of coverage) {
    const lf = a.live_findings || [];
    for (const f of lf) {
      entries.push({ finding: f, audit: a });
    }
  }

  if (entries.length === 0) return null;

  // Sort: severity descending (critical first), stable within.
  const severityRank = { critical: 0, high: 1, medium: 2, low: 3, info: 4 };
  entries.sort((x, y) => {
    const rx = severityRank[x.finding.severity] ?? 5;
    const ry = severityRank[y.finding.severity] ?? 5;
    return rx - ry;
  });

  return (
    <div style={{ marginTop: 16 }}>
      <div className="ps-principal-section-hdr">Live findings on current code</div>
      <div style={{ fontSize: 11, color: "#6b7590", marginBottom: 8 }}>
        Issues that were not marked "fixed" in the audit. May still be active.
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {entries.map((e, i) => {
          const sevMeta = SEVERITY_META[e.finding.severity] || SEVERITY_META.info;
          const statusLabel = STATUS_LABELS[e.finding.status] || e.finding.status || "?";
          return (
            <div
              key={i}
              style={{
                padding: "6px 8px",
                borderRadius: 5,
                border: "1px solid #e2e8f0",
                background: "#fff",
              }}
            >
              <div style={{ display: "flex", gap: 6, alignItems: "baseline", flexWrap: "wrap" }}>
                <MetaBadge meta={sevMeta} label={e.finding.severity || "info"} />
                <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
                  {statusLabel}
                </span>
                <span style={{ fontSize: 11, color: "#6b7590" }}>
                  {e.audit.auditor || "Unknown"}
                </span>
              </div>
              <div style={{ fontSize: 12, color: "#334155", marginTop: 4 }}>
                {e.finding.title || "(untitled)"}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function AuditsPanel({ machine, companyName }) {
  const [timeline, setTimeline] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const contractId = machine.contract_id;

  useEffect(() => {
    if (!contractId) {
      setTimeline(null);
      return undefined;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    getTimeline(contractId)
      .then((data) => {
        if (cancelled) return;
        setTimeline(data);
        setLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err?.message || "Failed to load audits");
        setLoading(false);
      });
    return () => { cancelled = true; };
  }, [contractId]);

  if (!contractId) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Contract not yet indexed for audit coverage.</div>
      </section>
    );
  }
  if (loading) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Loading audits…</div>
      </section>
    );
  }
  if (error) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Failed to load audits: {error}</div>
      </section>
    );
  }
  if (!timeline) return null;

  const statusMeta = AUDIT_STATUS_META[timeline.current_status] || AUDIT_STATUS_META.non_proxy_unaudited;
  const coverage = timeline.coverage || [];
  const topAudits = coverage.slice(0, 5);

  const handleAuditClick = (auditId) => {
    const url = `/company/${encodeURIComponent(companyName)}/audits?audit=${encodeURIComponent(auditId)}`;
    window.location.href = url;
  };

  return (
    <section className="ps-principal-section">
      <div className="ps-principal-section-hdr">Audit coverage</div>

      <div style={{ marginBottom: 12 }}>
        <MetaBadge meta={statusMeta} />
        <span style={{ marginLeft: 8, color: "#6b7590", fontSize: 12 }}>
          {coverage.length} audit{coverage.length === 1 ? "" : "s"}
          {coverage.length > topAudits.length ? ` (showing ${topAudits.length} most recent)` : ""}
        </span>
      </div>

      {topAudits.length === 0 ? (
        <div className="ps-inspector-empty">No audits cover this contract yet.</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {topAudits.map((a) => {
            const matchMeta = MATCH_TYPE_META[a.match_type] || MATCH_TYPE_META.direct;
            // drift === true means the bytecode at the impl address changed
            // since this audit was matched. drift === null/undefined means
            // we couldn't determine (missing keccak on either side) — show
            // no badge rather than a misleading one.
            const driftKnown = a.bytecode_drift === true || a.bytecode_drift === false;
            return (
              <button
                key={a.audit_id}
                onClick={() => handleAuditClick(a.audit_id)}
                style={{
                  textAlign: "left",
                  padding: "8px 10px",
                  borderRadius: 6,
                  border: "1px solid #e2e8f0",
                  background: "#fafafa",
                  cursor: "pointer",
                  font: "inherit",
                  color: "inherit",
                }}
              >
                <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 8 }}>
                  <div style={{ fontWeight: 600, fontSize: 13 }}>{a.auditor || "Unknown"}</div>
                  <div style={{ fontSize: 11, color: "#6b7590", whiteSpace: "nowrap" }}>{formatAuditDate(a.date)}</div>
                </div>
                <div style={{ fontSize: 12, color: "#334155", marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {a.title || ""}
                </div>
                <div style={{ marginTop: 4, display: "flex", gap: 4, flexWrap: "wrap" }}>
                  <MetaBadge meta={matchMeta} />
                  {a.match_confidence && (
                    <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
                      {a.match_confidence}
                    </span>
                  )}
                  {a.equivalence_status && a.equivalence_status !== "proven" && EQUIVALENCE_META[a.equivalence_status] && (
                    <MetaBadge
                      meta={EQUIVALENCE_META[a.equivalence_status]}
                      title={a.equivalence_reason || ""}
                    />
                  )}
                  {a.equivalence_status === "proven" && a.proof_kind && PROOF_KIND_META[a.proof_kind] && (
                    <MetaBadge
                      meta={PROOF_KIND_META[a.proof_kind]}
                      title={proofKindTitle(a.proof_kind)}
                    />
                  )}
                  {a.bytecode_drift === true && (
                    <MetaBadge
                      meta={DRIFT_TRUE_META}
                      label="⚠ code changed"
                      title="Runtime bytecode at this impl changed since the audit was matched"
                    />
                  )}
                  {a.bytecode_drift === false && (
                    <MetaBadge
                      meta={DRIFT_FALSE_META}
                      label="✓ bytecode stable"
                      title="Runtime bytecode hash matches the hash captured at audit match time"
                    />
                  )}
                  {!driftKnown && a.bytecode_keccak_now && !a.bytecode_keccak_at_match && (
                    <span
                      className="ps-badge"
                      title="Anchor not set — refresh coverage to stamp runtime bytecode hash"
                      style={{
                        "--badge-accent": "#6b7590",
                        fontSize: 10,
                      }}
                    >
                      drift unverified
                    </span>
                  )}
                </div>
              </button>
            );
          })}
        </div>
      )}

      <LiveFindingsSection coverage={coverage} />
    </section>
  );
}
