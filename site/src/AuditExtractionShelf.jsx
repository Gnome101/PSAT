import { useEffect, useMemo, useState } from "react";

import { getPipeline } from "./api/audits.js";

const AUDIT_COLORS = {
  text_extraction: "#0891b2",
  scope_extraction: "#7c3aed",
};

const AUDIT_TITLES = {
  text_extraction: "Text Extraction",
  scope_extraction: "Scope Extraction",
};

function formatElapsedSeconds(s) {
  if (s == null) return "—";
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function shortLabel(str, max = 48) {
  if (!str) return "";
  return str.length > max ? `${str.slice(0, max - 1)}…` : str;
}

function WorkerCard({ stage, bucket, onOpenCompany }) {
  const [expandedFail, setExpandedFail] = useState(null);
  const color = AUDIT_COLORS[stage];
  const title = AUDIT_TITLES[stage];
  const processing = bucket.processing || [];
  const pending = bucket.pending || [];
  const failed = bucket.failed || [];

  return (
    <article className="monitor-stage-card">
      <div className="monitor-stage-card-header">
        <div className="monitor-stage-heading">
          <span className="monitor-stage-name" style={{ color }}>{title}</span>
          <span className="monitor-stage-subtitle">
            {processing.length} processing · {pending.length} pending
            {failed.length ? ` · ${failed.length} failed` : ""}
          </span>
        </div>
        <span className="chip" style={{ background: `${color}18`, color, borderColor: `${color}33` }}>
          {processing.length ? "live" : pending.length ? "queued" : failed.length ? "recent failures" : "idle"}
        </span>
      </div>

      <div className="monitor-stage-list">
        {processing.slice(0, 3).map((item) => (
          <button
            className="monitor-stage-item"
            key={`proc-${item.audit_id}`}
            onClick={() => onOpenCompany && onOpenCompany(item)}
            style={{ textAlign: "left", width: "100%", cursor: onOpenCompany ? "pointer" : "default" }}
          >
            <div className="monitor-stage-item-top">
              <span className="monitor-stage-item-name">{shortLabel(`${item.auditor || "Unknown"} — ${item.title || ""}`)}</span>
              <span className="monitor-stage-item-time">
                {/* existing monitor uses formatElapsed(ms); elapsed_seconds is
                    already in seconds so convert to the same label format. */}
                <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                  <svg width="10" height="10" viewBox="0 0 10 10" style={{ display: "inline-block" }}>
                    <circle cx="5" cy="5" r="4" fill={color}>
                      <animate attributeName="opacity" values="1;0.35;1" dur="1.4s" repeatCount="indefinite" />
                    </circle>
                  </svg>
                  {formatElapsedSeconds(item.elapsed_seconds)}
                </span>
              </span>
            </div>
            <div className="monitor-stage-item-meta">for {item.company || "unknown protocol"}</div>
            <div className="monitor-stage-item-detail">
              Working…
              {item.worker_id ? <span className="muted" style={{ marginLeft: 6, fontSize: 11 }}>({item.worker_id})</span> : null}
            </div>
          </button>
        ))}
        {processing.length > 3 ? (
          <div className="monitor-stage-more">+{processing.length - 3} more processing</div>
        ) : null}

        {pending.slice(0, 3).map((item) => (
          <div className="monitor-stage-item" key={`pend-${item.audit_id}`} style={{ opacity: 0.85 }}>
            <div className="monitor-stage-item-top">
              <span className="monitor-stage-item-name">{shortLabel(`${item.auditor || "Unknown"} — ${item.title || ""}`)}</span>
              <span className="monitor-stage-item-time">queued</span>
            </div>
            <div className="monitor-stage-item-meta">for {item.company || "unknown protocol"}</div>
          </div>
        ))}
        {pending.length > 3 ? (
          <div className="monitor-stage-more">+{pending.length - 3} more waiting</div>
        ) : null}

        {failed.length ? (
          <div className="monitor-stage-more" style={{ marginTop: 8, fontWeight: 700, color: "#991b1b" }}>
            Recent failures ({failed.length})
          </div>
        ) : null}
        {failed.slice(0, 5).map((item) => (
          <div
            key={`fail-${item.audit_id}`}
            className="monitor-stage-item"
            style={{
              background: "rgba(239,68,68,0.06)",
              borderColor: "rgba(239,68,68,0.22)",
              cursor: "pointer",
            }}
            onClick={() => setExpandedFail(expandedFail === item.audit_id ? null : item.audit_id)}
          >
            <div className="monitor-stage-item-top">
              <span className="monitor-stage-item-name">{shortLabel(`${item.auditor || "Unknown"} — ${item.title || ""}`)}</span>
              <span className="monitor-stage-item-time" style={{ color: "#991b1b" }}>failed</span>
            </div>
            <div className="monitor-stage-item-meta">for {item.company || "unknown protocol"}</div>
            {expandedFail === item.audit_id ? (
              <pre className="pre-wrap" style={{
                margin: "6px 0 0",
                padding: 8,
                background: "rgba(239,68,68,0.08)",
                color: "#7f1d1d",
                fontSize: 11,
                borderRadius: 8,
                maxHeight: 200,
                overflow: "auto",
              }}>
                {item.error || "No error details recorded."}
              </pre>
            ) : (
              <div className="monitor-stage-item-detail" style={{ color: "#991b1b", fontSize: 12 }}>
                {shortLabel(item.error || "(no error string)", 72)}
              </div>
            )}
          </div>
        ))}

        {!processing.length && !pending.length && !failed.length ? (
          <div className="monitor-stage-more">No active extractions.</div>
        ) : null}
      </div>
    </article>
  );
}

export default function AuditExtractionShelf() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function fetchOnce() {
      try {
        const d = await getPipeline();
        if (!cancelled) { setData(d); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err.message || String(err));
      }
    }
    fetchOnce();
    const timer = setInterval(fetchOnce, 2500);
    return () => { cancelled = true; clearInterval(timer); };
  }, []);

  const totals = useMemo(() => {
    if (!data) return null;
    const t = {};
    for (const stage of ["text_extraction", "scope_extraction"]) {
      const b = data[stage] || {};
      t[stage] = {
        processing: (b.processing || []).length,
        pending: (b.pending || []).length,
        failed: (b.failed || []).length,
      };
    }
    return t;
  }, [data]);

  if (error) {
    return (
      <section className="panel" style={{ marginTop: 16 }}>
        <p className="empty" style={{ color: "#991b1b" }}>Audit pipeline error: {error}</p>
      </section>
    );
  }

  if (!data) {
    return (
      <section className="panel" style={{ marginTop: 16 }}>
        <p className="empty">Loading audit extraction status…</p>
      </section>
    );
  }

  function openCompanyAudit(item) {
    if (!item?.company) return;
    const suffix = item.audit_id ? `?audit=${item.audit_id}` : "";
    window.history.pushState({}, "", `/company/${encodeURIComponent(item.company)}/audits${suffix}`);
    // handlePopState in App.jsx re-parses the URL into viewMode / companyTab.
    window.dispatchEvent(new PopStateEvent("popstate"));
  }

  return (
    <section className="panel" style={{ marginTop: 16 }}>
      <div className="panel-header">
        <div>
          <p className="eyebrow">Audit Extraction</p>
          <h2>
            {totals
              ? `${totals.text_extraction.processing + totals.scope_extraction.processing} running`
              : "Running"}
          </h2>
        </div>
        <div className="chips">
          <span className="chip" style={{ background: "#ecfeff", color: AUDIT_COLORS.text_extraction }}>
            text · {totals.text_extraction.processing}/{totals.text_extraction.pending}/{totals.text_extraction.failed}
          </span>
          <span className="chip" style={{ background: "#f5f3ff", color: AUDIT_COLORS.scope_extraction }}>
            scope · {totals.scope_extraction.processing}/{totals.scope_extraction.pending}/{totals.scope_extraction.failed}
          </span>
        </div>
      </div>
      <div className="monitor-stage-grid">
        {["text_extraction", "scope_extraction"].map((stage) => (
          <WorkerCard
            key={stage}
            stage={stage}
            bucket={data[stage] || {}}
            onOpenCompany={openCompanyAudit}
          />
        ))}
      </div>
    </section>
  );
}
