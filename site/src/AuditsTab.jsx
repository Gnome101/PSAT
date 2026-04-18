import { useEffect, useMemo, useState } from "react";

import { listAudits, getCoverage, getScope } from "./api/audits.js";

// Status → chip colors. These are semantic: the in-repo .chip.alt / .chip.warn
// don't map cleanly to green/amber/red, so we set inline backgrounds for
// the cases where meaning is important and a wrong-color chip would mislead.
const CHIP_GREEN = { background: "#dcfce7", color: "#166534", borderColor: "#bbf7d0" };
const CHIP_AMBER = { background: "#fef3c7", color: "#92400e", borderColor: "#fde68a" };
const CHIP_RED = { background: "#fee2e2", color: "#991b1b", borderColor: "#fecaca" };
const CHIP_GRAY = { background: "#f1f5f9", color: "#475569", borderColor: "#e2e8f0" };
const CHIP_VIOLET = { background: "#ede9fe", color: "#5b21b6", borderColor: "#ddd6fe" };

function extractionStatusChip(status) {
  // status ∈ "success" | "processing" | "failed" | "skipped" | null
  if (status === "success") return { label: "ready", style: CHIP_GREEN };
  if (status === "processing") return { label: "processing", style: CHIP_AMBER };
  if (status === "failed") return { label: "failed", style: CHIP_RED };
  if (status === "skipped") return { label: "skipped", style: CHIP_GRAY };
  return { label: "pending", style: CHIP_GRAY };
}

function confidenceChip(confidence) {
  // numeric confidence 0.0-1.0. ≥0.8 green, ≥0.5 amber, else red.
  if (confidence == null) return { label: "unknown", style: CHIP_GRAY };
  const n = Number(confidence);
  if (Number.isNaN(n)) return { label: "unknown", style: CHIP_GRAY };
  const pct = `${Math.round(n * 100)}%`;
  if (n >= 0.8) return { label: pct, style: CHIP_GREEN };
  if (n >= 0.5) return { label: pct, style: CHIP_AMBER };
  return { label: pct, style: CHIP_RED };
}

function matchConfidenceStyle(conf) {
  if (conf === "high") return CHIP_GREEN;
  if (conf === "medium") return CHIP_AMBER;
  if (conf === "low") return CHIP_RED;
  return CHIP_GRAY;
}

function formatAuditDate(date) {
  if (!date) return "—";
  // date column is YYYY-MM-DD-ish but can also be free-text from extraction.
  const parsed = new Date(date);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
  }
  return String(date);
}

function formatSizeKb(bytes) {
  if (bytes == null) return null;
  if (bytes < 1024) return `${bytes} B`;
  return `${(bytes / 1024).toFixed(0)} KB`;
}

function AuditCard({ audit, highlight, onRegisterRef }) {
  const [scope, setScope] = useState(null);
  const [scopeLoading, setScopeLoading] = useState(false);
  const [scopeError, setScopeError] = useState(null);
  const [showScope, setShowScope] = useState(false);

  const textChip = extractionStatusChip(audit.text_extraction_status);
  const scopeChip = extractionStatusChip(audit.scope_extraction_status);
  const confChip = confidenceChip(audit.confidence);

  async function toggleScope() {
    if (showScope) {
      setShowScope(false);
      return;
    }
    setShowScope(true);
    if (scope || scopeLoading) return;
    setScopeLoading(true);
    setScopeError(null);
    try {
      const data = await getScope(audit.id);
      setScope(data);
    } catch (err) {
      setScopeError(err.message || String(err));
    } finally {
      setScopeLoading(false);
    }
  }

  return (
    <article
      className="card"
      ref={onRegisterRef}
      style={highlight ? { boxShadow: "0 0 0 2px #f59e0b" } : undefined}
    >
      <div className="card-header-row">
        <h3>{audit.auditor || "Unknown auditor"}</h3>
        <span className="chip" style={confChip.style}>confidence · {confChip.label}</span>
      </div>
      <p className="muted" style={{ marginTop: 4, marginBottom: 10 }}>{audit.title || "Untitled audit"}</p>

      <div className="kv-grid compact small">
        <div className="kv-row">
          <span className="key">Date</span>
          <span>{formatAuditDate(audit.date)}</span>
        </div>
        {audit.url ? (
          <div className="kv-row">
            <span className="key">Source</span>
            <span><a href={audit.url} target="_blank" rel="noreferrer noopener">open</a></span>
          </div>
        ) : null}
        {audit.text_size_bytes != null ? (
          <div className="kv-row">
            <span className="key">Text size</span>
            <span>{formatSizeKb(audit.text_size_bytes)}</span>
          </div>
        ) : null}
        {audit.scope_contract_count != null ? (
          <div className="kv-row">
            <span className="key">Scope size</span>
            <span>{audit.scope_contract_count} contracts</span>
          </div>
        ) : null}
      </div>

      <div className="chips" style={{ marginTop: 10 }}>
        <span className="chip" style={textChip.style}>text · {textChip.label}</span>
        <span className="chip" style={scopeChip.style}>scope · {scopeChip.label}</span>
      </div>

      <div className="chips" style={{ marginTop: 12 }}>
        {audit.pdf_url ? (
          <a href={audit.pdf_url} target="_blank" rel="noreferrer noopener" className="chip" style={{ ...CHIP_GRAY, textDecoration: "none" }}>
            PDF
          </a>
        ) : null}
        {audit.has_text ? (
          <a href={`/api/audits/${audit.id}/text`} target="_blank" rel="noreferrer noopener" className="chip" style={{ ...CHIP_GRAY, textDecoration: "none" }}>
            View text
          </a>
        ) : (
          <span className="chip" style={CHIP_GRAY} title="Text extraction not complete">
            text unavailable
          </span>
        )}
        {audit.has_scope ? (
          <button
            className="chip"
            style={{ ...CHIP_VIOLET, cursor: "pointer" }}
            onClick={toggleScope}
          >
            {showScope ? "Hide scope" : "View scope"}
          </button>
        ) : (
          <span className="chip" style={CHIP_GRAY} title="Scope extraction not complete">
            scope unavailable
          </span>
        )}
      </div>

      {showScope ? (
        <div className="subsection" style={{ marginTop: 12 }}>
          <div className="subsection-title">Scope</div>
          {scopeLoading ? (
            <p className="empty">Loading scope…</p>
          ) : scopeError ? (
            <p className="empty" style={{ color: "#991b1b" }}>Error: {scopeError}</p>
          ) : scope && scope.contracts?.length ? (
            <div className="chips">
              {scope.contracts.map((name) => (
                <span className="chip" key={name} style={CHIP_VIOLET}>{name}</span>
              ))}
            </div>
          ) : (
            <p className="empty">Scope is empty.</p>
          )}
        </div>
      ) : null}
    </article>
  );
}

export default function AuditsTab({ companyName, focusAuditId }) {
  const [audits, setAudits] = useState(null);
  const [coverage, setCoverage] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    setAudits(null);
    setCoverage(null);
    setError(null);
    Promise.all([listAudits(companyName), getCoverage(companyName)])
      .then(([a, c]) => {
        if (cancelled) return;
        setAudits(a);
        setCoverage(c);
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || String(err));
      });
    return () => { cancelled = true; };
  }, [companyName]);

  // Scroll the focused audit card into view after the list renders.
  const cardRefs = useMemo(() => new Map(), []);
  useEffect(() => {
    if (!focusAuditId || !audits?.audits?.length) return;
    const node = cardRefs.get(Number(focusAuditId));
    if (node && typeof node.scrollIntoView === "function") {
      node.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }, [focusAuditId, audits, cardRefs]);

  if (error) {
    return (
      <div className="page">
        <section className="panel">
          <p className="empty">Failed to load audits: {error}</p>
        </section>
      </div>
    );
  }

  if (!audits || !coverage) {
    return (
      <div className="page">
        <section className="panel"><p className="empty">Loading audits…</p></section>
      </div>
    );
  }

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Audits</p>
            <h2>{companyName}</h2>
          </div>
          <div className="chips">
            <span className="chip" style={CHIP_GRAY}>{audits.audit_count} audits</span>
          </div>
        </div>
        {audits.audits.length === 0 ? (
          <p className="empty" style={{ marginTop: 12 }}>No audits discovered yet for this protocol.</p>
        ) : (
          <div className="card-grid" style={{ marginTop: 12 }}>
            {audits.audits.map((audit) => (
              <AuditCard
                key={audit.id}
                audit={audit}
                highlight={Number(focusAuditId) === audit.id}
                onRegisterRef={(node) => {
                  if (node) cardRefs.set(audit.id, node);
                  else cardRefs.delete(audit.id);
                }}
              />
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <h3 style={{ marginBottom: 12 }}>Contract Coverage</h3>
        {coverage.coverage.length === 0 ? (
          <p className="empty">No contracts in this protocol yet.</p>
        ) : (
          <div className="runs-table">
            <div className="runs-table-header">
              <span style={{ flex: 2 }}>Contract</span>
              <span style={{ flex: 3 }}>Address</span>
              <span style={{ flex: 1 }}>Status</span>
              <span style={{ flex: 2 }}>Latest audit</span>
            </div>
            {[...coverage.coverage].sort((a, b) => {
              // Analyzed (named) contracts first so the useful part of
              // the table is at the top. Within each bucket keep the
              // server's order.
              const aNamed = !!(a.contract_name && String(a.contract_name).trim());
              const bNamed = !!(b.contract_name && String(b.contract_name).trim());
              if (aNamed !== bNamed) return aNamed ? -1 : 1;
              // Within named, audits-having first, then alphabetical by name.
              if (aNamed) {
                if (!!a.audit_count !== !!b.audit_count) return a.audit_count ? -1 : 1;
                return String(a.contract_name).localeCompare(String(b.contract_name));
              }
              return 0;
            }).map((row) => {
              const last = row.last_audit;
              const hasName = !!(row.contract_name && String(row.contract_name).trim());
              const hasOpen = (row.audits || []).some(
                (a) => a.covered_to_block == null && (a.match_confidence === "high" || a.match_type === "reviewed_commit"),
              );
              const status = !hasName
                ? { label: "pending analysis", style: CHIP_GRAY }
                : !row.audit_count
                  ? { label: "no audit", style: CHIP_RED }
                  : hasOpen
                    ? { label: "covered", style: CHIP_GREEN }
                    : { label: "before upgrade", style: CHIP_AMBER };
              return (
                <div key={row.address} className="runs-table-row" style={{ cursor: "default" }}>
                  <span className="runs-cell-name" style={{ flex: 2 }}>{row.contract_name || <span className="muted">—</span>}</span>
                  <span className="mono runs-cell-addr" style={{ flex: 3 }}>{row.address}</span>
                  <span style={{ flex: 1 }}>
                    <span className="chip" style={status.style}>{status.label}</span>
                  </span>
                  <span style={{ flex: 2 }}>
                    {last ? (
                      <>
                        <strong>{last.auditor || "—"}</strong>
                        <span className="muted" style={{ marginLeft: 6, fontSize: 12 }}>{formatAuditDate(last.date)}</span>
                        <span
                          className="chip"
                          style={{ ...matchConfidenceStyle(last.match_confidence), marginLeft: 6, fontSize: 10, padding: "2px 6px" }}
                        >
                          {last.match_type} · {last.match_confidence}
                        </span>
                      </>
                    ) : (
                      <span className="muted" style={{ fontSize: 12 }}>—</span>
                    )}
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </section>
    </div>
  );
}
