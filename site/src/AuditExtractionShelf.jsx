import { useEffect, useMemo, useState } from "react";

import { getPipeline } from "./api/audits.js";

const AUDIT_STAGES = [
  {
    key: "text",
    title: "Text Extraction",
    color: "#0891b2",
    description: "Download the report, extract raw text, and store it for later stages.",
  },
  {
    key: "scope",
    title: "Scope Extraction",
    color: "#7c3aed",
    description: "Pull contracts, address pins, reviewed commits, and repo references from the text.",
  },
  {
    key: "coverage",
    title: "Coverage Refresh",
    color: "#d97706",
    description: "Re-match the audit against live contracts and run source-equivalence checks automatically.",
  },
];

const LIVE_BUCKET_ORDER = [
  ["scope_extraction", "processing"],
  ["text_extraction", "processing"],
  ["scope_extraction", "failed"],
  ["text_extraction", "failed"],
  ["scope_extraction", "pending"],
  ["text_extraction", "pending"],
];

const DISPLAY_ROW_LIMIT = 14;

function formatElapsedSeconds(s) {
  if (s == null) return "—";
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function formatBytes(bytes) {
  if (bytes == null) return null;
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes;
  let unitIndex = -1;
  do {
    value /= 1024;
    unitIndex += 1;
  } while (value >= 1024 && unitIndex < units.length - 1);
  const precision = value >= 10 || unitIndex === units.length - 1 ? 0 : 1;
  return `${value.toFixed(precision)} ${units[unitIndex]}`;
}

function shortLabel(str, max = 64) {
  if (!str) return "";
  return str.length > max ? `${str.slice(0, max - 1)}…` : str;
}

function pluralize(count, singular, plural = `${singular}s`) {
  return count === 1 ? singular : plural;
}

function buildAuditRows(data) {
  const rows = [];
  const seen = new Set();

  for (const [stage, bucket] of LIVE_BUCKET_ORDER) {
    for (const item of data?.[stage]?.[bucket] || []) {
      if (seen.has(item.audit_id)) continue;
      seen.add(item.audit_id);
      rows.push({ ...item, live_stage: stage, live_bucket: bucket });
    }
  }

  return rows;
}

function currentStatusLabel(item) {
  const stage = item.live_stage === "scope_extraction" ? "scope" : "text";
  const bucket = item.live_bucket === "processing" ? "running" : item.live_bucket;
  return `${stage} ${bucket}`;
}

function buildGuideSummary(stageKey, totals) {
  if (!totals) return "loading";
  if (stageKey === "coverage") return "automatic after scope";

  const bucket = stageKey === "text" ? totals.text_extraction : totals.scope_extraction;
  const parts = [];
  if (bucket.processing) parts.push(`${bucket.processing} running`);
  if (bucket.pending) parts.push(`${bucket.pending} queued`);
  if (bucket.failed) parts.push(`${bucket.failed} failed`);
  return parts.length ? parts.join(" · ") : "idle";
}

function buildFactChips(item) {
  const facts = [];
  if (item.date) facts.push(item.date);
  if (item.text_size_bytes != null) facts.push(`${formatBytes(item.text_size_bytes)} text`);
  if (item.scope_contract_count) {
    facts.push(`${item.scope_contract_count} ${pluralize(item.scope_contract_count, "contract")}`);
  }
  if (item.scope_entry_count) {
    facts.push(`${item.scope_entry_count} ${pluralize(item.scope_entry_count, "address pin")}`);
  }
  if (item.reviewed_commit_count) {
    facts.push(`${item.reviewed_commit_count} reviewed ${pluralize(item.reviewed_commit_count, "SHA")}`);
  }
  if (item.referenced_repo_count) {
    facts.push(`${item.referenced_repo_count} ${pluralize(item.referenced_repo_count, "repo")}`);
  }
  if (item.classified_commit_count) {
    facts.push(`${item.classified_commit_count} commit ${pluralize(item.classified_commit_count, "label")}`);
  }
  return facts;
}

function runningDetail(item) {
  if (item.worker_id && item.elapsed_seconds != null) {
    return `${item.worker_id} · ${formatElapsedSeconds(item.elapsed_seconds)}`;
  }
  if (item.worker_id) return item.worker_id;
  if (item.elapsed_seconds != null) return formatElapsedSeconds(item.elapsed_seconds);
  return "In progress";
}

function textReadyDetail(item) {
  if (item.text_size_bytes != null) return `${formatBytes(item.text_size_bytes)} extracted`;
  if (item.text_extracted_at) return "Extracted and stored";
  return "Text ready for scope extraction";
}

function stageSnapshot(item, stageKey) {
  if (stageKey === "text") {
    if (item.live_stage === "text_extraction") {
      if (item.live_bucket === "processing") {
        return { status: "running", badge: "Running", headline: "Extracting text", detail: runningDetail(item) };
      }
      if (item.live_bucket === "pending") {
        return { status: "queued", badge: "Queued", headline: "Waiting for worker", detail: "Ready to claim" };
      }
      return {
        status: "failed",
        badge: "Failed",
        headline: "Text extraction failed",
        detail: shortLabel(item.error || "No error details recorded.", 88),
      };
    }
    return { status: "done", badge: "Done", headline: "Text ready", detail: textReadyDetail(item) };
  }

  if (stageKey === "scope") {
    if (item.live_stage === "text_extraction") {
      if (item.live_bucket === "failed") {
        return {
          status: "blocked",
          badge: "Blocked",
          headline: "Waiting on text",
          detail: "Scope extraction cannot start until text succeeds",
        };
      }
      return {
        status: "waiting",
        badge: "Waiting",
        headline: "Next stage",
        detail:
          item.live_bucket === "processing"
            ? "Starts as soon as text extraction finishes"
            : "Queued behind text extraction",
      };
    }
    if (item.live_bucket === "processing") {
      return { status: "running", badge: "Running", headline: "Parsing scope", detail: runningDetail(item) };
    }
    if (item.live_bucket === "pending") {
      return { status: "queued", badge: "Queued", headline: "Waiting for worker", detail: textReadyDetail(item) };
    }
    return {
      status: "failed",
      badge: "Failed",
      headline: "Scope extraction failed",
      detail: shortLabel(item.error || "No error details recorded.", 88),
    };
  }

  if (item.live_stage === "scope_extraction") {
    if (item.live_bucket === "failed") {
      return {
        status: "blocked",
        badge: "Blocked",
        headline: "Waiting on scope",
        detail: "Coverage refresh only runs after scope extraction succeeds",
      };
    }
    return {
      status: "auto",
      badge: "Auto",
      headline: "Coverage refresh",
      detail:
        item.live_bucket === "processing"
          ? "Runs immediately after scope extraction finishes"
          : "Queued behind scope extraction",
    };
  }

  if (item.live_bucket === "failed") {
    return {
      status: "blocked",
      badge: "Blocked",
      headline: "Not reachable yet",
      detail: "Coverage needs both text and scope extraction",
    };
  }
  return {
    status: "waiting",
    badge: "Later",
    headline: "Later stage",
    detail: "Coverage runs after text and scope extraction",
  };
}

function PipelineStage({ stage, snapshot }) {
  return (
    <div className={`audit-pipeline-stage ${snapshot.status}`}>
      <div className="audit-pipeline-stage-top">
        <span className="audit-pipeline-stage-label">
          <span className="audit-pipeline-stage-dot" style={{ background: stage.color }} />
          {stage.title}
        </span>
        <span className={`audit-pipeline-stage-badge ${snapshot.status}`}>{snapshot.badge}</span>
      </div>
      <div className="audit-pipeline-stage-headline">{snapshot.headline}</div>
      <div className="audit-pipeline-stage-detail">{snapshot.detail}</div>
    </div>
  );
}

function AuditRow({ item, onOpenCompany }) {
  const facts = buildFactChips(item);

  return (
    <button type="button" className="audit-pipeline-row buttonlike" onClick={() => onOpenCompany(item)}>
      <div className="audit-pipeline-row-meta">
        <div className="audit-pipeline-row-meta-top">
          <span className="audit-pipeline-row-title">
            {item.auditor || "Unknown auditor"} — {item.title || "Untitled audit"}
          </span>
          <span className={`audit-pipeline-live-pill ${item.live_bucket}`}>{currentStatusLabel(item)}</span>
        </div>
        <div className="audit-pipeline-row-company">for {item.company || "unknown protocol"}</div>
        {facts.length ? (
          <div className="audit-pipeline-row-facts">
            {facts.map((fact) => (
              <span key={fact} className="audit-pipeline-fact">
                {fact}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="audit-pipeline-track">
        {AUDIT_STAGES.map((stage) => (
          <PipelineStage key={stage.key} stage={stage} snapshot={stageSnapshot(item, stage.key)} />
        ))}
      </div>
    </button>
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
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError(err.message || String(err));
      }
    }

    fetchOnce();
    const timer = setInterval(fetchOnce, 2500);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  const totals = useMemo(() => {
    if (!data) return null;
    const t = {};
    for (const stage of ["text_extraction", "scope_extraction"]) {
      const bucket = data[stage] || {};
      t[stage] = {
        processing: (bucket.processing || []).length,
        pending: (bucket.pending || []).length,
        failed: (bucket.failed || []).length,
      };
    }
    return t;
  }, [data]);

  const rows = useMemo(() => buildAuditRows(data), [data]);
  const visibleRows = rows.slice(0, DISPLAY_ROW_LIMIT);
  const runningCount =
    (totals?.text_extraction.processing || 0) + (totals?.scope_extraction.processing || 0);
  const queuedCount = (totals?.text_extraction.pending || 0) + (totals?.scope_extraction.pending || 0);
  const failedCount = (totals?.text_extraction.failed || 0) + (totals?.scope_extraction.failed || 0);

  function openCompanyAudit(item) {
    if (!item?.company) return;
    const suffix = item.audit_id ? `?audit=${item.audit_id}` : "";
    window.history.pushState({}, "", `/company/${encodeURIComponent(item.company)}/audits${suffix}`);
    window.dispatchEvent(new PopStateEvent("popstate"));
  }

  if (error) {
    return (
      <section className="panel" style={{ marginTop: 16 }}>
        <p className="empty" style={{ color: "#991b1b" }}>
          Audit pipeline error: {error}
        </p>
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

  return (
    <section className="panel" style={{ marginTop: 16 }}>
      <div className="panel-header">
        <div>
          <p className="eyebrow">Audit Extraction</p>
          <h2>{runningCount || queuedCount ? `${runningCount} running · ${queuedCount} queued` : "Pipeline idle"}</h2>
          <p className="audit-pipeline-subtitle">
            This view is linear: text first, then scope, then an automatic coverage refresh.
          </p>
        </div>
        <div className="chips">
          <span className="chip" style={{ background: "#ecfeff", color: "#0891b2" }}>
            text · {totals.text_extraction.processing}/{totals.text_extraction.pending}/{totals.text_extraction.failed}
          </span>
          <span className="chip" style={{ background: "#f5f3ff", color: "#7c3aed" }}>
            scope · {totals.scope_extraction.processing}/{totals.scope_extraction.pending}/{totals.scope_extraction.failed}
          </span>
          <span className="chip" style={{ background: "#fff7ed", color: "#c2410c" }}>
            failures · {failedCount}
          </span>
        </div>
      </div>

      <div className="audit-pipeline-guide">
        {AUDIT_STAGES.map((stage) => (
          <article key={stage.key} className="audit-pipeline-guide-card">
            <div className="audit-pipeline-guide-top">
              <span className="audit-pipeline-stage-label">
                <span className="audit-pipeline-stage-dot" style={{ background: stage.color }} />
                {stage.title}
              </span>
              <span className="audit-pipeline-guide-summary">{buildGuideSummary(stage.key, totals)}</span>
            </div>
            <p>{stage.description}</p>
          </article>
        ))}
      </div>

      {visibleRows.length ? (
        <div className="audit-pipeline-list">
          {visibleRows.map((item) => (
            <AuditRow key={`${item.live_stage}-${item.live_bucket}-${item.audit_id}`} item={item} onOpenCompany={openCompanyAudit} />
          ))}
        </div>
      ) : (
        <div className="audit-pipeline-empty">
          No active, queued, or recently failed audits. Completed rows drop out once extraction succeeds.
        </div>
      )}

      {rows.length > DISPLAY_ROW_LIMIT ? (
        <div className="audit-pipeline-note">Showing {DISPLAY_ROW_LIMIT} of {rows.length} live pipeline rows.</div>
      ) : null}
      <div className="audit-pipeline-note">
        Only active, queued, and recent failed rows appear here. Coverage refresh runs automatically after scope
        extraction succeeds.
      </div>
    </section>
  );
}
