import React, { useEffect, useMemo, useRef, useState } from "react";

import {
  ADDRESS_GRAPH_COLUMNS,
  PRINCIPAL_COLUMNS,
  buildVisualAddressGraph,
  buildVisualPermissionGraph,
  layoutVisualAddressGraph,
  layoutVisualPermissionGraph,
  prettyFunctionName,
  shortenAddress,
  wrapText,
} from "./graph.js";
import DependencyGraphTab from "./DependencyGraphTab.jsx";
import ProtocolGraph from "./ProtocolGraph.jsx";
import RiskSurface from "./RiskSurface.jsx";
import ProtocolSurface from "./ProtocolSurface.jsx";
import { matchesEra } from "./auditMatching.js";
import {
  AUDIT_STATUS_META,
  DRIFT_FALSE_META,
  DRIFT_TRUE_META,
  EQUIVALENCE_META,
  formatAuditDate,
  formatAuditTimestamp,
  MATCH_TYPE_META,
  MetaBadge,
  proofKindTitle,
  PROOF_KIND_META,
} from "./auditUi.jsx";
import AuditsTab from "./AuditsTab.jsx";
import AuditExtractionShelf from "./AuditExtractionShelf.jsx";
import { api } from "./api/client.js";
import { getPipeline as getAuditPipeline } from "./api/audits.js";
import ProductHero from "./ProductHero.jsx";
// Shelved assembly-line hero — kept on disk, not rendered.
// import SplashHero from "./SplashHero.jsx";
// import AssemblyLine from "./AssemblyLine.jsx";
import ProtocolLogo from "./ProtocolLogo.jsx";
import ProtocolRadar from "./ProtocolRadar.jsx";
import AddressesModal from "./AddressesModal.jsx";
import AuditsAdminModal from "./AuditsAdminModal.jsx";
// SurfacePreview was a static SVG mini-map; we now embed the real
// ProtocolSurface component inline. File kept for possible reuse.
// import SurfacePreview from "./SurfacePreview.jsx";

// TODO: replace this with a real sign-in page + session-based auth. Options
// that fit our Fly deployment: (a) an identity-aware proxy sidecar such as
// oauth2-proxy or Pomerium that authenticates real users (Google/GitHub SSO)
// and injects X-PSAT-Admin-Key server-side so the key never touches a browser,
// or (b) an app-level user system with per-user login + roles (fastapi-users,
// a managed provider like WorkOS/Clerk, etc.). The window.prompt +
// localStorage pattern in api/client.js is a stopgap so admins can click
// buttons during local dev and early prod — a shared-secret bearer token
// sitting in every admin's browser, with no per-user audit log and no
// revocation story beyond rotating the key and logging everyone out.

const TABS = ["summary", "permissions", "principals", "graph", "dependencies", "upgrades", "raw"];
const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;
const BLOCK_NUMBER_FORMAT = new Intl.NumberFormat("en-US");

function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <div className="eyebrow">{label}</div>
      <div className="stat">{value}</div>
    </div>
  );
}

function formatJson(value) {
  return JSON.stringify(value, null, 2);
}

function normalizeTab(tab) {
  return TABS.includes(tab) ? tab : "summary";
}

function isAddress(value) {
  return ADDRESS_RE.test(String(value || "").trim());
}

function parseLocationPath(pathname) {
  const segments = String(pathname || "/")
    .split("/")
    .filter(Boolean)
    .map((segment) => decodeURIComponent(segment));

  if (!segments.length) {
    return { mode: "default", value: null, tab: "summary" };
  }

  if (segments[0] === "monitor") {
    return { mode: "monitor", value: null, tab: "summary" };
  }

  if (segments[0] === "company" && segments[1]) {
    const validCompanyTabs = ["overview", "surface", "graph", "risk", "monitoring", "audits"];
    const companyTab = validCompanyTabs.includes(segments[2]) ? segments[2] : "overview";
    return { mode: "company", value: segments[1], tab: "summary", companyTab };
  }

  if (segments[0] === "proxies") {
    return { mode: "proxies", value: null, tab: "summary" };
  }

  if (segments[0] === "runs" && segments[1]) {
    return {
      mode: "run",
      value: segments[1],
      tab: normalizeTab(segments[2]),
    };
  }

  if (segments[0] === "address" && segments[1] && isAddress(segments[1])) {
    return {
      mode: "address",
      value: segments[1],
      tab: normalizeTab(segments[2]),
    };
  }

  if (isAddress(segments[0])) {
    return {
      mode: "address",
      value: segments[0],
      tab: normalizeTab(segments[1]),
    };
  }

  return { mode: "default", value: null, tab: "summary" };
}

function buildLocationPath(runId, address, tab) {
  const nextTab = normalizeTab(tab);
  if (isAddress(address)) {
    return `/address/${String(address).trim()}/${nextTab}`;
  }
  if (runId) {
    return `/runs/${encodeURIComponent(runId)}/${nextTab}`;
  }
  return "/";
}


function renderNodeBody(node) {
  const titleLines = wrapText(node.title, node.shape === "rect" ? 30 : 18, node.shape === "rect" ? 3 : 3);
  const subtitleLines = wrapText(node.subtitle, node.shape === "rect" ? 42 : 18, node.shape === "rect" ? 4 : 3);
  const metaLines = wrapText(node.meta, node.shape === "rect" ? 42 : 18, 2);
  const centerX = node.width / 2;
  const centerY = node.height / 2;
  const titleStartY = node.shape === "rect" ? 30 : centerY - 28;
  const subtitleStartY = titleStartY + titleLines.length * 17 + 12;
  const metaStartY = subtitleStartY + subtitleLines.length * 15 + 14;

  const renderBlock = (lines, className, startY) =>
    lines.map((line, index) => (
      <text key={`${className}-${index}`} className={className} x="0" y={startY + index * 15} textAnchor="middle">
        {line}
      </text>
    ));

  return (
    <>
      <g className="graph-svg-shape" fill={node.fill} stroke={node.stroke} strokeWidth="3">
        {node.shape === "circle" ? (
          <ellipse cx={centerX} cy={centerY} rx={node.width / 2} ry={node.height / 2} />
        ) : (
          <rect x="0" y="0" width={node.width} height={node.height} rx="22" ry="22" />
        )}
      </g>
      <g transform={`translate(${centerX}, ${titleStartY})`} fill={node.text}>
        {renderBlock(titleLines, "graph-svg-title", 0)}
        {renderBlock(subtitleLines, "graph-svg-subtitle", subtitleLines.length ? subtitleStartY - titleStartY : 0)}
        {renderBlock(metaLines, "graph-svg-meta", metaLines.length ? metaStartY - titleStartY : 0)}
      </g>
    </>
  );
}

function edgeSlotPercents(count) {
  if (count <= 1) {
    return [50];
  }
  const start = 18;
  const end = 82;
  const step = (end - start) / (count - 1);
  return Array.from({ length: count }, (_, index) => start + index * step);
}

function SummaryTab({ detail }) {
  const summary = detail?.contract_analysis?.summary || detail?.summary || {};
  const subject = detail?.contract_analysis?.subject || {};
  const standards = summary.standards || [];
  return (
    <div className="stack">
      <div className="summary-grid">
        <StatCard label="Contract" value={displayName(detail) || subject.name || "Unknown"} />
        <StatCard label="Control Model" value={summary.control_model || "unknown"} />
        <StatCard label="Risk" value={summary.static_risk_level || "unknown"} />
        <StatCard label="Standards" value={standards.length || 0} />
      </div>
      <div className="card">
        <h3>Summary</h3>
        <div className="kv-grid">
          <div className="kv-row">
            <span className="key">Address</span>
            <span className="mono">{detail?.address || subject.address || "Unknown"}</span>
          </div>
          <div className="kv-row">
            <span className="key">Upgradeable</span>
            <span>{String(Boolean(summary.is_upgradeable))}</span>
          </div>
          <div className="kv-row">
            <span className="key">Pausable</span>
            <span>{String(Boolean(summary.is_pausable))}</span>
          </div>
          <div className="kv-row">
            <span className="key">Timelock</span>
            <span>{String(Boolean(summary.has_timelock))}</span>
          </div>
          <div className="kv-row">
            <span className="key">Standards</span>
            <span>{standards.join(", ") || "None"}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function PermissionsTab({ detail }) {
  const payload = detail?.effective_permissions;
  if (!payload?.functions?.length) {
    return <p className="empty">No permission artifact available.</p>;
  }
  return (
    <div className="card-grid">
      {payload.functions.map((entry) => {
        const principals = [
          ...(entry.direct_owner?.address ? [entry.direct_owner] : []),
          ...(entry.authority_roles || []).flatMap((role) => role.principals || []),
          ...(entry.controllers || []).flatMap((controller) => controller.principals || []),
        ];
        return (
          <article className="card" key={entry.selector}>
            <div className="card-header-row">
              <h3>{prettyFunctionName(entry.function)}</h3>
              <span className="chip alt">{(entry.effect_labels || []).join(" · ") || "permissioned"}</span>
            </div>
            <p className="muted">{entry.action_summary}</p>
            <div className="kv-grid compact">
              <div className="kv-row">
                <span className="key">Authority public</span>
                <span>{entry.authority_public ? "Yes" : "No"}</span>
              </div>
              <div className="kv-row">
                <span className="key">Direct owner</span>
                <span>{entry.direct_owner?.address ? shortenAddress(entry.direct_owner.address) : "None"}</span>
              </div>
              <div className="kv-row">
                <span className="key">Effect targets</span>
                <span>{(entry.effect_targets || []).join(", ") || "None"}</span>
              </div>
            </div>
            <div className="subsection">
              <div className="subsection-title">Current principals</div>
              <div className="chips">
                {principals.length
                  ? principals.map((principal) => (
                      <span className="chip" key={`${entry.selector}-${principal.address}`}>
                        {shortenAddress(principal.address)}
                      </span>
                    ))
                  : <span className="chip warn">No principals resolved in artifact</span>}
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}

function PrincipalsTab({ detail }) {
  const payload = detail?.principal_labels;
  if (!payload?.principals?.length) {
    return <p className="empty">No principal labels available.</p>;
  }
  return (
    <div className="card-grid">
      {payload.principals.map((principal) => (
        <article className="card" key={principal.address}>
          <div className="card-header-row">
            <h3>{principal.display_name || shortenAddress(principal.address)}</h3>
            <span className="chip alt">{principal.resolved_type}</span>
          </div>
          <div className="mono muted">{principal.address}</div>
          <div className="chips" style={{ marginTop: 12 }}>
            {(principal.labels || []).map((label) => (
              <span className="chip" key={label}>
                {label}
              </span>
            ))}
          </div>
          {principal.permissions?.length ? (
            <div className="subsection">
              <div className="subsection-title">Permissions</div>
              <div className="chips">
                {principal.permissions.map((permission, index) => (
                  <span className="chip" key={`${principal.address}-${index}`}>
                    {prettyFunctionName(permission.function)}
                    {permission.role != null ? ` · role ${permission.role}` : ""}
                  </span>
                ))}
              </div>
            </div>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function formatCoverageWindow(coverage) {
  const fromBlock = coverage?.covered_from_block;
  const toBlock = coverage?.covered_to_block;
  if (fromBlock == null && toBlock == null) return null;
  if (fromBlock != null && toBlock != null) {
    return `blocks ${BLOCK_NUMBER_FORMAT.format(fromBlock)}-${BLOCK_NUMBER_FORMAT.format(toBlock)}`;
  }
  if (fromBlock != null) {
    return `from block ${BLOCK_NUMBER_FORMAT.format(fromBlock)}`;
  }
  return `through block ${BLOCK_NUMBER_FORMAT.format(toBlock)}`;
}

function coverageVerificationLabel(coverage) {
  const stamp = formatAuditTimestamp(coverage?.verified_at || coverage?.equivalence_checked_at);
  if (!stamp) return null;
  return `${coverage?.equivalence_status === "proven" ? "verified" : "checked"} ${stamp}`;
}

function coverageSummary(coverage) {
  if (coverage?.proof_kind === "pre_fix_unpatched") {
    return proofKindTitle(coverage.proof_kind);
  }
  if (coverage?.equivalence_status === "proven" && coverage?.equivalence_reason) {
    return `Source proof: ${coverage.equivalence_reason}`;
  }
  if (coverage?.proof_kind === "post_fix" || coverage?.proof_kind === "cited_only") {
    return proofKindTitle(coverage.proof_kind);
  }
  if (coverage?.equivalence_reason) {
    return coverage.equivalence_reason;
  }
  if (coverage?.match_type === "reviewed_address") {
    return "The audit scope explicitly pinned this deployed address.";
  }
  return null;
}

function UpgradeAuditCard({ coverage, companyName }) {
  const matchMeta = MATCH_TYPE_META[coverage.match_type] || MATCH_TYPE_META.direct;
  const driftKnown = coverage.bytecode_drift === true || coverage.bytecode_drift === false;
  const verificationLabel = coverageVerificationLabel(coverage);
  const blockWindow = formatCoverageWindow(coverage);
  const note = coverageSummary(coverage);
  const findings = coverage.live_findings || [];
  const findingTitle = findings
    .slice(0, 3)
    .map((finding) => finding?.title)
    .filter(Boolean)
    .join(" • ");
  const href = companyName
    ? `/company/${encodeURIComponent(companyName)}/audits?audit=${encodeURIComponent(coverage.audit_id)}`
    : null;

  const content = (
    <>
      <div className="upgrade-audit-card-top">
        <div className="upgrade-audit-card-auditor">{coverage.auditor || "Unknown"}</div>
        <div className="upgrade-audit-card-date">{formatAuditDate(coverage.date)}</div>
      </div>
      <div className="upgrade-audit-card-title">{coverage.title || ""}</div>
      <div className="ps-inspector-badges upgrade-audit-card-badges">
        <MetaBadge meta={matchMeta} />
        {coverage.match_confidence ? (
          <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
            {coverage.match_confidence}
          </span>
        ) : null}
        {coverage.equivalence_status && coverage.equivalence_status !== "proven" && EQUIVALENCE_META[coverage.equivalence_status] ? (
          <MetaBadge
            meta={EQUIVALENCE_META[coverage.equivalence_status]}
            title={coverage.equivalence_reason || ""}
          />
        ) : null}
        {coverage.equivalence_status === "proven" && coverage.proof_kind && PROOF_KIND_META[coverage.proof_kind] ? (
          <MetaBadge
            meta={PROOF_KIND_META[coverage.proof_kind]}
            title={proofKindTitle(coverage.proof_kind)}
          />
        ) : null}
        {coverage.bytecode_drift === true ? (
          <MetaBadge
            meta={DRIFT_TRUE_META}
            label="⚠ code changed"
            title="Runtime bytecode at this impl changed since the audit was matched"
          />
        ) : null}
        {coverage.bytecode_drift === false ? (
          <MetaBadge
            meta={DRIFT_FALSE_META}
            label="✓ bytecode stable"
            title="Runtime bytecode hash matches the hash captured at audit match time"
          />
        ) : null}
        {!driftKnown && coverage.bytecode_keccak_now && !coverage.bytecode_keccak_at_match ? (
          <span
            className="ps-badge"
            title="Anchor not set — refresh coverage to stamp runtime bytecode hash"
            style={{ "--badge-accent": "#6b7590", fontSize: 10 }}
          >
            drift unverified
          </span>
        ) : null}
        {findings.length ? (
          <span
            className="ps-badge"
            title={findingTitle || "This audit still has live findings on the current code"}
            style={{ "--badge-accent": "#92400e", fontSize: 10 }}
          >
            {findings.length} live finding{findings.length === 1 ? "" : "s"}
          </span>
        ) : null}
      </div>
      {verificationLabel || blockWindow ? (
        <div className="upgrade-audit-card-facts">
          {verificationLabel ? <span>{verificationLabel}</span> : null}
          {blockWindow ? <span>{blockWindow}</span> : null}
        </div>
      ) : null}
      {note ? <div className="upgrade-audit-card-note">{note}</div> : null}
    </>
  );

  return href ? (
    <a className="upgrade-audit-card" href={href}>
      {content}
    </a>
  ) : (
    <div className="upgrade-audit-card">{content}</div>
  );
}

function UpgradesTab({ detail }) {
  const history = detail?.upgrade_history;
  const contractId = detail?.contract_id ?? null;
  const companyName = detail?.company || null;

  const [auditTimeline, setAuditTimeline] = useState(null);
  const [auditTimelineError, setAuditTimelineError] = useState(null);
  useEffect(() => {
    if (!contractId) {
      setAuditTimeline(null);
      setAuditTimelineError(null);
      return;
    }
    let cancelled = false;
    setAuditTimeline(null);
    setAuditTimelineError(null);
    api(`/api/contracts/${encodeURIComponent(contractId)}/audit_timeline`)
      .then((t) => { if (!cancelled) setAuditTimeline(t); })
      .catch((e) => { if (!cancelled) setAuditTimelineError(e.message || String(e)); });
    return () => { cancelled = true; };
  }, [contractId]);

  const coverageRows = auditTimeline?.coverage || [];
  const currentStatus = auditTimeline?.current_status || null;

  function frontendCurrentImplAudited() {
    if (!auditTimeline || !history?.proxies) return false;
    const targetProxy = Object.entries(history.proxies).find(
      ([addr]) => addr.toLowerCase() === (history.target_address || detail?.address || "").toLowerCase(),
    );
    if (!targetProxy) return false;
    const impls = targetProxy[1]?.implementations || [];
    if (!impls.length) return false;
    const current = impls[impls.length - 1];
    return coverageRows.some((c) => matchesEra(c, current));
  }

  function auditStatusBanner() {
    if (!contractId) return null;
    if (auditTimelineError) {
      return <span className="chip" style={{ background: "#fee2e2", color: "#991b1b" }}>audit lookup failed: {auditTimelineError}</span>;
    }
    if (!auditTimeline) {
      return <span className="chip" style={{ background: "#f1f5f9", color: "#475569" }}>loading audits…</span>;
    }
    // Backend is block-range strict; relax to "audited" when date alignment
    // on the current impl-era has evidence the backend couldn't see.
    if (currentStatus === "unaudited_since_upgrade" && frontendCurrentImplAudited()) {
      return <MetaBadge meta={AUDIT_STATUS_META.audited} label="Current impl audited" />;
    }
    const meta = AUDIT_STATUS_META[currentStatus] || null;
    if (!meta) return null;
    const label = currentStatus === "audited" ? "Current impl audited" : currentStatus === "non_proxy_unaudited" ? "Unaudited" : undefined;
    return <MetaBadge meta={meta} label={label} />;
  }

  if (!history || !Object.keys(history.proxies || {}).length) {
    return (
      <div className="stack">
        {contractId ? (
          <div className="chips">{auditStatusBanner()}</div>
        ) : null}
        <p className="empty">No proxy upgrade history available.</p>
      </div>
    );
  }

  const deps = detail?.dependencies?.dependencies || {};
  // history.target_address is always the proxy whose upgrade history this
  // artifact describes. detail.address can diverge on merged /api/analyses
  // rows — when an impl run is merged behind its proxy, detail.address is
  // the IMPL but the upgrade_history is keyed to the proxy. Preferring
  // target_address here keeps isTarget() matching the proxy's row in the
  // timeline, so audit chips attach instead of silently disappearing.
  const targetAddr = (history.target_address || detail?.address || "").toLowerCase();

  function proxyLabel(addr) {
    if (addr.toLowerCase() === targetAddr) {
      return detail?.run_name || detail?.contract_name || shortenAddress(addr);
    }
    const dep = deps[addr];
    if (dep?.contract_name) return dep.contract_name;
    return shortenAddress(addr);
  }

  function isTarget(addr) {
    return addr.toLowerCase() === targetAddr;
  }

  function formatTimestamp(ts) {
    if (!ts) return null;
    return new Date(ts * 1000).toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
  }

  function implLabel(addr, fallbackName) {
    if (!addr) return "unknown";
    if (fallbackName) return fallbackName;
    const dep = deps[addr];
    if (dep?.contract_name) return dep.contract_name;
    const nested = Object.values(deps).find(
      (d) => typeof d.implementation === "object" && d.implementation?.address === addr
    );
    if (nested?.implementation?.contract_name) return nested.implementation.contract_name;
    return null;
  }

  return (
    <div className="stack">
      {contractId ? (
        <div className="chips">
          {auditStatusBanner()}
          {auditTimeline && coverageRows.length ? (() => {
            // Raw coverageRows.length over-counts when audits linked to
            // the proxy via a generic scope name (e.g. "UUPSProxy").
            const proxy = history?.proxies
              ? Object.entries(history.proxies).find(
                  ([addr]) => addr.toLowerCase() === (history.target_address || detail?.address || "").toLowerCase(),
                )?.[1]
              : null;
            const impls = proxy?.implementations || [];
            const placed = new Set();
            for (const cov of coverageRows) {
              for (const impl of impls) {
                if (matchesEra(cov, impl)) {
                  placed.add(cov.audit_id);
                  break;
                }
              }
            }
            const n = placed.size;
            if (n === 0) return null;
            return (
              <span className="chip" style={{ background: "#f1f5f9", color: "#475569" }}>
                {n} audit{n === 1 ? "" : "s"} in history
              </span>
            );
          })() : null}
        </div>
      ) : null}
      <div className="summary-grid">
        <StatCard label="Proxies" value={Object.keys(history.proxies).length} />
        <StatCard label="Total Upgrades" value={history.total_upgrades} />
      </div>
      {Object.entries(history.proxies)
        .sort(([a], [b]) => (isTarget(a) ? -1 : isTarget(b) ? 1 : 0))
        .map(([addr, proxy]) => (
        <div className="card" key={addr}>
          <div className="card-header-row">
            <h3>{proxyLabel(addr)}</h3>
            {isTarget(addr) ? <span className="chip">target</span> : null}
            <span className="chip alt">{proxy.proxy_type}</span>
          </div>
          <div className="mono muted" style={{ marginBottom: 8 }}>{addr}</div>
          <div className="kv-grid compact">
            <div className="kv-row">
              <span className="key">Current implementation</span>
              <span>{proxy.current_implementation ? (<>{implLabel(proxy.current_implementation) ? <strong>{implLabel(proxy.current_implementation)} </strong> : null}<span className="mono">{shortenAddress(proxy.current_implementation)}</span></>) : "None"}</span>
            </div>
            <div className="kv-row">
              <span className="key">Upgrade count</span>
              <span>{proxy.upgrade_count}</span>
            </div>
            {proxy.first_upgrade_block ? (
              <div className="kv-row">
                <span className="key">First upgrade</span>
                <span>Block {proxy.first_upgrade_block.toLocaleString()}</span>
              </div>
            ) : null}
            {proxy.last_upgrade_block ? (
              <div className="kv-row">
                <span className="key">Last upgrade</span>
                <span>Block {proxy.last_upgrade_block.toLocaleString()}</span>
              </div>
            ) : null}
          </div>

          {proxy.implementations?.length > 0 ? (
            <div className="subsection">
              <div className="subsection-title">Implementation Timeline</div>
              <div className="timeline">
                {proxy.implementations.map((impl, idx) => {
                  // Other proxies would need their own timeline fetch.
                  const coverageForEra = isTarget(addr)
                    ? coverageRows.filter((c) => matchesEra(c, impl))
                    : [];
                  return (
                  <div className={`timeline-entry ${idx === proxy.implementations.length - 1 ? "current" : "past"}`} key={impl.address + idx}>
                    <div className="timeline-marker" />
                    <div className="timeline-content">
                      <div className="timeline-header">
                        <strong>{implLabel(impl.address, impl.contract_name) || shortenAddress(impl.address)}</strong>
                        {idx === proxy.implementations.length - 1 ? <span className="chip">current</span> : <span className="chip warn">replaced</span>}
                      </div>
                      <div className="kv-grid compact small" style={{ marginTop: 4 }}>
                        <div className="kv-row">
                          <span className="key">Address</span>
                          <span className="mono">{impl.address}</span>
                        </div>
                        {impl.block_introduced ? (
                          <div className="kv-row">
                            <span className="key">Introduced</span>
                            <span>
                              {formatTimestamp(impl.timestamp_introduced) || `Block ${impl.block_introduced.toLocaleString()}`}
                              {impl.block_replaced ? ` \u2192 replaced ${formatTimestamp(impl.timestamp_replaced) || `block ${impl.block_replaced.toLocaleString()}`}` : ""}
                            </span>
                          </div>
                        ) : null}
                        {impl.tx_hash ? (
                          <div className="kv-row">
                            <span className="key">Tx</span>
                            <span className="mono">{impl.tx_hash}</span>
                          </div>
                        ) : null}
                        {impl.contract_name ? (
                          <div className="kv-row">
                            <span className="key">Contract</span>
                            <span>{impl.contract_name}</span>
                          </div>
                        ) : null}
                      </div>
                      {isTarget(addr) && auditTimeline ? (
                        <div style={{ marginTop: 10 }}>
                          {coverageForEra.length === 0 ? (
                            <span className="chip" style={{ background: "#fee2e2", color: "#991b1b", fontSize: 10, padding: "2px 8px" }}>
                              no audit coverage
                            </span>
                          ) : (
                            <div className="upgrade-audit-list">
                              {coverageForEra.map((cov) => (
                                <UpgradeAuditCard
                                  key={cov.audit_id}
                                  coverage={cov}
                                  companyName={companyName}
                                />
                              ))}
                            </div>
                          )}
                        </div>
                      ) : null}
                    </div>
                  </div>
                  );
                })}
              </div>
            </div>
          ) : null}

          {proxy.events?.length > 0 ? (
            <div className="subsection">
              <div className="subsection-title">All Events ({proxy.events.length})</div>
              <table className="event-table">
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Type</th>
                    <th>Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {proxy.events.map((evt, idx) => (
                    <tr key={idx}>
                      <td>{formatTimestamp(evt.timestamp) || <span className="mono">{evt.block_number?.toLocaleString()}</span>}</td>
                      <td><span className={`chip ${evt.event_type === "upgraded" ? "alt" : ""}`}>{evt.event_type}</span></td>
                      <td className="small">
                        {evt.event_type === "upgraded" && evt.implementation ? (
                          <><span className="key">New impl: </span><strong>{implLabel(evt.implementation) || ""}</strong> <span className="mono">{shortenAddress(evt.implementation)}</span></>
                        ) : null}
                        {evt.event_type === "admin_changed" ? (
                          <><span className="key">Admin: </span><span className="mono">{shortenAddress(evt.previous_admin)}</span> {"\u2192"} <span className="mono">{shortenAddress(evt.new_admin)}</span></>
                        ) : null}
                        {evt.event_type === "beacon_upgraded" && evt.beacon ? (
                          <><span className="key">Beacon: </span><span className="mono">{shortenAddress(evt.beacon)}</span></>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function RawTab({ detail }) {
  const [selection, setSelection] = useState("contract_analysis");
  const available = {
    contract_analysis: detail?.contract_analysis,
    control_snapshot: detail?.control_snapshot,
    dependencies: detail?.dependencies,
    dependency_graph_viz: detail?.dependency_graph_viz,
    effective_permissions: detail?.effective_permissions,
    principal_labels: detail?.principal_labels,
    resolved_control_graph: detail?.resolved_control_graph,
    upgrade_history: detail?.upgrade_history,
  };

  return (
    <div className="stack">
      <select className="select" value={selection} onChange={(event) => setSelection(event.target.value)}>
        {Object.keys(available).map((key) => (
          <option key={key} value={key}>
            {key}
          </option>
        ))}
      </select>
      <pre className="pre-wrap code-block">{formatJson(available[selection] || {})}</pre>
    </div>
  );
}

function GraphNodeDetails({ node, extra }) {
  if (!node) {
    return (
      <div className="detail-panel empty">
        <p>Select a node to inspect it.</p>
      </div>
    );
  }

  const nested = extra?.analysis;
  const policy = extra?.policy_state;
  return (
    <div className="detail-panel">
      <div className="eyebrow">Selected Node</div>
      <h3>{node.detailTitle || node.title}</h3>
      <div className="mono muted">{node.address || node.raw?.address || "no address"}</div>
      <div className="chips" style={{ marginTop: 12 }}>
        <span className="chip alt">{node.kind}</span>
        {node.resolvedType ? <span className="chip alt">{node.resolvedType}</span> : null}
      </div>
      <div className="kv-grid compact" style={{ marginTop: 12 }}>
        {node.subtitle ? (
          <div className="kv-row">
            <span className="key">Subtitle</span>
            <span>{node.subtitle}</span>
          </div>
        ) : null}
        {node.meta ? (
          <div className="kv-row">
            <span className="key">Meta</span>
            <span>{node.meta}</span>
          </div>
        ) : null}
      </div>

      {nested ? (
        <div className="subsection">
          <div className="subsection-title">Recursive Contract</div>
          <div className="kv-grid compact">
            <div className="kv-row">
              <span className="key">Name</span>
              <span>{nested.contract_name || nested.contract_analysis?.subject?.name}</span>
            </div>
            <div className="kv-row">
              <span className="key">Control model</span>
              <span>{nested.summary?.control_model || nested.contract_analysis?.summary?.control_model || "unknown"}</span>
            </div>
            <div className="kv-row">
              <span className="key">Pausable</span>
              <span>{String(Boolean(nested.summary?.is_pausable || nested.contract_analysis?.summary?.is_pausable))}</span>
            </div>
          </div>
          {policy ? (
            <div className="summary-grid detail-stats">
              <StatCard label="Public Caps" value={(policy.public_capabilities || []).length} />
              <StatCard label="Role Caps" value={(policy.role_capabilities || []).length} />
              <StatCard label="User Roles" value={(policy.user_roles || []).length} />
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="subsection">
        <div className="subsection-title">Raw Node</div>
        <pre className="pre-wrap code-block small">{formatJson(node.raw || {})}</pre>
      </div>
    </div>
  );
}

function GraphTab({ detail }) {
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedExtra, setSelectedExtra] = useState(null);
  const [graphMode, setGraphMode] = useState("address");
  const [panArmed, setPanArmed] = useState(false);
  const stageRef = useRef(null);
  const svgRef = useRef(null);
  const viewportRef = useRef(null);
  const transformRef = useRef({ x: 0, y: 0, scale: 1 });
  const fitTransformRef = useRef({ x: 0, y: 0, scale: 1 });
  const rafRef = useRef(0);
  const dragRef = useRef(null);
  const suppressClickUntilRef = useRef(0);

  useEffect(() => {
    setSelectedNode(null);
    setSelectedExtra(null);
  }, [detail?.run_name, graphMode]);

  useEffect(() => {
    function isTypingTarget(target) {
      const element = target instanceof HTMLElement ? target : null;
      if (!element) {
        return false;
      }
      const tag = element.tagName;
      return tag === "INPUT" || tag === "TEXTAREA" || element.isContentEditable;
    }

    function onKeyDown(event) {
      if (event.code !== "Space" || isTypingTarget(event.target)) {
        return;
      }
      event.preventDefault();
      setPanArmed(true);
    }

    function onKeyUp(event) {
      if (event.code !== "Space") {
        return;
      }
      setPanArmed(false);
    }

    function onBlur() {
      setPanArmed(false);
    }

    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("keyup", onKeyUp);
    window.addEventListener("blur", onBlur);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("keyup", onKeyUp);
      window.removeEventListener("blur", onBlur);
    };
  }, []);

  const graph = useMemo(() => {
    const visual = graphMode === "address" ? buildVisualAddressGraph(detail) : buildVisualPermissionGraph(detail);
    if (!visual) {
      return null;
    }
    return graphMode === "address" ? layoutVisualAddressGraph(visual) : layoutVisualPermissionGraph(visual);
  }, [detail, graphMode]);

  const graphBounds = useMemo(() => {
    if (!graph) return { minX: 0, minY: 0, maxX: 0, maxY: 0, width: 0, height: 0 };
    const margin = 40;
    const minX = Math.min(...graph.nodes.map((node) => node.x)) - margin;
    const minY = Math.min(...graph.nodes.map((node) => node.y)) - margin;
    const maxX = Math.max(...graph.nodes.map((node) => node.x + node.width)) + margin;
    const maxY = Math.max(...graph.nodes.map((node) => node.y + node.height)) + margin;
    return {
      minX,
      minY,
      maxX,
      maxY,
      width: maxX - minX,
      height: maxY - minY,
    };
  }, [graph]);

  const edgeGeometry = useMemo(() => {
    if (!graph) return [];
    const outgoingById = new Map();
    const incomingById = new Map();

    for (const edge of graph.edges) {
      if (!edge.from || !edge.to) {
        continue;
      }
      const outgoing = outgoingById.get(edge.from.id) || [];
      outgoing.push(edge);
      outgoingById.set(edge.from.id, outgoing);

      const incoming = incomingById.get(edge.to.id) || [];
      incoming.push(edge);
      incomingById.set(edge.to.id, incoming);
    }

    return graph.edges
      .filter((edge) => edge.from && edge.to)
      .map((edge) => {
        const outgoing = outgoingById.get(edge.from.id) || [];
        const incoming = incomingById.get(edge.to.id) || [];
        const sourceIndex = outgoing.findIndex((item) => item === edge);
        const targetIndex = incoming.findIndex((item) => item === edge);
        const sourcePercents = edgeSlotPercents(outgoing.length);
        const targetPercents = edgeSlotPercents(incoming.length);
        const startX = edge.from.x + edge.from.width;
        const startY = edge.from.y + (edge.from.height * (sourcePercents[sourceIndex] || 50)) / 100;
        const endX = edge.to.x;
        const endY = edge.to.y + (edge.to.height * (targetPercents[targetIndex] || 50)) / 100;
        const curve = Math.max(50, (endX - startX) / 2.2);
        return {
          ...edge,
          edgeId: `${edge.from.id}|${edge.to.id}|${edge.label || ""}`,
          startX,
          startY,
          endX,
          endY,
          path: `M ${startX} ${startY} C ${startX + curve} ${startY}, ${endX - curve} ${endY}, ${endX} ${endY}`,
        };
      });
  }, [graph]);

  const connectedSelection = useMemo(() => {
    if (!graph || !selectedNode) {
      return { activeNodeIds: null, activeEdgeIds: null };
    }

    if (selectedNode.id === "contract:root") {
      return {
        activeNodeIds: new Set(graph.nodes.map((node) => node.id)),
        activeEdgeIds: new Set(
          edgeGeometry
            .filter((edge) => edge.from && edge.to)
            .map((edge) => edge.edgeId),
        ),
      };
    }

    const outgoingById = new Map();
    const incomingById = new Map();
    for (const edge of graph.edges) {
      if (!edge.from || !edge.to) {
        continue;
      }
      const outgoing = outgoingById.get(edge.from.id) || [];
      outgoing.push(edge);
      outgoingById.set(edge.from.id, outgoing);
      const incoming = incomingById.get(edge.to.id) || [];
      incoming.push(edge);
      incomingById.set(edge.to.id, incoming);
    }

    const activeNodeIds = new Set();
    const activeEdgeIds = new Set();

    function walk(startId, direction) {
      const visited = new Set();
      const queue = [startId];
      while (queue.length) {
        const current = queue.shift();
        if (!current || visited.has(current)) {
          continue;
        }
        visited.add(current);
        activeNodeIds.add(current);
        const edges = direction === "forward" ? outgoingById.get(current) || [] : incomingById.get(current) || [];
        for (const edge of edges) {
          if (!edge.from || !edge.to) {
            continue;
          }
          activeEdgeIds.add(`${edge.from.id}|${edge.to.id}|${edge.label || ""}`);
          const nextId = direction === "forward" ? edge.to.id : edge.from.id;
          if (!visited.has(nextId)) {
            queue.push(nextId);
          }
        }
      }
    }

    walk(selectedNode.id, "forward");
    walk(selectedNode.id, "backward");

    return { activeNodeIds, activeEdgeIds };
  }, [graph, edgeGeometry, selectedNode]);

  useEffect(() => {
    function applyTransform() {
      const viewport = viewportRef.current;
      if (!viewport) {
        return;
      }
      const { x, y, scale } = transformRef.current;
      viewport.setAttribute("transform", `translate(${x} ${y}) scale(${scale})`);
    }

    function scheduleApply() {
      if (rafRef.current) {
        return;
      }
      rafRef.current = window.requestAnimationFrame(() => {
        rafRef.current = 0;
        applyTransform();
      });
    }

    const stage = stageRef.current;
    const svg = svgRef.current;
    if (!stage || !svg || !graph) {
      return undefined;
    }

    function fitToView() {
      const nextRect = stage.getBoundingClientRect();
      const paddingX = 56;
      const paddingY = 44;
      const fitScaleX = ((nextRect.width - paddingX * 2) / graphBounds.width) * (graph.width / nextRect.width);
      const fitScaleY = ((nextRect.height - paddingY * 2) / graphBounds.height) * (graph.height / nextRect.height);
      const scale = Math.min(Math.max(Math.min(fitScaleX, fitScaleY) * 1.12, 0.94), 2.9);
      const nextTransform = {
        x: (graph.width - graphBounds.width * scale) / 2 - graphBounds.minX * scale,
        y: (graph.height - graphBounds.height * scale) / 2 - graphBounds.minY * scale,
        scale,
      };
      transformRef.current = nextTransform;
      fitTransformRef.current = nextTransform;
      applyTransform();
    }

    fitToView();

    function onWheel(event) {
      event.preventDefault();
      const svgRect = svg.getBoundingClientRect();
      const pointX = event.clientX - svgRect.left;
      const pointY = event.clientY - svgRect.top;
      const current = transformRef.current;
      if (!event.ctrlKey && !event.metaKey) {
        const panFactor = event.shiftKey ? 4.2 : 3.1;
        transformRef.current = {
          ...current,
          x: current.x - (event.shiftKey ? event.deltaY : event.deltaX) * panFactor,
          y: current.y - event.deltaY * (event.shiftKey ? 0 : panFactor),
        };
        scheduleApply();
        return;
      }
      const factor = event.deltaY < 0 ? 1.08 : 0.92;
      const nextScale = Math.min(3.6, Math.max(0.22, current.scale * factor));
      const worldX = (pointX - current.x) / current.scale;
      const worldY = (pointY - current.y) / current.scale;
      transformRef.current = {
        scale: nextScale,
        x: pointX - worldX * nextScale,
        y: pointY - worldY * nextScale,
      };
      scheduleApply();
    }

    function startDrag(event) {
      const isPrimary = event.button === 0;
      const isMiddle = event.button === 1;
      if (!isPrimary && !isMiddle) {
        return;
      }
      if (!panArmed && !isMiddle && event.target !== stage && event.target !== svg) {
        return;
      }
      dragRef.current = {
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        originX: transformRef.current.x,
        originY: transformRef.current.y,
        moved: false,
      };
      stage.classList.add("dragging");
      stage.setPointerCapture(event.pointerId);
    }

    function onPointerDown(event) {
      startDrag(event);
    }

    function onPointerMove(event) {
      const drag = dragRef.current;
      if (!drag || drag.pointerId !== event.pointerId) {
        return;
      }
      const sensitivity = 3.4;
      if (Math.abs(event.clientX - drag.startX) > 3 || Math.abs(event.clientY - drag.startY) > 3) {
        drag.moved = true;
      }
      transformRef.current = {
        ...transformRef.current,
        x: drag.originX + (event.clientX - drag.startX) * sensitivity,
        y: drag.originY + (event.clientY - drag.startY) * sensitivity,
      };
      scheduleApply();
    }

    function stopDrag(event) {
      const drag = dragRef.current;
      if (!drag || (event && drag.pointerId !== event.pointerId)) {
        return;
      }
      dragRef.current = null;
      stage.classList.remove("dragging");
      if (drag.moved) {
        suppressClickUntilRef.current = Date.now() + 220;
      }
      if (event) {
        stage.releasePointerCapture(event.pointerId);
      }
    }

    stage.addEventListener("wheel", onWheel, { passive: false });
    stage.addEventListener("pointerdown", onPointerDown);
    stage.addEventListener("pointermove", onPointerMove);
    stage.addEventListener("pointerup", stopDrag);
    stage.addEventListener("pointercancel", stopDrag);

    return () => {
      stage.removeEventListener("wheel", onWheel);
      stage.removeEventListener("pointerdown", onPointerDown);
      stage.removeEventListener("pointermove", onPointerMove);
      stage.removeEventListener("pointerup", stopDrag);
      stage.removeEventListener("pointercancel", stopDrag);
      if (rafRef.current) {
        window.cancelAnimationFrame(rafRef.current);
        rafRef.current = 0;
      }
    };
  }, [graph, graphBounds, panArmed]);

  async function selectNode(node) {
    if (Date.now() < suppressClickUntilRef.current) {
      return;
    }
    if (selectedNode?.id === node?.id) {
      setSelectedNode(null);
      setSelectedExtra(null);
      return;
    }
    setSelectedNode(node || null);
    setSelectedExtra(null);

    const rawNode = node || null;
    const analysisPath = rawNode?.graphNode?.artifacts?.analysis || rawNode?.raw?.artifacts?.analysis;
    if (!analysisPath) {
      return;
    }
    const parts = String(analysisPath).split("/");
    const runName = parts[parts.length - 2];
    if (!runName) {
      return;
    }
    try {
      const analysis = await api(`/api/analyses/${encodeURIComponent(runName)}`);
      let policyState = null;
      try {
        policyState = await api(`/api/analyses/${encodeURIComponent(runName)}/artifact/policy_state.json`);
      } catch {
        policyState = null;
      }
      setSelectedExtra({ analysis, policy_state: policyState });
    } catch {
      setSelectedExtra(null);
    }
  }

  function clearSelection() {
    if (Date.now() < suppressClickUntilRef.current) {
      return;
    }
    setSelectedNode(null);
    setSelectedExtra(null);
  }

  if (!graph) {
    return <p className="empty">No resolved permission graph available.</p>;
  }

  return (
    <div className="stack">
      <div className="graph-toolbar">
        <button
          type="button"
          className={`chip alt buttonlike ${graphMode === "address" ? "active" : ""}`}
          onClick={() => setGraphMode("address")}
        >
          Address view
        </button>
        <button
          type="button"
          className={`chip alt buttonlike ${graphMode === "path" ? "active" : ""}`}
          onClick={() => setGraphMode("path")}
        >
          Path view
        </button>
        {graphMode === "address" ? (
          <>
            <span className="chip alt">{graph.nodes.filter((node) => ADDRESS_GRAPH_COLUMNS.includes(node.column)).length} addresses</span>
            <span className="chip alt">{graph.edges.length} links</span>
          </>
        ) : (
          <>
            <span className="chip alt">{graph.nodes.filter((node) => PRINCIPAL_COLUMNS.has(node.column)).length} principals</span>
            <span className="chip alt">{graph.nodes.filter((node) => node.column === "controller").length} controllers</span>
            <span className="chip alt">{graph.nodes.filter((node) => node.column === "function").length} functions</span>
          </>
        )}
        <span className="chip alt">Space + drag or middle mouse to pan</span>
        <button
          type="button"
          className="chip alt buttonlike"
          onClick={() => {
            transformRef.current = fitTransformRef.current;
            const viewport = viewportRef.current;
            if (viewport) {
              const { x, y, scale } = transformRef.current;
              viewport.setAttribute("transform", `translate(${x} ${y}) scale(${scale})`);
            }
          }}
        >
          Reset view
        </button>
      </div>
      <div className="graph-layout">
        <div className="graph-panel">
          <div className="graph-legend">
            <span className="chip alt">
              {graphMode === "address"
                ? "Address graph: one node per address, edges are typed control relationships"
                : "Left to right: who can call it → gate/role → function → contract"}
            </span>
            <span className="chip alt">Green circle = EOA</span>
            <span className="chip alt">Square = contract-like principal</span>
          </div>
          <div
            ref={stageRef}
            className={`graph-stage svg-stage ${panArmed ? "pan-armed" : ""}`}
            onClick={(event) => {
              if (event.target === event.currentTarget) {
                clearSelection();
              }
            }}
          >
            <svg
              ref={svgRef}
              className="graph-svg-root"
              viewBox={`0 0 ${graph.width} ${graph.height}`}
              onClick={(event) => {
                if (event.target === event.currentTarget) {
                  clearSelection();
                }
              }}
            >
              <defs>
                <marker id="graph-arrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="rgba(34, 29, 23, 0.24)" />
                </marker>
              </defs>
              <g ref={viewportRef}>
                {edgeGeometry.map((edge, index) => (
                  <path
                    key={edge.edgeId || `${edge.from.id}-${edge.to.id}-${index}`}
                    className={`graph-edge ${
                      connectedSelection.activeEdgeIds && !connectedSelection.activeEdgeIds.has(edge.edgeId) ? "dimmed" : ""
                    }`}
                    d={edge.path}
                    markerEnd="url(#graph-arrow)"
                  />
                ))}
                {graph.nodes.map((node) => (
                  <g
                    key={node.id}
                    className={`graph-svg-node ${node.kind} ${selectedNode?.id === node.id ? "selected" : ""} ${
                      connectedSelection.activeNodeIds && !connectedSelection.activeNodeIds.has(node.id) ? "dimmed" : ""
                    }`}
                    transform={`translate(${node.x}, ${node.y})`}
                    onPointerDown={(event) => {
                      if (panArmed || event.button === 1) {
                        const stage = stageRef.current;
                        if (stage) {
                          const isPrimary = event.button === 0;
                          const isMiddle = event.button === 1;
                          if (isPrimary || isMiddle) {
                            dragRef.current = {
                              pointerId: event.pointerId,
                              startX: event.clientX,
                              startY: event.clientY,
                              originX: transformRef.current.x,
                              originY: transformRef.current.y,
                              moved: false,
                            };
                            stage.classList.add("dragging");
                            stage.setPointerCapture(event.pointerId);
                          }
                        }
                      }
                      event.stopPropagation();
                    }}
                    onClick={(event) => {
                      event.stopPropagation();
                      selectNode(node);
                    }}
                    style={{ cursor: "pointer" }}
                  >
                    <title>{[node.detailTitle || node.title, node.subtitle, node.meta].filter(Boolean).join(" | ")}</title>
                    {renderNodeBody(node)}
                  </g>
                ))}
              </g>
            </svg>
          </div>
        </div>
        <GraphNodeDetails node={selectedNode} extra={selectedExtra} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Company overview
// ---------------------------------------------------------------------------

function computeProtocolScore(contracts, hierarchy, auditCoverage) {
  const safe = Array.isArray(contracts) ? contracts : [];
  const total = safe.length || 1;

  const riskCounts = { high: 0, medium: 0, low: 0, unknown: 0 };
  for (const c of safe) {
    const lvl = c.risk_level && riskCounts[c.risk_level] != null ? c.risk_level : "unknown";
    riskCounts[lvl]++;
  }
  const riskScore = safe.length
    ? Math.max(0, 1 - (riskCounts.high * 1.0 + riskCounts.medium * 0.5) / safe.length)
    : 0.5;

  const controlledOwners = (hierarchy || []).filter((g) => g.owner && g.owner_is_contract).length;
  const ownerGroups = (hierarchy || []).length || 1;
  const controlScore = Math.min(1, controlledOwners / ownerGroups);

  const proxyCount = safe.filter((c) => c.is_proxy).length;
  const upgradesKnown = safe.filter((c) => c.upgrade_count != null).length;
  const upgradeScore = proxyCount === 0 ? 0.6 : Math.min(1, upgradesKnown / Math.max(1, proxyCount));

  let auditScore = 0;
  if (auditCoverage?.coverage?.length) {
    const covered = auditCoverage.coverage.filter((r) => (r.audit_count || 0) > 0).length;
    auditScore = covered / auditCoverage.coverage.length;
  }

  const transparency = safe.filter((c) => c.name && c.name.toLowerCase() !== "unknown").length / total;

  const axes = [
    { key: "coverage", label: "Coverage", value: auditScore, display: `${Math.round(auditScore * 100)}%` },
    { key: "control", label: "Control", value: controlScore, display: `${Math.round(controlScore * 100)}%` },
    { key: "risk", label: "Risk", value: riskScore, display: `${Math.round(riskScore * 100)}%` },
    { key: "upgrades", label: "Upgrades", value: upgradeScore, display: `${Math.round(upgradeScore * 100)}%` },
    { key: "transparency", label: "Transparency", value: transparency, display: `${Math.round(transparency * 100)}%` },
  ];

  const composite = Math.round((axes.reduce((a, x) => a + x.value, 0) / axes.length) * 100);
  const grade = composite >= 85 ? "a" : composite >= 70 ? "b" : composite >= 55 ? "c" : composite >= 40 ? "d" : "f";
  return { axes, composite, grade };
}

function CompanyOverview({ companyName, onSelectContract, onNavigateToSurface }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [auditCoverage, setAuditCoverage] = useState(null);
  const [addressesModalOpen, setAddressesModalOpen] = useState(false);
  const [auditsAdminOpen, setAuditsAdminOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api(`/api/company/${encodeURIComponent(companyName)}`)
      .then((d) => { if (!cancelled) setData(d); })
      .catch((e) => { if (!cancelled) setError(e.message); });
    // Audit coverage is a separate concern — fetching it in parallel means
    // the overview still renders even if the audits pipeline hasn't been
    // wired up yet for this protocol. 404 / 500 / network errors are
    // swallowed; the audit column just stays empty.
    api(`/api/company/${encodeURIComponent(companyName)}/audit_coverage`)
      .then((c) => { if (!cancelled) setAuditCoverage(c); })
      .catch(() => { /* audits optional — keep the page usable */ });
    return () => { cancelled = true; };
  }, [companyName]);

  if (error) return <div className="page"><section className="panel"><p className="empty">Failed to load company overview: {error}</p></section></div>;
  if (!data) return <div className="page"><section className="panel"><p className="empty">Loading...</p></section></div>;

  const { contracts, ownership_hierarchy: hierarchy } = data;

  // Composite-score inputs still use hierarchy + coverageByAddr even though
  // the hierarchy list itself is no longer rendered below the fold.
  const coverageByAddr = (() => {
    const map = {};
    for (const row of auditCoverage?.coverage || []) {
      if (row.address) map[row.address.toLowerCase()] = row;
    }
    return map;
  })();

  const { axes, composite, grade } = computeProtocolScore(contracts, hierarchy, auditCoverage);
  const coveredContracts = Object.values(coverageByAddr).filter((r) => r.audit_count > 0).length;

  const proxyCount = contracts.filter((c) => c.is_proxy).length;

  return (
    <div className="company-page">
      {/* Hero band — edge-to-edge, no card borders */}
      <section className="company-hero-band">
        <div className="company-hero-inner">
          <ProtocolLogo name={companyName} size="xlarge" />
          <div className="company-hero-title-block">
            <p className="company-hero-eyebrow">Protocol</p>
            <h1 className="company-hero-title">{companyName}</h1>
            <p className="company-hero-subtitle">
              {contracts.length} contracts mapped · {auditCoverage?.audit_count ?? 0} reports on file
            </p>
          </div>
        </div>
      </section>

      {/* Score + stats on the left, radar on the right */}
      <section className="company-score-band">
        <div className="company-score-left">
          <div>
            <p className="eyebrow" style={{ margin: 0 }}>Composite Score</p>
            <div className={`company-hero-score grade-${grade}`}>
              <span className="company-hero-score-value">{composite}</span>
              <span className="company-hero-score-unit">/ 100</span>
            </div>
            <div className="company-hero-score-label">Grade {grade.toUpperCase()}</div>
            <div className={`company-hero-grade-bar grade-${grade}`}>
              <div className="company-hero-grade-bar-fill" style={{ width: `${Math.max(4, composite)}%` }} />
            </div>
          </div>

          <div className="company-hero-stats">
            <button
              type="button"
              className="company-hero-stat company-hero-stat--clickable"
              onClick={() => setAddressesModalOpen(true)}
              title="Browse all addresses"
            >
              <div className="company-hero-stat-value">{contracts.length}</div>
              <div className="company-hero-stat-label">Contracts ↗</div>
            </button>
            <button
              type="button"
              className="company-hero-stat company-hero-stat--clickable"
              onClick={() => setAuditsAdminOpen(true)}
              title="Manage audits (admin)"
            >
              <div className="company-hero-stat-value">{auditCoverage?.audit_count ?? "—"}</div>
              <div className="company-hero-stat-label">Reports ↗</div>
            </button>
            <div className="company-hero-stat">
              <div className="company-hero-stat-value">{coveredContracts}</div>
              <div className="company-hero-stat-label">Covered</div>
            </div>
            <div className="company-hero-stat">
              <div className="company-hero-stat-value">{proxyCount}</div>
              <div className="company-hero-stat-label">Proxies</div>
            </div>
          </div>
        </div>

        <div className="company-score-right">
          <p className="eyebrow" style={{ margin: 0 }}>Security Radar</p>
          <ProtocolRadar axes={axes} size={300} />
        </div>
      </section>

      {/* Inline Control Surface — real ProtocolSurface, not a static preview. */}
      <section className="company-surface-band">
        <div className="company-surface-band-header">
          <div>
            <p className="eyebrow" style={{ margin: 0 }}>Control Surface</p>
            <h2 className="company-surface-band-title">
              {contracts.length} contracts · {proxyCount} proxies · audits in the side panel
            </h2>
          </div>
          <div className="company-surface-band-actions">
            <button
              type="button"
              className="company-surface-action"
              onClick={() => setAddressesModalOpen(true)}
              title="Browse, label, and compare addresses"
            >
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <path d="M3 7h18" />
                <path d="M3 12h18" />
                <path d="M3 17h18" />
                <circle cx="5" cy="7" r="0.5" fill="currentColor" />
                <circle cx="5" cy="12" r="0.5" fill="currentColor" />
                <circle cx="5" cy="17" r="0.5" fill="currentColor" />
              </svg>
              <span>Addresses</span>
              <span className="company-surface-action-count">
                {data.all_addresses?.length ?? contracts.length}
              </span>
            </button>
            <button
              type="button"
              className="company-surface-action"
              onClick={() => setAuditsAdminOpen(true)}
              title="Manage audit reports"
            >
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <path d="M12 2 4 6v6c0 5 3.5 9.3 8 10 4.5-.7 8-5 8-10V6l-8-4Z" />
                <path d="m9 12 2 2 4-4" />
              </svg>
              <span>Audits</span>
              {auditCoverage?.audit_count != null && (
                <span className="company-surface-action-count">{auditCoverage.audit_count}</span>
              )}
            </button>
            <button
              type="button"
              className="company-surface-action primary"
              onClick={onNavigateToSurface}
              title="Open the fullscreen Control Surface"
            >
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <path d="M15 3h6v6" />
                <path d="M9 21H3v-6" />
                <path d="M21 3 14 10" />
                <path d="M3 21l7-7" />
              </svg>
              <span>Fullscreen</span>
            </button>
          </div>
        </div>
        <div className="company-surface-embed">
          <ProtocolSurface companyName={companyName} />
        </div>
      </section>

      {addressesModalOpen && (
        <AddressesModal
          companyName={companyName}
          onClose={() => setAddressesModalOpen(false)}
          onSelectContract={(row) => {
            // Only jump into the job view for addresses that were actually
            // analyzed; discovered-only rows don't have a job_id. Pass the
            // matching Contract job_id up to the App-level loader.
            const full = contracts.find((c) => c.address?.toLowerCase() === row.address?.toLowerCase());
            if (full?.job_id) onSelectContract(full.job_id);
          }}
        />
      )}
      {auditsAdminOpen && (
        <AuditsAdminModal
          companyName={companyName}
          onClose={() => setAuditsAdminOpen(false)}
        />
      )}
    </div>
  );
}

const PIPELINE_STAGES = ["discovery", "dapp_crawl", "defillama_scan", "selection", "static", "resolution", "policy", "coverage"];
const ALL_STAGES = [...PIPELINE_STAGES, "done"];
const GENERIC_PROXY_NAMES = new Set(["uupsproxy", "erc1967proxy", "transparentupgradeableproxy", "proxy", "beaconproxy", "ossifiableproxy", "withdrawalsmanagerproxy", "upgradeablebeacon"]);

function displayName(entry) {
  const explicit = entry?.display_name || "";
  if (explicit) {
    return explicit;
  }
  const contractName = entry?.contract_name || "";
  if (GENERIC_PROXY_NAMES.has(contractName.toLowerCase())) {
    return entry.run_name || contractName;
  }
  return contractName || entry?.run_name || "";
}

function mergeProxyImpl(analyses) {
  const implByProxy = new Map();
  const mergedProxies = new Set();

  for (const a of analyses) {
    if (a.proxy_address) implByProxy.set(a.proxy_address.toLowerCase(), a);
  }

  const merged = [];
  for (const a of analyses) {
    if (a.proxy_address) continue; // skip standalone impl entries — they'll be merged into their proxy
    if (a.is_proxy && a.implementation_address) {
      const impl = implByProxy.get(a.address?.toLowerCase());
      if (impl) {
        merged.push({
          ...impl,
          proxy_address_display: a.address,
          proxy_type_display: a.proxy_type,
          display_name: displayName(a) || displayName(impl),
          rank_score: a.rank_score ?? impl.rank_score,
          company: a.company || impl.company,
        });
        mergedProxies.add(a.address?.toLowerCase());
        continue;
      }
    }
    merged.push(a);
  }
  // Add impl entries whose proxy wasn't in the list
  for (const a of analyses) {
    if (a.proxy_address && !mergedProxies.has(a.proxy_address.toLowerCase())) {
      merged.push(a);
    }
  }
  return merged;
}

// ---------------------------------------------------------------------------
// Protocol Monitoring
// ---------------------------------------------------------------------------

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
  role_granted: "#f59e0b",
  role_revoked: "#f59e0b",
  threshold_changed: "#f59e0b",
  delay_changed: "#f59e0b",
  state_changed_poll: "#8b5cf6",
};

function ProtocolMonitoringPage({ companyName }) {
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
    if (config.watch_signers) flags.push("signers");
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

// ---------------------------------------------------------------------------
// Proxy Watcher (WIP)
// ---------------------------------------------------------------------------

function ProxyWatcherPage() {
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

// ---------------------------------------------------------------------------
// Pipeline monitor
// ---------------------------------------------------------------------------

function shortFailReason(error) {
  if (!error) return "Unknown";
  if (error.includes("No verified source")) return "Not Verified";
  if (error.includes("No such file or directory")) return "Crawler Missing";
  if (error.includes("Read timed out")) return "RPC Timeout";
  if (error.includes("name resolution") || error.includes("NameResolutionError")) return "DNS Failure";
  if (error.includes("Max retries exceeded")) return "RPC Unreachable";
  if (error.includes("value too long")) return "DB Column Overflow";
  if (error.includes("StringDataRightTruncation")) return "DB Column Overflow";
  if (error.includes("execution reverted")) return "Contract Reverted";
  if (error.includes("rate limit") || error.includes("429")) return "Rate Limited";
  if (error.includes("PendingRollbackError")) return "DB Session Error";
  const last = error.split("\n").filter(Boolean).pop() || "";
  const match = last.match(/^\w+Error:\s*(.{0,40})/);
  return match ? match[1] : last.slice(0, 40) || "Unknown";
}

function formatElapsed(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function formatStageLabel(stage) {
  return String(stage || "").replaceAll("_", " ").toUpperCase();
}

function sortByUpdatedAtDesc(a, b) {
  return new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime();
}

function monitorJobLabel(job) {
  return job.name || job.company || (job.address ? shortenAddress(job.address) : "Job");
}

function monitorJobScope(job) {
  const request = job?.request && typeof job.request === "object" ? job.request : {};

  if (job.stage === "dapp_crawl") {
    const urls = Array.isArray(request.dapp_urls) ? request.dapp_urls.filter(Boolean) : [];
    if (urls.length) {
      try {
        return new URL(urls[0]).host;
      } catch {
        return String(urls[0]);
      }
    }
  }

  if (job.stage === "defillama_scan" && request.defillama_protocol) {
    return `protocol ${request.defillama_protocol}`;
  }

  if (job.stage === "selection" && job.company) {
    return `ranking ${job.company}`;
  }

  if (job.company) return job.company;
  if (job.address) return shortenAddress(job.address);
  return job.job_id?.slice(0, 8) || "job";
}

function PipelineDashboard() {
  const [allJobs, setAllJobs] = useState([]);
  const [stats, setStats] = useState(null);
  const [auditPipeline, setAuditPipeline] = useState(null);
  const [loaded, setLoaded] = useState(false);
  const [now, setNow] = useState(Date.now());
  const [expandedError, setExpandedError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function fetchAll() {
      try {
        const [jobs, s, audits] = await Promise.all([
          api("/api/jobs"),
          api("/api/stats"),
          getAuditPipeline().catch(() => null),
        ]);
        if (!cancelled) {
          setAllJobs(jobs);
          setStats(s);
          setAuditPipeline(audits);
          setLoaded(true);
        }
      } catch {}
    }
    fetchAll();
    const timer = setInterval(fetchAll, 2500);
    return () => { cancelled = true; clearInterval(timer); };
  }, []);

  // Tick every second so elapsed timers update live
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  // Filter to only show meaningful analysis jobs:
  // - Skip proxy jobs once their impl child job exists (the impl does the real work)
  // - Skip company/discovery-only jobs once child contract jobs exist
  const hasChildJobs = useMemo(() =>
    allJobs.some((j) => !j.company && j.address),
  [allJobs]);
  const implProxyAddresses = useMemo(() =>
    new Set(allJobs.map((j) => (j.request?.proxy_address || "").toLowerCase()).filter(Boolean)),
  [allJobs]);
  const visiblePipelineJobs = useMemo(() =>
    allJobs.filter((j) => {
      // Always show jobs that are still actively running
      const isActive = j.status === "queued" || j.status === "processing";
      if (j.is_proxy && !isActive) return false;
      if (!j.is_proxy && j.address && implProxyAddresses.has(j.address.toLowerCase()) && !isActive) return false;
      if (j.company && hasChildJobs && j.status === "completed") return false;
      return true;
    }),
  [allJobs, hasChildJobs, implProxyAddresses]);

  const buckets = useMemo(() => {
    const b = {};
    for (const s of ALL_STAGES) b[s] = { queued: [], processing: [], completed: [], failed: [] };
    for (const j of visiblePipelineJobs) {
      const stage = j.stage || "discovery";
      const status = j.status || "queued";
      if (b[stage] && b[stage][status]) b[stage][status].push(j);
    }
    return b;
  }, [visiblePipelineJobs]);

  const totals = useMemo(() => {
    const t = { queued: 0, processing: 0, completed: 0, failed: 0, total: 0 };
    for (const j of visiblePipelineJobs) {
      t[j.status] = (t[j.status] || 0) + 1; t.total++;
    }
    return t;
  }, [visiblePipelineJobs]);

  const activeStageGroups = useMemo(() =>
    ALL_STAGES
      .map((stage) => ({
        stage,
        jobs: visiblePipelineJobs
          .filter((job) => job.stage === stage && job.status === "processing")
          .sort(sortByUpdatedAtDesc),
      }))
      .filter((entry) => entry.jobs.length > 0),
  [visiblePipelineJobs]);

  // Protocol-centric grouping: one card per protocol, collapsing the old
  // "Live Stage Activity" (stage-grouped) and "Audit Extraction" (separate
  // panel) into a single unified view. Audits are a parallel sidecar —
  // they aren't a stage in the job pipeline, they run alongside it.
  const protocolGroups = useMemo(() => {
    const byCompany = new Map();
    function ensure(company) {
      const key = company || "__standalone__";
      if (!byCompany.has(key)) {
        byCompany.set(key, {
          key,
          company: company || null,
          jobs: [],
          audits: { text: { processing: [], pending: [], failed: [] }, scope: { processing: [], pending: [], failed: [] } },
        });
      }
      return byCompany.get(key);
    }
    for (const j of visiblePipelineJobs) {
      ensure(j.company).jobs.push(j);
    }
    if (auditPipeline) {
      for (const [apiStage, localStage] of [["text_extraction", "text"], ["scope_extraction", "scope"]]) {
        const bucket = auditPipeline[apiStage] || {};
        for (const status of ["processing", "pending", "failed"]) {
          for (const item of bucket[status] || []) {
            ensure(item.company).audits[localStage][status].push(item);
          }
        }
      }
    }
    // Keep only protocols with *active* work (running/queued jobs or any audit
    // activity). Completed-only protocols drop out — their completion shows
    // in the "Recently Completed" section below.
    const groups = [...byCompany.values()].filter((g) => {
      const anyActiveJob = g.jobs.some((j) => j.status === "processing" || j.status === "queued");
      const anyAudit = ["text", "scope"].some((s) =>
        g.audits[s].processing.length + g.audits[s].pending.length + g.audits[s].failed.length > 0,
      );
      return anyActiveJob || anyAudit;
    });
    // Sort: protocols with running jobs first (by most-recent update), then
    // audit-only, then standalone.
    groups.sort((a, b) => {
      const rankA = a.jobs.some((j) => j.status === "processing") ? 0 : 1;
      const rankB = b.jobs.some((j) => j.status === "processing") ? 0 : 1;
      if (rankA !== rankB) return rankA - rankB;
      const lastA = a.jobs.length ? Math.max(...a.jobs.map((j) => new Date(j.updated_at || j.created_at).getTime())) : 0;
      const lastB = b.jobs.length ? Math.max(...b.jobs.map((j) => new Date(j.updated_at || j.created_at).getTime())) : 0;
      return lastB - lastA;
    });
    return groups;
  }, [visiblePipelineJobs, auditPipeline]);

  // Completed-in-the-last-hour feed — replaces the old "Recent Activity"
  // table, which duplicated the processing/queued information shown above.
  const RECENT_WINDOW_MS = 60 * 60 * 1000;
  const recentlyCompleted = useMemo(() => {
    const cutoff = now - RECENT_WINDOW_MS;
    return allJobs
      .filter((j) => (j.status === "completed" || j.status === "failed") && j.updated_at && new Date(j.updated_at).getTime() >= cutoff)
      .sort(sortByUpdatedAtDesc)
      .slice(0, 20);
  }, [allJobs, now]);

  if (!loaded) {
    return <div className="page"><section className="panel"><p style={{ textAlign: "center", padding: "2rem 0", color: "#64748b" }}>Loading pipeline status...</p></section></div>;
  }
  if (!allJobs.length) {
    return <div className="page"><section className="panel empty-state"><p className="empty">No jobs yet. Submit an analysis to get started.</p></section></div>;
  }

  const stageColors = { discovery: "#0f766e", dapp_crawl: "#0e7490", defillama_scan: "#0891b2", selection: "#6366f1", static: "#d97706", resolution: "#2563eb", policy: "#7c3aed", coverage: "#059669", done: "#16a34a" };
  const statusColors = { queued: "#94a3b8", processing: "#f59e0b", completed: "#22c55e", failed: "#ef4444" };
  const colW = 160, gapW = 80, headerH = 64, dotR = 6;
  const totalW = ALL_STAGES.length * colW + (ALL_STAGES.length - 1) * gapW;
  const dotsPerRow = Math.floor((colW - 20) / (dotR * 2 + 4));
  const maxDots = Math.max(1, ...ALL_STAGES.map((s) => { const b = buckets[s]; return (b.processing?.length || 0) + (b.queued?.length || 0) + (b.completed?.length || 0) + (b.failed?.length || 0); }));
  const dotsAreaH = Math.max(60, Math.ceil(maxDots / dotsPerRow) * (dotR * 2 + 4) + 20);
  const totalH = headerH + dotsAreaH + 40;

  function renderDots(jobs, startX, startY) {
    return jobs.map((j, i) => {
      const cx = startX + 10 + (i % dotsPerRow) * (dotR * 2 + 4) + dotR;
      const cy = startY + Math.floor(i / dotsPerRow) * (dotR * 2 + 4) + dotR;
      return (
        <g key={j.job_id}>
          <title>{`${j.name || j.company || j.address || j.job_id}\n${j.status} / ${j.stage}`}</title>
          <circle cx={cx} cy={cy} r={dotR} fill={statusColors[j.status] || "#94a3b8"} opacity={j.status === "processing" ? 1 : 0.8}>
            {j.status === "processing" && <animate attributeName="opacity" values="1;0.4;1" dur="1.5s" repeatCount="indefinite" />}
          </circle>
        </g>
      );
    });
  }

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <div><p className="eyebrow">Pipeline Status</p><h2>{totals.total} Jobs</h2></div>
          <div className="chips">
            {stats && <span className="chip" style={{ background: "#e0e7ff", color: "#3730a3" }}>{stats.unique_addresses} addresses</span>}
            <span className="chip" style={{ background: "#dcfce7", color: "#166534" }}>{totals.completed} done</span>
            {totals.processing > 0 && <span className="chip" style={{ background: "#fef3c7", color: "#92400e" }}>{totals.processing} running</span>}
            {totals.queued > 0 && <span className="chip" style={{ background: "#f1f5f9", color: "#475569" }}>{totals.queued} queued</span>}
            {totals.failed > 0 && <span className="chip" style={{ background: "#fee2e2", color: "#991b1b" }}>{totals.failed} failed</span>}
          </div>
        </div>
        <svg viewBox={`0 0 ${totalW + 40} ${totalH}`} style={{ width: "100%", height: "auto", marginTop: 16 }}>
          {ALL_STAGES.map((stage, i) => {
            const x = 20 + i * (colW + gapW);
            const b = buckets[stage];
            const all = [...(b.processing || []), ...(b.queued || []), ...(b.failed || []), ...(b.completed || [])];
            return (
              <g key={stage}>
                <rect x={x} y={0} width={colW} height={totalH} rx="12" fill={stageColors[stage]} opacity="0.06" />
                <rect x={x} y={0} width={colW} height={headerH} rx="12" fill={stageColors[stage]} opacity="0.12" />
                <rect x={x} y={headerH - 12} width={colW} height={12} fill={stageColors[stage]} opacity="0.12" />
                <text x={x + colW / 2} y={24} textAnchor="middle" fontSize="12" fontWeight="700" fill={stageColors[stage]}>{formatStageLabel(stage)}</text>
                <text x={x + colW / 2} y={40} textAnchor="middle" fontSize="11" fill={stageColors[stage]} opacity="0.7">{all.length}</text>
                {b.processing.length > 0 && (
                  <>
                    <circle cx={x + colW / 2 - 28} cy={54} r="4" fill={statusColors.processing}>
                      <animate attributeName="opacity" values="1;0.35;1" dur="1.4s" repeatCount="indefinite" />
                    </circle>
                    <text x={x + colW / 2} y={58} textAnchor="middle" fontSize="10" fontWeight="700" fill={statusColors.processing}>
                      {`${b.processing.length} active`}
                    </text>
                  </>
                )}
                {renderDots(all, x, headerH + 10)}
                {i < ALL_STAGES.length - 1 && <line x1={x + colW + 8} y1={totalH / 2} x2={x + colW + gapW - 8} y2={totalH / 2} stroke="#cbd5e1" strokeWidth="2" markerEnd="url(#pipeline-arrow)" />}
              </g>
            );
          })}
          <defs><marker id="pipeline-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M 0 0 L 8 4 L 0 8 z" fill="#cbd5e1" /></marker></defs>
        </svg>
        <div className="chips" style={{ marginTop: 12, justifyContent: "center" }}>
          <span className="chip" style={{ background: "#fef3c7", color: "#92400e", fontSize: 10 }}>Processing</span>
          <span className="chip" style={{ background: "#f1f5f9", color: "#475569", fontSize: 10 }}>Queued</span>
          <span className="chip" style={{ background: "#dcfce7", color: "#166534", fontSize: 10 }}>Completed</span>
          <span className="chip" style={{ background: "#fee2e2", color: "#991b1b", fontSize: 10 }}>Failed</span>
        </div>
      </section>

      {protocolGroups.length > 0 && (
        <section className="panel" style={{ marginTop: 16 }}>
          <div className="panel-header">
            <div>
              <p className="eyebrow">Running Protocols</p>
              <h2>{protocolGroups.length} active</h2>
            </div>
          </div>
          <div className="protocol-card-grid">
            {protocolGroups.map((group) => (
              <ProtocolCard
                key={group.key}
                group={group}
                now={now}
                stageColors={stageColors}
                statusColors={statusColors}
                expandedError={expandedError}
                setExpandedError={setExpandedError}
              />
            ))}
          </div>
        </section>
      )}

      <section className="panel" style={{ marginTop: 16 }}>
        <div className="panel-header">
          <div>
            <p className="eyebrow">Recently Completed</p>
            <h2>Last hour</h2>
          </div>
          <div className="chips">
            <span className="chip" style={{ background: "rgba(34,197,94,0.12)", color: "#4ade80" }}>
              {recentlyCompleted.filter((j) => j.status === "completed").length} done
            </span>
            {recentlyCompleted.some((j) => j.status === "failed") && (
              <span className="chip" style={{ background: "rgba(239,68,68,0.12)", color: "#fca5a5" }}>
                {recentlyCompleted.filter((j) => j.status === "failed").length} failed
              </span>
            )}
          </div>
        </div>
        {recentlyCompleted.length === 0 ? (
          <p className="empty" style={{ textAlign: "center", padding: "16px 0" }}>
            No jobs have completed in the last hour.
          </p>
        ) : (
          <div className="completion-tape">
            {recentlyCompleted.map((j) => {
              const done = new Date(j.updated_at).getTime();
              const ago = now - done;
              const label = j.name || j.company || (j.address ? shortenAddress(j.address) : "Job");
              const isFailed = j.status === "failed";
              return (
                <React.Fragment key={j.job_id}>
                  <div
                    className={`completion-row ${isFailed ? "failed" : ""}`}
                    onClick={() => isFailed && setExpandedError(expandedError === j.job_id ? null : j.job_id)}
                    style={{ cursor: isFailed ? "pointer" : "default" }}
                  >
                    <span className={`completion-dot ${isFailed ? "failed" : ""}`} />
                    <span className="completion-name">{label}</span>
                    <span className="completion-stage" style={{ color: stageColors[j.stage] || "#94a3b8" }}>
                      {formatStageLabel(j.stage)}
                    </span>
                    <span className="completion-detail">
                      {isFailed
                        ? <span style={{ color: "#fca5a5" }}>{shortFailReason(j.error)}</span>
                        : (j.detail || "")}
                    </span>
                    <span className="completion-time">{formatElapsed(ago)} ago</span>
                  </div>
                  {isFailed && expandedError === j.job_id && (
                    <pre
                      style={{
                        margin: "2px 0 8px",
                        padding: "10px 14px",
                        background: "rgba(239,68,68,0.06)",
                        color: "#fca5a5",
                        fontSize: 11,
                        fontFamily: "monospace",
                        whiteSpace: "pre-wrap",
                        wordBreak: "break-all",
                        maxHeight: 260,
                        overflow: "auto",
                        borderRadius: 8,
                        border: "1px solid rgba(239,68,68,0.18)",
                      }}
                    >
                      {j.error || "No error details available"}
                    </pre>
                  )}
                </React.Fragment>
              );
            })}
          </div>
        )}
      </section>
    </div>
  );
}

// ── Protocol card: shows all running work for a single protocol in one place,
// including per-stage counts for its jobs and the audit extraction sidecar
// that runs in parallel with the main pipeline.
function ProtocolCard({ group, now, stageColors, statusColors, expandedError, setExpandedError }) {
  const { company, jobs, audits } = group;
  const running = jobs.filter((j) => j.status === "processing");
  const queued = jobs.filter((j) => j.status === "queued");
  const failed = jobs.filter((j) => j.status === "failed");
  const completedChildren = jobs.filter((j) => j.status === "completed").length;

  // Stage pills — every stage the protocol has jobs in, with running/queued counts.
  const stagesInPlay = ALL_STAGES
    .map((stage) => {
      const stageJobs = jobs.filter((j) => j.stage === stage);
      return { stage, stageJobs };
    })
    .filter((s) => s.stageJobs.length > 0);

  const auditTotals = {
    text: audits.text.processing.length + audits.text.pending.length,
    scope: audits.scope.processing.length + audits.scope.pending.length,
    textRunning: audits.text.processing.length,
    scopeRunning: audits.scope.processing.length,
    failed: audits.text.failed.length + audits.scope.failed.length,
  };
  const hasAuditActivity = auditTotals.text + auditTotals.scope + auditTotals.failed > 0;

  return (
    <article className="protocol-card">
      <div className="protocol-card-header">
        <div className="protocol-card-title">
          <span className="protocol-card-name">{company || "Standalone contracts"}</span>
          <span className="protocol-card-sub">
            {jobs.length} job{jobs.length === 1 ? "" : "s"}
            {completedChildren > 0 ? ` · ${completedChildren} done` : ""}
          </span>
        </div>
        <div className="protocol-card-chips">
          {running.length > 0 && (
            <span className="chip" style={{ background: "rgba(245,158,11,0.12)", color: "#fbbf24" }}>
              {running.length} running
            </span>
          )}
          {queued.length > 0 && (
            <span className="chip" style={{ background: "rgba(148,163,184,0.12)", color: "#cbd5e1" }}>
              {queued.length} queued
            </span>
          )}
          {failed.length > 0 && (
            <span className="chip" style={{ background: "rgba(239,68,68,0.12)", color: "#fca5a5" }}>
              {failed.length} failed
            </span>
          )}
        </div>
      </div>

      {/* Main pipeline lane: stage pills for stages the protocol currently has jobs in */}
      <div className="protocol-lane">
        <span className="protocol-lane-label">pipeline</span>
        <div className="protocol-stage-pills">
          {stagesInPlay.length === 0 ? (
            <span className="protocol-stage-empty">—</span>
          ) : (
            stagesInPlay.map(({ stage, stageJobs }) => {
              const stageRunning = stageJobs.filter((j) => j.status === "processing").length;
              const stageQueued = stageJobs.filter((j) => j.status === "queued").length;
              const stageFailed = stageJobs.filter((j) => j.status === "failed").length;
              const color = stageColors[stage] || "#94a3b8";
              return (
                <span
                  key={stage}
                  className="protocol-stage-pill"
                  style={{ borderColor: `${color}55`, color }}
                >
                  <span className="protocol-stage-pill-name">{formatStageLabel(stage)}</span>
                  <span className="protocol-stage-pill-counts">
                    {stageRunning > 0 && <span style={{ color: statusColors.processing }}>{stageRunning}</span>}
                    {stageQueued > 0 && <span style={{ color: statusColors.queued }}>{stageQueued}</span>}
                    {stageFailed > 0 && <span style={{ color: statusColors.failed }}>{stageFailed}</span>}
                  </span>
                </span>
              );
            })
          )}
        </div>
      </div>

      {/* Audit sidecar — parallel to the pipeline, not a stage in it */}
      {hasAuditActivity && (
        <div className="protocol-lane protocol-lane-audit">
          <span className="protocol-lane-label">audits</span>
          <div className="protocol-stage-pills">
            <span className="protocol-stage-pill" style={{ borderColor: "#0891b255", color: "#22d3ee" }}>
              <span className="protocol-stage-pill-name">Text</span>
              <span className="protocol-stage-pill-counts">
                {auditTotals.textRunning > 0 && <span style={{ color: statusColors.processing }}>{auditTotals.textRunning}</span>}
                {audits.text.pending.length > 0 && <span style={{ color: statusColors.queued }}>{audits.text.pending.length}</span>}
                {audits.text.failed.length > 0 && <span style={{ color: statusColors.failed }}>{audits.text.failed.length}</span>}
              </span>
            </span>
            <span className="protocol-stage-pill" style={{ borderColor: "#7c3aed55", color: "#a78bfa" }}>
              <span className="protocol-stage-pill-name">Scope</span>
              <span className="protocol-stage-pill-counts">
                {auditTotals.scopeRunning > 0 && <span style={{ color: statusColors.processing }}>{auditTotals.scopeRunning}</span>}
                {audits.scope.pending.length > 0 && <span style={{ color: statusColors.queued }}>{audits.scope.pending.length}</span>}
                {audits.scope.failed.length > 0 && <span style={{ color: statusColors.failed }}>{audits.scope.failed.length}</span>}
              </span>
            </span>
          </div>
        </div>
      )}

      {/* Inline children: up to 3 running jobs with their detail */}
      {(running.length > 0 || failed.length > 0) && (
        <div className="protocol-children">
          {running.slice(0, 3).map((job) => {
            const created = new Date(job.created_at).getTime();
            return (
              <div className="protocol-child" key={job.job_id}>
                <span className="protocol-child-dot processing" />
                <span className="protocol-child-name">{monitorJobLabel(job)}</span>
                <span className="protocol-child-stage" style={{ color: stageColors[job.stage] }}>
                  {formatStageLabel(job.stage)}
                </span>
                <span className="protocol-child-detail">{job.detail || "Working…"}</span>
                <span className="protocol-child-time">{formatElapsed(now - created)}</span>
              </div>
            );
          })}
          {running.length > 3 && (
            <div className="protocol-child-more">+{running.length - 3} more running</div>
          )}
          {failed.slice(0, 2).map((job) => (
            <React.Fragment key={job.job_id}>
              <div
                className="protocol-child failed"
                onClick={() => setExpandedError(expandedError === job.job_id ? null : job.job_id)}
              >
                <span className="protocol-child-dot failed" />
                <span className="protocol-child-name">{monitorJobLabel(job)}</span>
                <span className="protocol-child-stage" style={{ color: stageColors[job.stage] }}>
                  {formatStageLabel(job.stage)}
                </span>
                <span className="protocol-child-detail" style={{ color: "#fca5a5" }}>
                  {shortFailReason(job.error)}
                </span>
              </div>
              {expandedError === job.job_id && (
                <pre className="protocol-child-error">{job.error || "No error details available"}</pre>
              )}
            </React.Fragment>
          ))}
        </div>
      )}
    </article>
  );
}

// ---------------------------------------------------------------------------
// Runs list page
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

function RunsPage({ analyses, activeJobs, onSelect, onDiscoverMore, onSelectCompany }) {
  const [search, setSearch] = useState("");
  const protocolSectionRef = useRef(null);

  const { companies, standalone } = useMemo(() => {
    const map = new Map();
    const solo = [];
    for (const a of analyses) {
      const co = a.company;
      if (!co) { solo.push(a); continue; }
      if (!map.has(co)) map.set(co, { company: co, contracts: 0 });
      map.get(co).contracts++;
    }
    return { companies: [...map.values()].sort((a, b) => b.contracts - a.contracts), standalone: solo };
  }, [analyses]);

  const filtered = useMemo(() => {
    if (!search.trim()) return companies;
    const q = search.toLowerCase();
    return companies.filter((c) => c.company.toLowerCase().includes(q));
  }, [companies, search]);

  const contractCount = analyses.length;

  function scrollToProtocols() {
    protocolSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  return (
    <div>
      {activeJobs.length > 0 && (
        <div className="active-jobs-bar" style={{ maxWidth: 1400, margin: "0 auto", padding: "0 24px" }}>
          {activeJobs.slice(0, 8).map((j) => {
            const stageIdx = PIPELINE_STAGES.indexOf(j.stage);
            const isDone = j.stage === "done" || j.status === "completed";
            const isFailed = j.status === "failed";
            return (
              <div key={j.job_id} className={`active-job-chip ${isDone ? "done" : ""} ${isFailed ? "err" : ""}`}>
                <span className="active-job-name">{j.name || j.company || j.address || "Job"}</span>
                <span className="active-job-stage">{j.stage}</span>
                <div className="mini-bar">
                  {PIPELINE_STAGES.map((s, i) => (
                    <div key={s} className={`mini-step ${isDone || i < stageIdx ? "done" : i === stageIdx ? "current" : ""}`} />
                  ))}
                </div>
              </div>
            );
          })}
          {activeJobs.length > 8 && <div className="active-job-chip" style={{ opacity: 0.6 }}>+{activeJobs.length - 8} more</div>}
        </div>
      )}

      <section ref={protocolSectionRef} id="protocols" className="home-protocol-section">
        <div className="home-protocol-header">
          <div>
            <p className="eyebrow" style={{ margin: 0 }}>Analyzed Protocols</p>
            <h2>All protocols</h2>
          </div>
          <div className="home-protocol-search">
            <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search protocols..." />
          </div>
        </div>

        {filtered.length > 0 ? (
          <div className="home-protocol-list">
            {filtered.map((c) => (
              <button key={c.company} className="home-protocol-row" onClick={() => onSelectCompany(c.company)}>
                <ProtocolLogo name={c.company} />
                <span className="home-protocol-row-name">{c.company}</span>
                <span className="home-protocol-row-count">{c.contracts} contracts</span>
                <span className="home-protocol-row-arrow" aria-hidden="true">→</span>
              </button>
            ))}
          </div>
        ) : (
          <p className="empty">{search ? "No protocols match your search." : "No analyses yet. Submit a protocol to get started."}</p>
        )}

        {standalone.length > 0 && (
          <section className="panel" style={{ marginTop: 32 }}>
            <h3 style={{ marginBottom: 12 }}>Standalone analyses</h3>
            <div className="runs-table">
              <div className="runs-table-header">
                <span style={{ flex: 2 }}>Contract</span>
                <span style={{ flex: 3 }}>Address</span>
              </div>
              {standalone.map((a) => (
                <button key={a.job_id || a.run_name} className="runs-table-row" onClick={() => onSelect(a.job_id)}>
                  <span className="runs-cell-name" style={{ flex: 2 }}>{a.contract_name || a.run_name || "Unknown"}</span>
                  <span className="mono runs-cell-addr" style={{ flex: 3 }}>{a.address || ""}</span>
                </button>
              ))}
            </div>
          </section>
        )}
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Error boundary
// ---------------------------------------------------------------------------

class ErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { error: null }; }
  static getDerivedStateFromError(error) { return { error }; }
  render() {
    if (this.state.error) {
      return (
        <div className="page" style={{ paddingTop: 80 }}>
          <div className="card" style={{ maxWidth: 600, margin: "0 auto" }}>
            <h3>Something went wrong</h3>
            <p className="muted">{String(this.state.error)}</p>
            <button onClick={() => { this.setState({ error: null }); window.location.reload(); }}>Reload</button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// ---------------------------------------------------------------------------
// Hamburger menu (slide-out drawer)
// ---------------------------------------------------------------------------

function HamburgerMenu({ onClose, viewMode, companyName, companyTab, onNavigate, onNavigateCompanyTab }) {
  return (
    <>
      <div className="hamburger-backdrop" onClick={onClose} />
      <aside className="hamburger-drawer">
        <div className="hamburger-header">
          <span className="hamburger-brand">PSAT</span>
          <button className="hamburger-close" onClick={onClose}>&times;</button>
        </div>
        <nav className="hamburger-nav">
          <div className="hamburger-section-label">Navigation</div>
          <button className={`hamburger-link ${viewMode === "default" ? "active" : ""}`} onClick={() => { onNavigate("/", "default"); onClose(); }}>Runs</button>
          <button className={`hamburger-link ${viewMode === "monitor" ? "active" : ""}`} onClick={() => { onNavigate("/monitor", "monitor"); onClose(); }}>Monitor</button>
          <button className={`hamburger-link ${viewMode === "proxies" ? "active" : ""}`} onClick={() => { onNavigate("/proxies", "proxies"); onClose(); }}>Proxies</button>
        </nav>
        {companyName && (
          <nav className="hamburger-nav hamburger-company-section">
            <div className="hamburger-section-label">{companyName}</div>
            <button className={`hamburger-link ${viewMode === "company" && companyTab === "overview" ? "active" : ""}`} onClick={() => { onNavigateCompanyTab("overview"); onClose(); }}>Overview</button>
            <button className={`hamburger-link ${viewMode === "company" && companyTab === "surface" ? "active" : ""}`} onClick={() => { onNavigateCompanyTab("surface"); onClose(); }}>Surface</button>
            <button className={`hamburger-link ${viewMode === "company" && companyTab === "graph" ? "active" : ""}`} onClick={() => { onNavigateCompanyTab("graph"); onClose(); }}>Ownership</button>
            <button className={`hamburger-link ${viewMode === "company" && companyTab === "risk" ? "active" : ""}`} onClick={() => { onNavigateCompanyTab("risk"); onClose(); }}>Risk Matrix</button>
            <button className={`hamburger-link ${viewMode === "company" && companyTab === "audits" ? "active" : ""}`} onClick={() => { onNavigateCompanyTab("audits"); onClose(); }}>Audits</button>
            <button className={`hamburger-link ${viewMode === "company" && companyTab === "monitoring" ? "active" : ""}`} onClick={() => { onNavigateCompanyTab("monitoring"); onClose(); }}>Monitoring</button>
          </nav>
        )}
      </aside>
    </>
  );
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

export default function App() {
  const [analyses, setAnalyses] = useState([]);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDetail, setSelectedDetail] = useState(null);
  const [viewMode, setViewMode] = useState(() => parseLocationPath(window.location.pathname).mode);
  const [companyName, setCompanyName] = useState(() => { const r = parseLocationPath(window.location.pathname); return r.mode === "company" ? r.value : null; });
  const [companyTab, setCompanyTab] = useState(() => parseLocationPath(window.location.pathname).companyTab || "overview");
  const [menuOpen, setMenuOpen] = useState(false);
  // Initialize activeTab from the URL so /address/<addr>/upgrades loads
  // the upgrades tab directly on refresh — otherwise activeTab starts as
  // "summary" and only flips once loadAnalysis resolves, which means
  // UpgradesTab briefly doesn't mount and any URL-dependent tab content
  // can race with loadAnalysis' state batch.
  const [activeTab, setActiveTab] = useState(() => parseLocationPath(window.location.pathname).tab);
  const [job, setJob] = useState(null);
  const [activeJobs, setActiveJobs] = useState([]);
  const [form, setForm] = useState({ target: "", name: "", chain: "", analyzeLimit: "5" });
  const [formOpen, setFormOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const analysesRef = useRef([]);
  const activeTabRef = useRef(parseLocationPath(window.location.pathname).tab);
  const doneTimerRef = useRef(null);

  useEffect(() => { analysesRef.current = analyses; }, [analyses]);
  useEffect(() => { activeTabRef.current = activeTab; }, [activeTab]);
  useEffect(() => {
    function handleKey(e) { if (e.key === "Escape" && menuOpen) setMenuOpen(false); }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [menuOpen]);

  function navigate(path, mode) {
    const m = mode || parseLocationPath(path).mode;
    setViewMode(m);
    if (m !== "company") setCompanyName(null);
    window.history.pushState({}, "", path);
  }

  function openCompany(name) {
    setCompanyName(name);
    setCompanyTab("overview");
    setViewMode("company");
    window.history.pushState({}, "", `/company/${encodeURIComponent(name)}`);
  }

  function navigateCompanyTab(tab) {
    setCompanyTab(tab);
    const suffix = tab === "overview" ? "" : `/${tab}`;
    window.history.pushState({}, "", `/company/${encodeURIComponent(companyName)}${suffix}`);
  }

  async function loadAnalysis(runId, options = {}) {
    try {
      const payload = await api(`/api/analyses/${encodeURIComponent(runId)}`);
      const nextTab = normalizeTab(options.tab ?? activeTabRef.current);
      setSelectedRun(runId);
      setSelectedDetail(payload);
      setActiveTab(nextTab);
      setViewMode("run");
      const address = payload?.address || payload?.contract_analysis?.subject?.address;
      const path = buildLocationPath(runId, address, nextTab);
      window.history[options.history === "replace" ? "replaceState" : "pushState"]({}, "", path);
      return payload;
    } catch (err) {
      console.error("Failed to load analysis:", runId, err);
      return null;
    }
  }

  async function refreshAnalyses() {
    const payload = await api("/api/analyses");
    const filtered = payload.filter((a) => a.address);
    setAnalyses(filtered);
    return filtered;
  }

  // Initial load
  useEffect(() => {
    function handlePopState() {
      const route = parseLocationPath(window.location.pathname);
      setViewMode(route.mode);
      if (route.mode === "company") {
        setCompanyName(route.value);
        setCompanyTab(route.companyTab || "overview");
      } else if (route.mode === "run" || route.mode === "address") {
        setCompanyName(null);
        // For /address/<x> we pass the address directly: /api/analyses/<name>
        // falls back to a by-address lookup and returns the run whose primary
        // address is <x>. This bypasses the merged /api/analyses list — which
        // hides the proxy run behind the impl run and would otherwise cause
        // /address/<proxy>/upgrades to load the impl's detail (where the
        // impl run's upgrade_history doesn't include its own proxy chain).
        loadAnalysis(route.value, { tab: route.tab, history: "replace" });
      } else {
        setCompanyName(null);
      }
    }

    refreshAnalyses().catch(() => null);
    const route = parseLocationPath(window.location.pathname);
    if (route.mode === "company") {
      setCompanyName(route.value);
      setCompanyTab(route.companyTab || "overview");
    } else if (route.mode === "run" || route.mode === "address") {
      loadAnalysis(route.value, { tab: route.tab, history: "replace" });
    }

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  // Job polling — scoped to the current submission's job tree
  useEffect(() => {
    if (!job?.job_id) return undefined;
    let stopped = false;
    let timer;

    // Collect all job IDs belonging to this submission's tree
    function getJobTree(allJobs, rootId) {
      const ids = new Set([rootId]);
      let changed = true;
      while (changed) {
        changed = false;
        for (const j of allJobs) {
          if (ids.has(j.job_id)) continue;
          if (ids.has(j.request?.parent_job_id) || ids.has(j.request?.root_job_id)) {
            ids.add(j.job_id);
            changed = true;
          }
        }
      }
      return ids;
    }

    async function poll() {
      if (stopped) return;
      try {
        const allJobs = await api("/api/jobs");
        if (stopped) return;
        const now = new Date();
        const treeIds = getJobTree(allJobs, job.job_id);
        const treeJobs = allJobs.filter((j) => treeIds.has(j.job_id));
        const visible = treeJobs.filter((j) =>
          j.status === "queued" || j.status === "processing" ||
          ((j.status === "completed" || j.status === "failed") && j.updated_at && now - new Date(j.updated_at) < 30000)
        );
        setActiveJobs(visible);
        const parent = allJobs.find((j) => j.job_id === job.job_id);
        if (parent) setJob(parent);
        const stillRunning = treeJobs.some((j) => j.status === "queued" || j.status === "processing");
        if (!stillRunning && !doneTimerRef.current) {
          doneTimerRef.current = setTimeout(async () => {
            stopped = true; clearInterval(timer); setActiveJobs([]); doneTimerRef.current = null;
            await refreshAnalyses();
          }, 5000);
        }
      } catch {}
    }

    poll();
    timer = setInterval(poll, 2000);
    return () => { stopped = true; clearInterval(timer); if (doneTimerRef.current) { clearTimeout(doneTimerRef.current); doneTimerRef.current = null; } };
  }, [job?.job_id]);

  async function submit(event) {
    event.preventDefault();
    if (!form.target) return;
    setLoading(true);
    try {
      const target = form.target.trim();
      const payload = isAddress(target)
        ? { address: target, name: form.name.trim() || null }
        : {
            company: target,
            chain: form.chain.trim() || null,
            analyze_limit: Number.parseInt(form.analyzeLimit, 10) || 5,
          };
      const nextJob = await api("/api/analyze", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
      setJob(nextJob);
      setFormOpen(false);
      navigate("/monitor", "monitor");
    } finally { setLoading(false); }
  }

  async function discoverMore(company) {
    try {
      const nextJob = await api("/api/analyze", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ company, analyze_limit: 5 }) });
      setJob(nextJob);
    } catch (err) { console.error("Failed to start discovery:", err); }
  }

  function handleTabChange(tab) {
    const nextTab = normalizeTab(tab);
    setActiveTab(nextTab);
    const address = selectedDetail?.address || selectedDetail?.contract_analysis?.subject?.address;
    const path = buildLocationPath(selectedRun, address, nextTab);
    window.history.pushState({}, "", path);
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  const isDetail = viewMode === "run" || viewMode === "address";
  const isMonitor = viewMode === "monitor";
  const isCompany = viewMode === "company";
  const isProxies = viewMode === "proxies";

  const detailContent = selectedDetail ? {
    summary: <SummaryTab detail={selectedDetail} />,
    permissions: <PermissionsTab detail={selectedDetail} />,
    principals: <PrincipalsTab detail={selectedDetail} />,
    graph: <GraphTab detail={selectedDetail} />,
    dependencies: <DependencyGraphTab data={selectedDetail?.dependency_graph_viz} runName={selectedRun} />,
    upgrades: <UpgradesTab detail={selectedDetail} />,
    raw: <RawTab detail={selectedDetail} />,
  } : {};

  return (
    <ErrorBoundary>
      {/* Top nav */}
      <nav className={`top-nav ${isCompany && companyTab === "surface" ? "top-nav-dark" : ""}`}>
        <div className="top-nav-left">
          <button className="hamburger-btn" onClick={() => setMenuOpen(!menuOpen)} aria-label="Menu">
            <span className="hamburger-icon" />
          </button>
          <button className="top-nav-brand" onClick={() => { navigate("/", "default"); refreshAnalyses(); }}>PSAT</button>
          {companyName && <span className="top-nav-context">{companyName}</span>}
        </div>
        <div className="top-nav-right">
          <button className="top-nav-submit-btn" onClick={() => setFormOpen(!formOpen)}>
            {formOpen ? "Close" : "+ New Analysis"}
          </button>
        </div>
      </nav>

      {/* Hamburger drawer */}
      {menuOpen && (
        <HamburgerMenu
          onClose={() => setMenuOpen(false)}
          viewMode={viewMode}
          companyName={companyName}
          companyTab={companyTab}
          onNavigate={(path, mode) => { navigate(path, mode); refreshAnalyses(); }}
          onNavigateCompanyTab={navigateCompanyTab}
        />
      )}

      {/* Submit form dropdown */}
      {formOpen && (
        <div className="submit-dropdown">
          <form className="submit-form" onSubmit={submit}>
            <label><span>Address or company</span><input value={form.target} onChange={(e) => setForm((c) => ({ ...c, target: e.target.value }))} placeholder="0x... or etherfi" required /></label>
            <label><span>Run name</span><input value={form.name} onChange={(e) => setForm((c) => ({ ...c, name: e.target.value }))} placeholder="Optional" /></label>
            <label><span>Chain</span><input value={form.chain} onChange={(e) => setForm((c) => ({ ...c, chain: e.target.value }))} placeholder="Optional" /></label>
            <label><span>Analyze limit</span><input type="number" min="1" max="200" value={form.analyzeLimit} onChange={(e) => setForm((c) => ({ ...c, analyzeLimit: e.target.value }))} /></label>
            <button type="submit" disabled={loading}>{loading ? "Starting..." : "Run"}</button>
          </form>
        </div>
      )}

      {/* Page content */}
      {isMonitor && <PipelineDashboard />}
      {isProxies && <ProxyWatcherPage />}

      {isDetail && selectedDetail && (
        <div className="page">
          {/* Proxy banner */}
          {(selectedDetail.proxy_address_display || selectedDetail.proxy_address) && (
            <div className="proxy-banner">
              Proxy at <span className="mono">{shortenAddress(selectedDetail.proxy_address_display || selectedDetail.proxy_address)}</span>
              {selectedDetail.proxy_type_display && <span className="chip alt" style={{ marginLeft: 8, padding: "2px 8px", fontSize: 10 }}>{selectedDetail.proxy_type_display}</span>}
              <span style={{ margin: "0 6px" }}>&rarr;</span>
              Implementation at <span className="mono">{shortenAddress(selectedDetail.address)}</span>
            </div>
          )}
          <section className="panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Contract Analysis</p>
                <h2>{displayName(selectedDetail) || selectedRun || "Unknown"}</h2>
              </div>
              <div className="meta-stack">
                <div className="mono">{selectedDetail.proxy_address_display || selectedDetail.address || ""}</div>
                <div>{selectedDetail.summary?.control_model || selectedDetail.contract_analysis?.summary?.control_model || ""}</div>
              </div>
            </div>
            <div className="tabs">
              {TABS.map((tab) => (
                <button key={tab} className={`tab ${activeTab === tab ? "active" : ""}`} onClick={() => handleTabChange(tab)}>
                  {tab === "raw" ? "Raw JSON" : tab.charAt(0).toUpperCase() + tab.slice(1)}
                </button>
              ))}
            </div>
            <div className="tab-panel active">{detailContent[activeTab]}</div>
          </section>
        </div>
      )}

      {isCompany && companyName && companyTab === "overview" && (
        <CompanyOverview
          companyName={companyName}
          onSelectContract={(jobId) => loadAnalysis(jobId, { history: "push" })}
          onNavigateToSurface={() => navigateCompanyTab("surface")}
        />
      )}
      {isCompany && companyName && companyTab === "surface" && (
        <div className="fullscreen-surface">
          <ProtocolSurface companyName={companyName} />
        </div>
      )}
      {isCompany && companyName && companyTab === "graph" && (
        <div className="page" style={{ height: "calc(100vh - 52px)", display: "flex", flexDirection: "column" }}>
          <div className="protocol-graph-wrapper" style={{ flex: 1, minHeight: 0 }}>
            <ProtocolGraph companyName={companyName} />
          </div>
        </div>
      )}
      {isCompany && companyName && companyTab === "risk" && (
        <div className="page">
          <RiskSurface companyName={companyName} />
        </div>
      )}
      {isCompany && companyName && companyTab === "monitoring" && (
        <ProtocolMonitoringPage companyName={companyName} />
      )}
      {isCompany && companyName && companyTab === "audits" && (
        <AuditsTab
          companyName={companyName}
          focusAuditId={new URLSearchParams(window.location.search).get("audit")}
        />
      )}

      {!isDetail && !isMonitor && !isCompany && !isProxies && (
        <>
          <ProductHero form={form} setForm={setForm} onSubmit={submit} loading={loading} />
          <RunsPage
            analyses={analyses}
            activeJobs={activeJobs}
            onSelect={(runId) => loadAnalysis(runId, { history: "push" })}
            onDiscoverMore={discoverMore}
            onSelectCompany={openCompany}
          />
        </>
      )}
    </ErrorBoundary>
  );
}
