import { useState, useEffect } from "react";

import { matchesEra } from "../../auditMatching.js";
import { isBytecodeVerifiedAudit } from "../../auditCoverage.js";
import { AUDIT_STATUS_META, MetaBadge } from "../../auditUi.jsx";
import { api } from "../../api/client.js";
import { shortenAddress } from "../../graph.js";
import { StatCard } from "../../ui/StatCard.jsx";
import { UpgradeAuditCard } from "../../ui/UpgradeAuditCard.jsx";

function formatTimestamp(ts) {
  if (!ts) return null;
  return new Date(ts * 1000).toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

export function UpgradesPanel({
  upgradeHistory,
  contractId,
  companyName,
  contractAddress,
  contractName,
  dependencies,
  loading = false,
}) {
  const history = upgradeHistory;
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
  const verifiedCoverageRows = coverageRows.filter(isBytecodeVerifiedAudit);
  const currentStatus = auditTimeline?.current_status || null;

  function frontendCurrentImplAudited() {
    if (!auditTimeline || !history?.proxies) return false;
    const targetProxy = Object.entries(history.proxies).find(
      ([addr]) => addr.toLowerCase() === (history.target_address || contractAddress || "").toLowerCase(),
    );
    if (!targetProxy) return false;
    const impls = targetProxy[1]?.implementations || [];
    if (!impls.length) return false;
    const current = impls[impls.length - 1];
    return verifiedCoverageRows.some((c) => matchesEra(c, current));
  }

  function auditStatusBanner() {
    if (!contractId) return null;
    if (auditTimelineError) {
      return <span className="chip" style={{ background: "#fee2e2", color: "#991b1b" }}>audit lookup failed: {auditTimelineError}</span>;
    }
    if (!auditTimeline) {
      return <span className="chip" style={{ background: "#f1f5f9", color: "#475569" }}>loading audits…</span>;
    }
    if (frontendCurrentImplAudited()) {
      return <MetaBadge meta={AUDIT_STATUS_META.audited} label="Current impl bytecode verified" />;
    }
    if (currentStatus === "audited" || currentStatus === "non_proxy_audited") {
      return <MetaBadge meta={AUDIT_STATUS_META.unaudited_since_upgrade} label="No bytecode proof" />;
    }
    const meta = AUDIT_STATUS_META[currentStatus] || null;
    if (!meta) return null;
    const label = currentStatus === "non_proxy_unaudited" ? "No bytecode proof" : undefined;
    return <MetaBadge meta={meta} label={label} />;
  }

  if (loading) {
    return (
      <div className="stack">
        <p className="empty">Loading upgrade history…</p>
      </div>
    );
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

  const deps = dependencies || {};
  // history.target_address is always the proxy whose upgrade history this
  // artifact describes. detail.address can diverge on merged /api/analyses
  // rows — when an impl run is merged behind its proxy, detail.address is
  // the IMPL but the upgrade_history is keyed to the proxy. Preferring
  // target_address here keeps isTarget() matching the proxy's row in the
  // timeline, so audit chips attach instead of silently disappearing.
  const targetAddr = (history.target_address || contractAddress || "").toLowerCase();

  function proxyLabel(addr) {
    if (addr.toLowerCase() === targetAddr) {
      return contractName || shortenAddress(addr);
    }
    const dep = deps[addr];
    if (dep?.contract_name) return dep.contract_name;
    return shortenAddress(addr);
  }

  function isTarget(addr) {
    return addr.toLowerCase() === targetAddr;
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
          {auditTimeline && verifiedCoverageRows.length ? (() => {
            // Raw coverageRows.length over-counts when audits linked to
            // the proxy via a generic scope name (e.g. "UUPSProxy").
            const proxy = history?.proxies
              ? Object.entries(history.proxies).find(
                  ([addr]) => addr.toLowerCase() === (history.target_address || contractAddress || "").toLowerCase(),
                )?.[1]
              : null;
            const impls = proxy?.implementations || [];
            const placed = new Set();
            for (const cov of verifiedCoverageRows) {
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
                {n} bytecode match{n === 1 ? "" : "es"} in history
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
                    ? verifiedCoverageRows.filter((c) => matchesEra(c, impl))
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
                              {impl.block_replaced ? ` → replaced ${formatTimestamp(impl.timestamp_replaced) || `block ${impl.block_replaced.toLocaleString()}`}` : ""}
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
                              no bytecode proof
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
                          <><span className="key">Admin: </span><span className="mono">{shortenAddress(evt.previous_admin)}</span> {"→"} <span className="mono">{shortenAddress(evt.new_admin)}</span></>
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

export default UpgradesPanel;
