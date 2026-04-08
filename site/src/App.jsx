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

const TABS = ["summary", "permissions", "principals", "graph", "dependencies", "upgrades", "raw"];
const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;

async function api(path, options) {
  const response = await fetch(path, options);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  const type = response.headers.get("content-type") || "";
  if (type.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

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
    return { mode: "company", value: segments[1], tab: "summary" };
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

function findRunByAddress(analyses, address) {
  const target = String(address || "").toLowerCase();
  return analyses.find((analysis) => {
    const subjectAddress = String(analysis.address || "").toLowerCase();
    const proxyAddress = String(analysis.proxy_address || analysis.proxy_address_display || "").toLowerCase();
    return subjectAddress === target || proxyAddress === target;
  })?.job_id || null;
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
      {detail?.analysis_report ? (
        <div className="card">
          <h3>Analysis Report</h3>
          <pre className="pre-wrap">{detail.analysis_report}</pre>
        </div>
      ) : null}
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

function UpgradesTab({ detail }) {
  const history = detail?.upgrade_history;
  if (!history || !Object.keys(history.proxies || {}).length) {
    return <p className="empty">No proxy upgrade history available.</p>;
  }

  const deps = detail?.dependencies?.dependencies || {};
  const targetAddr = (detail?.address || history.target_address || "").toLowerCase();

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
                {proxy.implementations.map((impl, idx) => (
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
                    </div>
                  </div>
                ))}
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

function CompanyOverview({ companyName, onSelectContract }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    api(`/api/company/${encodeURIComponent(companyName)}`)
      .then((d) => { if (!cancelled) setData(d); })
      .catch((e) => { if (!cancelled) setError(e.message); });
    return () => { cancelled = true; };
  }, [companyName]);

  if (error) return <div className="page"><section className="panel"><p className="empty">Failed to load company overview: {error}</p></section></div>;
  if (!data) return <div className="page"><section className="panel"><p className="empty">Loading...</p></section></div>;

  const { contracts, ownership_hierarchy: hierarchy } = data;
  const riskColor = { high: "#ef4444", medium: "#f59e0b", low: "#22c55e", unknown: "#94a3b8" };

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Company Overview</p>
            <h2>{companyName}</h2>
          </div>
          <div className="chips"><span className="chip alt">{contracts.length} contracts</span></div>
        </div>
      </section>

      {/* Ownership hierarchy */}
      <section className="panel">
        <h3 style={{ marginBottom: 16 }}>Ownership Hierarchy</h3>
        <div className="stack" style={{ gap: 20 }}>
          {hierarchy.map((group, gi) => (
            <div key={gi} className="card" style={{ borderLeft: `3px solid ${group.owner ? "#2563eb" : "#94a3b8"}` }}>
              <div style={{ marginBottom: 10 }}>
                <div style={{ fontWeight: 700, fontSize: 14 }}>
                  {group.owner_name || (group.owner ? "External address" : "No owner detected")}
                </div>
                {group.owner && (
                  <div className="mono" style={{ fontSize: 11, opacity: 0.7 }}>
                    {group.owner}
                    {group.owner_is_contract && <span className="chip" style={{ marginLeft: 6, fontSize: 9, padding: "1px 5px" }}>contract</span>}
                  </div>
                )}
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                {group.contracts.map((c) => {
                  const full = contracts.find((x) => x.address === c.address);
                  return (
                    <button
                      key={c.address}
                      className="runs-table-row"
                      style={{ padding: "6px 10px" }}
                      onClick={() => onSelectContract(full?.job_id)}
                    >
                      <span className="runs-cell-name" style={{ flex: 2 }}>
                        {c.name}
                        {full?.is_proxy && <span className="proxy-badge" title={full.proxy_type}>{full.proxy_type || "proxy"}</span>}
                      </span>
                      <span className="mono runs-cell-addr" style={{ flex: 2 }}>{c.address}</span>
                      <span style={{ flex: 1 }}>{full?.control_model || ""}</span>
                      <span style={{ flex: 1 }}>
                        {full?.risk_level && <span className="risk-dot" style={{ background: riskColor[full.risk_level] || "#94a3b8" }} />}
                        {full?.risk_level || ""}
                      </span>
                      <span style={{ flex: 1 }}>{full?.upgrade_count != null ? `${full.upgrade_count} upgrades` : ""}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* All contracts detail table */}
      <section className="panel">
        <h3 style={{ marginBottom: 16 }}>Controller Details</h3>
        <div className="stack" style={{ gap: 12 }}>
          {contracts.filter((c) => Object.keys(c.controllers || {}).length > 0).map((c) => (
            <div key={c.address} className="card">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                <div>
                  <strong>{c.name}</strong>
                  {c.is_proxy && <span className="proxy-badge" style={{ marginLeft: 6 }}>{c.proxy_type || "proxy"}</span>}
                </div>
                <span className="mono" style={{ fontSize: 11, opacity: 0.7 }}>{c.address}</span>
              </div>
              <div className="kv-grid">
                {Object.entries(c.controllers).map(([cid, val]) => (
                  <div className="kv-row" key={cid}>
                    <span className="key">{cid}</span>
                    <span className="mono" style={{ fontSize: 11 }}>{val != null ? String(val) : "null"}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

const PIPELINE_STAGES = ["discovery", "dapp_crawl", "defillama_scan", "static", "resolution", "policy"];
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
// Proxy Watcher (WIP)
// ---------------------------------------------------------------------------

function ProxyWatcherPage() {
  const [proxies, setProxies] = useState([]);
  const [events, setEvents] = useState([]);
  const [loaded, setLoaded] = useState(false);
  const [address, setAddress] = useState("");
  const [label, setLabel] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  async function refresh() {
    try {
      const [p, e] = await Promise.all([
        api("/api/watched-proxies"),
        api("/api/proxy-events?limit=100"),
      ]);
      setProxies(p);
      setEvents(e);
      setLoaded(true);
    } catch (err) {
      console.error("Failed to load proxy data:", err);
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
        body: JSON.stringify({ address: address.trim(), label: label.trim() || null }),
      });
      setAddress("");
      setLabel("");
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
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}>Last Block</th>
                  <th style={{ padding: "8px 12px", color: "#94a3b8" }}></th>
                </tr>
              </thead>
              <tbody>
                {proxies.map((p) => (
                  <tr key={p.id} style={{ borderBottom: "1px solid #1e293b" }}>
                    <td style={{ padding: "8px 12px" }}>{p.label || <span style={{ color: "#475569" }}>-</span>}</td>
                    <td style={{ padding: "8px 12px" }}><span className="mono">{shortenAddress(p.proxy_address)}</span></td>
                    <td style={{ padding: "8px 12px" }}>{p.proxy_type ? <span className="chip alt">{p.proxy_type}</span> : <span style={{ color: "#475569" }}>unknown</span>}</td>
                    <td style={{ padding: "8px 12px" }}><span className="mono">{p.last_known_implementation ? shortenAddress(p.last_known_implementation) : "-"}</span></td>
                    <td style={{ padding: "8px 12px" }}>{p.needs_polling ? <span className="chip warn">polling</span> : <span className="chip">events</span>}</td>
                    <td style={{ padding: "8px 12px" }}>{p.last_scanned_block ? p.last_scanned_block.toLocaleString() : "-"}</td>
                    <td style={{ padding: "8px 12px" }}>
                      <button onClick={() => removeProxy(p.id)} style={{ background: "none", border: "none", color: "#ef4444", cursor: "pointer", fontSize: 12 }}>remove</button>
                    </td>
                  </tr>
                ))}
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

function PipelineDashboard() {
  const [allJobs, setAllJobs] = useState([]);
  const [stats, setStats] = useState(null);
  const [loaded, setLoaded] = useState(false);
  const [now, setNow] = useState(Date.now());
  const [expandedError, setExpandedError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function fetchAll() {
      try {
        const [jobs, s] = await Promise.all([api("/api/jobs"), api("/api/stats")]);
        if (!cancelled) { setAllJobs(jobs); setStats(s); setLoaded(true); }
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
  const analysisJobs = useMemo(() =>
    allJobs.filter((j) => {
      if (j.is_proxy) return false;
      if (!j.is_proxy && j.address && implProxyAddresses.has(j.address.toLowerCase())) return false;
      if (j.company && hasChildJobs) return false;
      return true;
    }),
  [allJobs, hasChildJobs, implProxyAddresses]);

  const buckets = useMemo(() => {
    const b = {};
    for (const s of ALL_STAGES) b[s] = { queued: [], processing: [], completed: [], failed: [] };
    for (const j of analysisJobs) {
      const stage = j.stage || "discovery";
      const status = j.status || "queued";
      if (b[stage] && b[stage][status]) b[stage][status].push(j);
    }
    return b;
  }, [analysisJobs]);

  const totals = useMemo(() => {
    const t = { queued: 0, processing: 0, completed: 0, failed: 0, total: 0 };
    for (const j of analysisJobs) {
      t[j.status] = (t[j.status] || 0) + 1; t.total++;
    }
    return t;
  }, [analysisJobs]);

  if (!loaded) {
    return <div className="page"><section className="panel"><p style={{ textAlign: "center", padding: "2rem 0", color: "#64748b" }}>Loading pipeline status…</p></section></div>;
  }
  if (!allJobs.length) {
    return <div className="page"><section className="panel empty-state"><p className="empty">No jobs yet. Submit an analysis to get started.</p></section></div>;
  }

  const stageColors = { discovery: "#0f766e", dapp_crawl: "#0e7490", defillama_scan: "#0891b2", static: "#d97706", resolution: "#2563eb", policy: "#7c3aed", done: "#16a34a" };
  const statusColors = { queued: "#94a3b8", processing: "#f59e0b", completed: "#22c55e", failed: "#ef4444" };
  const colW = 160, gapW = 80, headerH = 50, dotR = 6;
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
                <text x={x + colW / 2} y={24} textAnchor="middle" fontSize="12" fontWeight="700" fill={stageColors[stage]}>{stage.toUpperCase()}</text>
                <text x={x + colW / 2} y={40} textAnchor="middle" fontSize="11" fill={stageColors[stage]} opacity="0.7">{all.length}</text>
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

      {/* Active / Recent jobs table */}
      <section className="panel" style={{ marginTop: 16 }}>
        <div className="panel-header">
          <div><p className="eyebrow">Recent Activity</p></div>
        </div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ borderBottom: "1px solid rgba(148,163,184,0.15)", color: "#94a3b8", textAlign: "left" }}>
              <th style={{ padding: "8px 12px" }}>Name</th>
              <th style={{ padding: "8px 12px" }}>Address</th>
              <th style={{ padding: "8px 12px" }}>Stage</th>
              <th style={{ padding: "8px 12px" }}>Status</th>
              <th style={{ padding: "8px 12px" }}>Time</th>
              <th style={{ padding: "8px 12px" }}>Detail</th>
            </tr>
          </thead>
          <tbody>
            {allJobs
              .filter((j) => j.status === "processing" || j.status === "failed" || j.status === "queued")
              .sort((a, b) => {
                const order = { processing: 0, failed: 1, queued: 2 };
                return (order[a.status] ?? 3) - (order[b.status] ?? 3);
              })
              .slice(0, 30)
              .map((j) => (
                <React.Fragment key={j.job_id}>
                  <tr
                    style={{ borderBottom: "1px solid rgba(148,163,184,0.08)", cursor: j.status === "failed" ? "pointer" : "default" }}
                    onClick={() => j.status === "failed" && setExpandedError(expandedError === j.job_id ? null : j.job_id)}
                  >
                    <td style={{ padding: "6px 12px", color: "#e2e8f0", fontWeight: 600 }}>{j.name || j.company || "—"}</td>
                    <td style={{ padding: "6px 12px", color: "#64748b", fontFamily: "monospace", fontSize: 11 }}>{j.address ? j.address.slice(0, 10) + ".." : "—"}</td>
                    <td style={{ padding: "6px 12px" }}>
                      <span style={{ color: stageColors[j.stage] || "#94a3b8", fontWeight: 600, fontSize: 11, textTransform: "uppercase" }}>{j.stage}</span>
                    </td>
                    <td style={{ padding: "6px 12px" }}>
                      <span style={{ color: statusColors[j.status] || "#94a3b8", fontWeight: 600, fontSize: 11 }}>
                        {j.status === "processing" ? "⚡ " : j.status === "failed" ? "✕ " : ""}{j.status}
                      </span>
                    </td>
                    <td style={{ padding: "6px 12px", color: "#94a3b8", fontSize: 11, fontFamily: "monospace" }}>
                      {(() => {
                        const created = new Date(j.created_at).getTime();
                        const end = (j.status === "completed" || j.status === "failed") ? new Date(j.updated_at).getTime() : now;
                        return formatElapsed(end - created);
                      })()}
                    </td>
                    <td style={{ padding: "6px 12px", color: "#64748b", fontSize: 11, maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {j.status === "failed"
                        ? <span style={{ color: "#fca5a5", fontWeight: 600 }}>{shortFailReason(j.error)}</span>
                        : j.detail || ""}
                    </td>
                  </tr>
                  {j.status === "failed" && expandedError === j.job_id && (
                    <tr>
                      <td colSpan={6} style={{ padding: 0 }}>
                        <pre style={{ margin: 0, padding: "12px 16px", background: "rgba(239,68,68,0.06)", color: "#fca5a5", fontSize: 11, fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-all", maxHeight: 300, overflow: "auto", borderBottom: "1px solid rgba(239,68,68,0.15)" }}>
                          {j.error || "No error details available"}
                        </pre>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Runs list page
// ---------------------------------------------------------------------------

function ProtocolView({ companyName }) {
  const [protoTab, setProtoTab] = useState("surface");
  return (
    <div className="page" style={{ height: "calc(100vh - 60px)", display: "flex", flexDirection: "column", gap: 8 }}>
      <div className="proto-tabs">
        <button className={`proto-tab ${protoTab === "surface" ? "active" : ""}`} onClick={() => setProtoTab("surface")}>surface</button>
        <button className={`proto-tab ${protoTab === "graph" ? "active" : ""}`} onClick={() => setProtoTab("graph")}>ownership</button>
        <button className={`proto-tab ${protoTab === "risk" ? "active" : ""}`} onClick={() => setProtoTab("risk")}>risk matrix</button>
      </div>
      <div style={{ flex: 1, minHeight: 0 }}>
        {protoTab === "surface" && (
          <ProtocolSurface companyName={companyName} />
        )}
        {protoTab === "graph" && (
          <div className="protocol-graph-wrapper" style={{ height: "100%" }}>
            <ProtocolGraph companyName={companyName} />
          </div>
        )}
        {protoTab === "risk" && (
          <RiskSurface companyName={companyName} />
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------

function RunsPage({ analyses, activeJobs, onSelect, onDiscoverMore, onSelectCompany }) {
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    if (!search.trim()) return analyses;
    const q = search.toLowerCase();
    return analyses.filter((a) =>
      (displayName(a) || "").toLowerCase().includes(q) ||
      (a.address || "").toLowerCase().includes(q) ||
      (a.company || "").toLowerCase().includes(q)
    );
  }, [analyses, search]);

  const grouped = useMemo(() => {
    const groups = [];
    const map = new Map();
    const standalone = [];
    const sorted = [...filtered].sort((a, b) => (b.rank_score ?? -1) - (a.rank_score ?? -1));
    for (const a of sorted) {
      if (a.company) {
        if (!map.has(a.company)) { const g = { company: a.company, items: [] }; map.set(a.company, g); groups.push(g); }
        map.get(a.company).items.push(a);
      } else {
        standalone.push(a);
      }
    }
    return { groups, standalone };
  }, [filtered]);

  const riskColor = { high: "#ef4444", medium: "#f59e0b", low: "#22c55e", unknown: "#94a3b8" };

  return (
    <div className="page">
      {activeJobs.length > 0 && (
        <div className="active-jobs-bar">
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

      <div className="runs-search">
        <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search contracts..." />
      </div>

      {grouped.groups.map((group) => (
        <section key={group.company} className="panel runs-group-panel">
          <div className="runs-group-header">
            <h3><button className="top-nav-link" style={{ fontSize: "inherit", fontWeight: "inherit" }} onClick={() => onSelectCompany && onSelectCompany(group.company)}>{group.company}</button></h3>
            <div className="chips" style={{ gap: 6 }}>
              <span className="chip alt">{group.items.length} contracts</span>
              <button className="discover-more" title={`Discover more`} onClick={() => onDiscoverMore(group.company)}>+</button>
            </div>
          </div>
          <div className="runs-table">
            <div className="runs-table-header">
              <span>Contract</span>
              <span>Address</span>
              <span>Model</span>
              <span>Risk</span>
              <span>Score</span>
            </div>
            {group.items.map((a) => (
              <button key={a.job_id || a.run_name} className="runs-table-row" onClick={() => onSelect(a.job_id)}>
                <span className="runs-cell-name">
                  {displayName(a)}
                  {a.proxy_address_display && <span className="proxy-badge" title={`${a.proxy_type_display || "proxy"} at ${a.proxy_address_display}`}>proxy</span>}
                  {a.is_proxy && !a.proxy_address_display && <span className="proxy-badge dim">proxy</span>}
                </span>
                <span className="mono runs-cell-addr">{a.proxy_address_display || a.address || ""}</span>
                <span>{a.summary?.control_model || "unknown"}</span>
                <span><span className="risk-dot" style={{ background: riskColor[a.summary?.static_risk_level] || "#94a3b8" }} />{a.summary?.static_risk_level || "unknown"}</span>
                <span>{a.rank_score != null ? a.rank_score.toFixed(2) : "-"}</span>
              </button>
            ))}
          </div>
        </section>
      ))}

      {grouped.standalone.length > 0 && (
        <section className="panel runs-group-panel">
          <div className="runs-group-header"><h3>Standalone</h3></div>
          <div className="runs-table">
            <div className="runs-table-header">
              <span>Contract</span><span>Address</span><span>Model</span><span>Risk</span><span>Score</span>
            </div>
            {grouped.standalone.map((a) => (
              <button key={a.job_id || a.run_name} className="runs-table-row" onClick={() => onSelect(a.job_id)}>
                <span className="runs-cell-name">{displayName(a)}</span>
                <span className="mono runs-cell-addr">{a.address || ""}</span>
                <span>{a.summary?.control_model || "unknown"}</span>
                <span><span className="risk-dot" style={{ background: riskColor[a.summary?.static_risk_level] || "#94a3b8" }} />{a.summary?.static_risk_level || "unknown"}</span>
                <span>{a.rank_score != null ? a.rank_score.toFixed(2) : "-"}</span>
              </button>
            ))}
          </div>
        </section>
      )}

      {!analyses.length && !activeJobs.length && (
        <section className="panel empty-state"><p className="empty">No analyses yet. Submit a contract or company to get started.</p></section>
      )}
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
// App
// ---------------------------------------------------------------------------

export default function App() {
  const [analyses, setAnalyses] = useState([]);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDetail, setSelectedDetail] = useState(null);
  const [viewMode, setViewMode] = useState(() => parseLocationPath(window.location.pathname).mode);
  const [companyName, setCompanyName] = useState(() => { const r = parseLocationPath(window.location.pathname); return r.mode === "company" ? r.value : null; });
  const [activeTab, setActiveTab] = useState("summary");
  const [job, setJob] = useState(null);
  const [activeJobs, setActiveJobs] = useState([]);
  const [form, setForm] = useState({ target: "", name: "", chain: "", analyzeLimit: "5" });
  const [formOpen, setFormOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const analysesRef = useRef([]);
  const activeTabRef = useRef("summary");
  const doneTimerRef = useRef(null);

  useEffect(() => { analysesRef.current = analyses; }, [analyses]);
  useEffect(() => { activeTabRef.current = activeTab; }, [activeTab]);

  function navigate(path, mode) {
    const m = mode || parseLocationPath(path).mode;
    setViewMode(m);
    if (m !== "company") setCompanyName(null);
    window.history.pushState({}, "", path);
  }

  function openCompany(name) {
    setCompanyName(name);
    setViewMode("company");
    window.history.pushState({}, "", `/company/${encodeURIComponent(name)}`);
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
      } else if (route.mode === "run" || route.mode === "address") {
        setCompanyName(null);
        const list = analysesRef.current;
        let run = route.mode === "run" ? route.value : findRunByAddress(list, route.value);
        if (run) loadAnalysis(run, { tab: route.tab, history: "replace" });
      } else {
        setCompanyName(null);
      }
    }

    refreshAnalyses().then((list) => {
      const route = parseLocationPath(window.location.pathname);
      if (route.mode === "company") {
        setCompanyName(route.value);
      } else if (route.mode === "run" || route.mode === "address") {
        let run = route.mode === "run" ? route.value : findRunByAddress(list, route.value);
        if (run) loadAnalysis(run, { tab: route.tab, history: "replace" });
      }
    }).catch(() => null);

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  // Job polling
  useEffect(() => {
    if (!job?.job_id) return undefined;
    let stopped = false;
    let timer;

    async function poll() {
      if (stopped) return;
      try {
        const allJobs = await api("/api/jobs");
        if (stopped) return;
        const now = new Date();
        const visible = allJobs.filter((j) =>
          j.status === "queued" || j.status === "processing" ||
          ((j.status === "completed" || j.status === "failed") && j.updated_at && now - new Date(j.updated_at) < 30000)
        );
        setActiveJobs(visible);
        const parent = allJobs.find((j) => j.job_id === job.job_id);
        if (parent) setJob(parent);
        const stillRunning = allJobs.some((j) => j.status === "queued" || j.status === "processing");
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
      <nav className="top-nav">
        <div className="top-nav-left">
          <button className="top-nav-brand" onClick={() => { navigate("/", "default"); refreshAnalyses(); }}>PSAT</button>
          <button className={`top-nav-link ${!isDetail && !isMonitor && !isProxies ? "active" : ""}`} onClick={() => { navigate("/", "default"); refreshAnalyses(); }}>Runs</button>
          <button className={`top-nav-link ${isMonitor ? "active" : ""}`} onClick={() => navigate("/monitor", "monitor")}>Monitor</button>
          <button className={`top-nav-link ${isProxies ? "active" : ""}`} onClick={() => navigate("/proxies", "proxies")}>Proxies</button>
        </div>
        <div className="top-nav-right">
          <button className="top-nav-submit-btn" onClick={() => setFormOpen(!formOpen)}>
            {formOpen ? "Close" : "+ New Analysis"}
          </button>
        </div>
      </nav>

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

      {isCompany && companyName && (
        <ProtocolView companyName={companyName} />
      )}

      {!isDetail && !isMonitor && !isCompany && !isProxies && (
        <RunsPage
          analyses={analyses}
          activeJobs={activeJobs}
          onSelect={(runId) => loadAnalysis(runId, { history: "push" })}
          onDiscoverMore={discoverMore}
          onSelectCompany={openCompany}
        />
      )}
    </ErrorBoundary>
  );
}
