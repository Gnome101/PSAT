import { useEffect, useMemo, useRef, useState } from "react";

import { buildVisualPermissionGraph, layoutVisualPermissionGraph, prettyFunctionName, shortenAddress, wrapText } from "./graph.js";
import DependencyGraphTab from "./DependencyGraphTab.jsx";

const TABS = ["summary", "permissions", "principals", "graph", "dependencies", "raw"];
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

function buildLocationPath(runName, address, tab) {
  const nextTab = normalizeTab(tab);
  if (isAddress(address)) {
    return `/address/${String(address).trim()}/${nextTab}`;
  }
  if (runName) {
    return `/runs/${encodeURIComponent(runName)}/${nextTab}`;
  }
  return "/";
}

function findRunByAddress(analyses, address) {
  const target = String(address || "").toLowerCase();
  return analyses.find((analysis) => String(analysis.address || "").toLowerCase() === target)?.run_name || null;
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
        <StatCard label="Contract" value={subject.name || detail?.contract_name || "Unknown"} />
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
        const principals = (entry.authority_roles || []).flatMap((role) => role.principals || []);
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
  const stageRef = useRef(null);
  const svgRef = useRef(null);
  const viewportRef = useRef(null);
  const transformRef = useRef({ x: 0, y: 0, scale: 1 });
  const rafRef = useRef(0);
  const dragRef = useRef(null);

  useEffect(() => {
    setSelectedNode(null);
    setSelectedExtra(null);
  }, [detail?.run_name]);

  const graph = useMemo(() => {
    const visual = buildVisualPermissionGraph(detail);
    if (!visual) {
      return null;
    }
    return layoutVisualPermissionGraph(visual);
  }, [detail]);
  if (!graph) {
    return <p className="empty">No resolved permission graph available.</p>;
  }

  const graphBounds = useMemo(() => {
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
          startX,
          startY,
          endX,
          endY,
          path: `M ${startX} ${startY} C ${startX + curve} ${startY}, ${endX - curve} ${endY}, ${endX} ${endY}`,
        };
      });
  }, [graph]);

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

    const rect = stage.getBoundingClientRect();
    const paddingX = 56;
    const paddingY = 44;
    const fitScaleX = ((rect.width - paddingX * 2) / graphBounds.width) * (graph.width / rect.width);
    const fitScaleY = ((rect.height - paddingY * 2) / graphBounds.height) * (graph.height / rect.height);
    const scale = Math.min(Math.max(Math.min(fitScaleX, fitScaleY) * 1.12, 0.94), 2.9);
    transformRef.current = {
      x: (graph.width - graphBounds.width * scale) / 2 - graphBounds.minX * scale,
      y: (graph.height - graphBounds.height * scale) / 2 - graphBounds.minY * scale,
      scale,
    };
    applyTransform();

    function onWheel(event) {
      event.preventDefault();
      const svgRect = svg.getBoundingClientRect();
      const pointX = event.clientX - svgRect.left;
      const pointY = event.clientY - svgRect.top;
      const current = transformRef.current;
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

    function onPointerDown(event) {
      if (event.button !== 0) {
        return;
      }
      if (event.target !== stage && event.target !== svg) {
        return;
      }
      dragRef.current = {
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        originX: transformRef.current.x,
        originY: transformRef.current.y,
      };
      stage.classList.add("dragging");
      stage.setPointerCapture(event.pointerId);
    }

    function onPointerMove(event) {
      const drag = dragRef.current;
      if (!drag || drag.pointerId !== event.pointerId) {
        return;
      }
      const sensitivity = 1.45;
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
  }, [graph, graphBounds]);

  async function selectNode(node) {
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

  return (
    <div className="stack">
      <div className="graph-toolbar">
        <span className="chip alt">{graph.nodes.filter((node) => node.column === "principal").length} principals</span>
        <span className="chip alt">{graph.nodes.filter((node) => node.column === "controller").length} controllers</span>
        <span className="chip alt">{graph.nodes.filter((node) => node.column === "function").length} functions</span>
      </div>
      <div className="graph-layout">
        <div className="graph-panel">
          <div className="graph-legend">
            <span className="chip alt">Left to right: who can call it → gate/role → function → contract</span>
            <span className="chip alt">Green circle = EOA</span>
            <span className="chip alt">Square = contract-like principal</span>
          </div>
          <div ref={stageRef} className="graph-stage svg-stage">
            <svg ref={svgRef} className="graph-svg-root" viewBox={`0 0 ${graph.width} ${graph.height}`}>
              <defs>
                <marker id="graph-arrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="rgba(34, 29, 23, 0.24)" />
                </marker>
              </defs>
              <g ref={viewportRef}>
                {edgeGeometry.map((edge, index) => (
                  <path
                    key={`${edge.from.id}-${edge.to.id}-${index}`}
                    className="graph-edge"
                    d={edge.path}
                    markerEnd="url(#graph-arrow)"
                  />
                ))}
                {graph.nodes.map((node) => (
                  <g
                    key={node.id}
                    className={`graph-svg-node ${node.kind} ${selectedNode?.id === node.id ? "selected" : ""}`}
                    transform={`translate(${node.x}, ${node.y})`}
                    onPointerDown={(event) => event.stopPropagation()}
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

export default function App() {
  const [analyses, setAnalyses] = useState([]);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDetail, setSelectedDetail] = useState(null);
  const [activeTab, setActiveTab] = useState("summary");
  const [job, setJob] = useState(null);
  const [form, setForm] = useState({ target: "", name: "", chain: "", analyzeLimit: "5" });
  const [loading, setLoading] = useState(false);
  const analysesRef = useRef([]);
  const selectedRunRef = useRef(null);
  const activeTabRef = useRef("summary");

  useEffect(() => {
    analysesRef.current = analyses;
  }, [analyses]);

  useEffect(() => {
    selectedRunRef.current = selectedRun;
  }, [selectedRun]);

  useEffect(() => {
    activeTabRef.current = activeTab;
  }, [activeTab]);

  function syncLocation(runName, detailOrAddress, tab, historyMode = "replace") {
    const address =
      typeof detailOrAddress === "string"
        ? detailOrAddress
        : detailOrAddress?.address || detailOrAddress?.contract_analysis?.subject?.address || null;
    const nextPath = buildLocationPath(runName, address, tab);
    const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (currentPath === nextPath) {
      return;
    }
    const method = historyMode === "push" ? "pushState" : "replaceState";
    window.history[method]({}, "", nextPath);
  }

  async function loadAnalysis(runName, options = {}) {
    const payload = await api(`/api/analyses/${encodeURIComponent(runName)}`);
    const nextTab = normalizeTab(options.tab ?? activeTabRef.current);
    setSelectedRun(runName);
    setSelectedDetail(payload);
    setActiveTab(nextTab);
    syncLocation(runName, payload, nextTab, options.history || "replace");
    return payload;
  }

  async function resolveRoute(route, analysesPayload, options = {}) {
    const nextRoute = route || parseLocationPath(window.location.pathname);
    const list = analysesPayload || analysesRef.current;
    if (!list.length) {
      return;
    }

    let nextRun = options.preferredRun || null;
    let nextTab = normalizeTab(options.tab ?? nextRoute.tab ?? activeTabRef.current);

    if (!nextRun && nextRoute.mode === "run" && list.some((analysis) => analysis.run_name === nextRoute.value)) {
      nextRun = nextRoute.value;
    }
    if (!nextRun && nextRoute.mode === "address") {
      nextRun = findRunByAddress(list, nextRoute.value);
    }
    if (!nextRun) {
      nextRun = selectedRunRef.current || list[0]?.run_name || null;
      nextTab = normalizeTab(options.tab ?? activeTabRef.current);
    }
    if (nextRun) {
      await loadAnalysis(nextRun, { tab: nextTab, history: options.history || "replace" });
    }
  }

  async function refreshAnalyses(preferredRun = null, options = {}) {
    const payload = await api("/api/analyses");
    setAnalyses(payload);
    await resolveRoute(options.route || parseLocationPath(window.location.pathname), payload, {
      preferredRun,
      history: options.history || "replace",
    });
  }

  useEffect(() => {
    function handlePopState() {
      resolveRoute(parseLocationPath(window.location.pathname), null, { history: "replace" }).catch(() => null);
    }

    refreshAnalyses(null, { route: parseLocationPath(window.location.pathname), history: "replace" }).catch(() => null);
    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  useEffect(() => {
    if (!job?.job_id || job.status === "completed" || job.status === "failed") {
      return undefined;
    }
    const timer = window.setInterval(async () => {
      try {
        const next = await api(`/api/jobs/${job.job_id}`);
        setJob(next);
        if (next.status === "completed" && next.run_name) {
          await refreshAnalyses(next.run_name, { history: "push" });
        }
      } catch {
        // ignore transient polling failures in the demo UI
      }
    }, 2500);
    return () => window.clearInterval(timer);
  }, [job]);

  async function submit(event) {
    event.preventDefault();
    if (!form.target) {
      return;
    }
    setLoading(true);
    try {
      const target = form.target.trim();
      const payload = isAddress(target)
        ? {
            address: target,
            name: form.name.trim() || null,
          }
        : {
            company: target,
            chain: form.chain.trim() || null,
            analyze_limit: Number.parseInt(form.analyzeLimit, 10) || 5,
          };
      const nextJob = await api("/api/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setJob(nextJob);
      setForm((current) => ({ ...current, name: "" }));
    } finally {
      setLoading(false);
    }
  }

  function handleTabChange(tab) {
    const nextTab = normalizeTab(tab);
    setActiveTab(nextTab);
    syncLocation(selectedRun, selectedDetail, nextTab, "push");
  }

  const tabContent = {
    summary: <SummaryTab detail={selectedDetail} />,
    permissions: <PermissionsTab detail={selectedDetail} />,
    principals: <PrincipalsTab detail={selectedDetail} />,
    graph: <GraphTab detail={selectedDetail} />,
    dependencies: <DependencyGraphTab data={selectedDetail?.dependency_graph_viz} runName={selectedRun} />,
    raw: <RawTab detail={selectedDetail} />,
  };

  return (
    <div className="shell">
      <div className="sr-copy">Run an address or company and inspect the control surface</div>
      <header className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Protocol Security Assessment Tool</p>
          <h1>Run an address or company and inspect the control surface</h1>
          <p className="lede">
            Submit a contract address for direct analysis, or a company name to run discovery first and then analyze the top discovered contracts.
          </p>
        </div>
        <form className="submit-card" onSubmit={submit}>
          <label>
            <span>Address or company</span>
            <input
              value={form.target}
              onChange={(event) => setForm((current) => ({ ...current, target: event.target.value }))}
              placeholder="0x... or etherfi"
              required
            />
          </label>
          <label>
            <span>Run name</span>
            <input
              value={form.name}
              onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
              placeholder="Optional, address mode only"
            />
          </label>
          <label>
            <span>Discovery chain</span>
            <input
              value={form.chain}
              onChange={(event) => setForm((current) => ({ ...current, chain: event.target.value }))}
              placeholder="Optional, company mode only"
            />
          </label>
          <label>
            <span>Analyze top N</span>
            <input
              type="number"
              min="1"
              max="25"
              value={form.analyzeLimit}
              onChange={(event) => setForm((current) => ({ ...current, analyzeLimit: event.target.value }))}
            />
          </label>
          <button type="submit" disabled={loading}>
            {loading ? "Starting..." : "Run Analysis"}
          </button>
        </form>
      </header>

      <main className="layout">
        <aside className="sidebar">
          <section className="panel">
            <div className="panel-header">
              <h2>Runs</h2>
              <button className="ghost" onClick={() => refreshAnalyses()}>
                Refresh
              </button>
            </div>
            <div className="run-list">
              {analyses.map((analysis) => (
                <button
                  className={`run-item ${analysis.run_name === selectedRun ? "active" : ""}`}
                  key={analysis.run_name}
                  onClick={() => loadAnalysis(analysis.run_name, { tab: activeTab, history: "push" })}
                >
                  <div className="run-name">{analysis.contract_name || analysis.run_name}</div>
                  <div className="run-address">{analysis.address || "Unknown address"}</div>
                  <div className="card-subtitle">{(analysis.summary?.standards || []).join(" · ") || "No standards yet"}</div>
                </button>
              ))}
            </div>
          </section>

          <section className="panel">
            <div className="panel-header">
              <h2>Job Status</h2>
            </div>
            {!job ? (
              <div className="job-status empty">No active job</div>
            ) : (
              <div className="job-status">
                <div className={`job-pill ${job.status}`}>{job.status}</div>
                <div>
                  <strong>{job.stage || "pending"}</strong>
                </div>
                <div>{job.detail || ""}</div>
                {job.run_name ? <div className="mono">{job.run_name}</div> : null}
                {job.error ? <pre className="pre-wrap code-block small">{job.error}</pre> : null}
              </div>
            )}
          </section>
        </aside>

        <section className="content">
          <section className="panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Selected Run</p>
                <h2>{selectedDetail?.contract_name || selectedRun || "Nothing selected"}</h2>
              </div>
              <div className="meta-stack">
                <div className="mono">{selectedDetail?.address || "Unknown address"}</div>
                <div>{selectedDetail?.summary?.control_model || selectedDetail?.contract_analysis?.summary?.control_model || ""}</div>
              </div>
            </div>

            <div className="tabs">
              {TABS.map((tab) => (
                <button
                  key={tab}
                  className={`tab ${activeTab === tab ? "active" : ""}`}
                  onClick={() => handleTabChange(tab)}
                >
                  {tab === "raw" ? "Raw JSON" : tab.charAt(0).toUpperCase() + tab.slice(1)}
                </button>
              ))}
            </div>

            <div className="tab-panel active">{tabContent[activeTab]}</div>
          </section>
        </section>
      </main>
    </div>
  );
}
