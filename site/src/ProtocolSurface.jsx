/**
 * ProtocolSurface — 2D spatial visualization of contracts as shapes
 * with functions as dots on their perimeter and guard chains behind them.
 */

import { useEffect, useRef, useState, useCallback } from "react";

// ── Helpers ──────────────────────────────────────────────────────────────────

function shortAddr(a) { return a ? a.slice(0, 6) + ".." + a.slice(-4) : ""; }

const ROLE_COLORS = {
  PAUSER: "#F59E0B", UPGRADER: "#EC4899", ORACLE: "#00D4AA",
  ADMIN: "#6366F1", PROPOSER: "#00D4AA", EXECUTOR: "#6366F1",
  CANCELLER: "#FF6B35",
};

function roleColor(role) {
  if (!role) return "#555";
  const u = role.toUpperCase();
  for (const [k, c] of Object.entries(ROLE_COLORS)) if (u.includes(k)) return c;
  return "#555";
}

function inferRole(fnName) {
  const n = fnName.toLowerCase();
  if (n.includes("pause")) return "PAUSER";
  if (n.includes("upgrade")) return "UPGRADER";
  if (n.includes("execute") || n.includes("oracle") || n.includes("validator")) return "ORACLE_EXEC";
  return "ADMIN";
}

function effectSeverity(effects, name) {
  if (effects.includes("implementation_update") || effects.includes("delegatecall_execution")) return 3;
  if (effects.includes("ownership_transfer") || name.includes("renounce")) return 3;
  if (effects.includes("pause_toggle")) return 2;
  if (effects.includes("asset_send")) return 2;
  if (effects.includes("role_management")) return 2;
  return 1;
}

const SEV_COLORS = { 3: "#EC4899", 2: "#FF6B35", 1: "#555" };

// ── Layout computation ───────────────────────────────────────────────────────

function buildVisualization(companyData, functionData) {
  const contracts = [];
  const guardEntities = new Map(); // address -> { type, label, ... }
  const timelockAddr = "0x9f26d4c958fd811a1f59b01b86be7dffc9d20761";
  const registryAddr = "0x62247d29b4b9becf4bb73e0c722cf6445cfc7ce9";

  for (const c of companyData.contracts) {
    const fns = functionData[c.address] || [];
    if (fns.length === 0 && !c.is_proxy) continue;

    const fnNodes = fns.map((fn) => {
      const name = fn.function?.replace(/\(.*/, "") || "?";
      const effects = fn.effect_labels || [];
      const sev = effectSeverity(effects, name);
      const owner = fn.direct_owner;
      const controllers = fn.controllers || [];
      const hasRegistry = controllers.some((ct) => ct.label === "roleRegistry");
      const hasOwner = !!owner;

      let gateType = "public";
      let gateColor = "#555";
      let role = null;
      let guardChain = [];

      if (hasRegistry) {
        role = inferRole(name);
        gateType = "role";
        gateColor = roleColor(role);
        guardChain.push({ label: role, color: gateColor, addr: registryAddr });
        guardChain.push({ label: "roleRegistry", color: "#6366F1", addr: registryAddr });
      }
      if (hasOwner) {
        gateType = hasRegistry ? "role+owner" : "owner";
        if (owner.resolved_type === "timelock") {
          guardChain.push({ label: `${Math.round((owner.details?.delay || 0) / 86400)}d delay`, color: "#FF6B3588", addr: null });
          guardChain.push({ label: "Timelock", color: "#FF6B35", addr: timelockAddr });
          guardChain.push({ label: "Safe", color: "#00D4AA", addr: null });
        } else {
          guardChain.push({ label: "owner", color: "#FF6B35", addr: owner.address });
        }
        if (!hasRegistry) gateColor = "#FF6B35";
      }

      // Collect guard entities
      if (owner?.address) {
        guardEntities.set(owner.address.toLowerCase(), {
          type: owner.resolved_type, label: owner.resolved_type === "timelock" ? "Timelock" : shortAddr(owner.address),
        });
      }

      return { name, fullSig: fn.function, effects, sev, gateType, gateColor, role, guardChain, action: fn.action_summary };
    });

    // Sort: highest severity first, then by gate complexity
    fnNodes.sort((a, b) => b.sev - a.sev || b.guardChain.length - a.guardChain.length);

    contracts.push({
      name: c.name,
      address: c.address,
      isProxy: c.is_proxy,
      upgradeCount: c.upgrade_count,
      functions: fnNodes,
      radius: Math.max(60, 20 + fnNodes.length * 6),
    });
  }

  // Sort contracts: most functions first
  contracts.sort((a, b) => b.functions.length - a.functions.length);

  // Layout contracts in a grid-like arrangement
  let x = 0, y = 0;
  let rowHeight = 0;
  const maxRowWidth = 2400;
  const padding = 80;

  for (const c of contracts) {
    const diameter = c.radius * 2 + padding;
    if (x + diameter > maxRowWidth && x > 0) {
      x = 0;
      y += rowHeight + padding;
      rowHeight = 0;
    }
    c.cx = x + c.radius + 40;
    c.cy = y + c.radius + 40;
    x += diameter;
    rowHeight = Math.max(rowHeight, diameter);
  }

  return { contracts, totalWidth: maxRowWidth, totalHeight: y + rowHeight + padding + 200 };
}

// ── SVG rendering ────────────────────────────────────────────────────────────

function ContractShape({ contract, hoveredFn, setHoveredFn, setSelectedFn }) {
  const { cx, cy, radius, functions, name, isProxy } = contract;
  const borderColor = isProxy ? "#FF6B35" : "#00D4AA";

  return (
    <g>
      {/* Contract body */}
      <circle cx={cx} cy={cy} r={radius} fill="#2D2D2D" stroke={borderColor} strokeWidth={2} opacity={0.9} />
      <circle cx={cx} cy={cy} r={radius - 4} fill="none" stroke={borderColor} strokeWidth={0.5} opacity={0.3} />

      {/* Contract name */}
      <text x={cx} y={cy - 6} textAnchor="middle" fill="#fff" fontSize={11} fontWeight={700} fontFamily="Oswald">{name}</text>
      <text x={cx} y={cy + 8} textAnchor="middle" fill="#777" fontSize={7} fontFamily="JetBrains Mono">{functions.length} functions</text>

      {/* Function dots on perimeter */}
      {functions.map((fn, i) => {
        const angle = (i / functions.length) * Math.PI * 2 - Math.PI / 2;
        const fx = cx + Math.cos(angle) * (radius - 1);
        const fy = cy + Math.sin(angle) * (radius - 1);
        const isHovered = hoveredFn === `${contract.address}-${i}`;
        const dotSize = isHovered ? 6 : fn.sev === 3 ? 4 : 3;

        // Guard chain lines extending outward
        const chainNodes = fn.guardChain.map((step, si) => {
          const dist = radius + 18 + si * 22;
          return {
            x: cx + Math.cos(angle) * dist,
            y: cy + Math.sin(angle) * dist,
            ...step,
          };
        });

        return (
          <g key={i}
            onMouseEnter={() => setHoveredFn(`${contract.address}-${i}`)}
            onMouseLeave={() => setHoveredFn(null)}
            onClick={() => setSelectedFn({ ...fn, contract: name, address: contract.address })}
            style={{ cursor: "pointer" }}
          >
            {/* Guard chain lines */}
            {chainNodes.map((node, ni) => {
              const prevX = ni === 0 ? fx : chainNodes[ni - 1].x;
              const prevY = ni === 0 ? fy : chainNodes[ni - 1].y;
              return (
                <g key={ni}>
                  <line x1={prevX} y1={prevY} x2={node.x} y2={node.y}
                    stroke={node.color} strokeWidth={isHovered ? 1.5 : 0.5} opacity={isHovered ? 0.9 : 0.15} />
                  <circle cx={node.x} cy={node.y} r={isHovered ? 4 : 2}
                    fill={node.color} opacity={isHovered ? 1 : 0.3} />
                  {isHovered && (
                    <text x={node.x + 6} y={node.y + 3} fill={node.color} fontSize={7}
                      fontFamily="JetBrains Mono" fontWeight={600}>{node.label}</text>
                  )}
                </g>
              );
            })}

            {/* Function dot */}
            <circle cx={fx} cy={fy} r={dotSize} fill={fn.gateColor} stroke="#1A1A1A" strokeWidth={1} />

            {/* Hover label */}
            {isHovered && (
              <g>
                <rect x={fx + 8} y={fy - 12} width={fn.name.length * 6.5 + 16} height={18}
                  rx={4} fill="#2D2D2DEE" stroke={fn.gateColor} strokeWidth={0.5} />
                <text x={fx + 16} y={fy + 1} fill="#fff" fontSize={9}
                  fontFamily="JetBrains Mono" fontWeight={600}>{fn.name}()</text>
              </g>
            )}
          </g>
        );
      })}
    </g>
  );
}

function SelectedPanel({ fn, onClose }) {
  if (!fn) return null;
  return (
    <div className="ps-panel">
      <div className="ps-panel-header">
        <div>
          <div className="ps-panel-name">{fn.name}()</div>
          <div className="ps-panel-contract">{fn.contract} · {shortAddr(fn.address)}</div>
        </div>
        <button className="ps-panel-close" onClick={onClose}>×</button>
      </div>
      <div className="ps-panel-sig">{fn.fullSig}</div>
      {fn.action && <div className="ps-panel-action">{fn.action}</div>}
      {fn.effects?.length > 0 && (
        <div className="ps-panel-effects">
          {fn.effects.map((e, i) => (
            <span key={i} className="ps-panel-pill" style={{
              background: (e.includes("impl") ? "#EC4899" : e.includes("owner") ? "#FF6B35" : "#777") + "18",
              color: e.includes("impl") ? "#EC4899" : e.includes("owner") ? "#FF6B35" : "#777",
            }}>{e}</span>
          ))}
        </div>
      )}
      <div className="ps-panel-section">Guard Chain</div>
      <div className="ps-panel-chain">
        {fn.guardChain.length === 0 ? (
          <span style={{ color: "#777", fontSize: 10 }}>public — no guards</span>
        ) : fn.guardChain.map((step, i) => (
          <span key={i} className="ps-chain-step">
            {i > 0 && <span className="ps-chain-arrow">→</span>}
            <span className="ps-panel-pill" style={{ background: step.color + "18", color: step.color }}>{step.label}</span>
          </span>
        ))}
      </div>
      <div className="ps-panel-section">Severity</div>
      <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
        <span style={{ width: 10, height: 10, borderRadius: 2, background: SEV_COLORS[fn.sev] }} />
        <span style={{ color: SEV_COLORS[fn.sev], fontSize: 10, fontWeight: 600 }}>
          {fn.sev === 3 ? "Critical" : fn.sev === 2 ? "High" : "Standard"}
        </span>
      </div>
    </div>
  );
}

// ── Main component ───────────────────────────────────────────────────────────

export default function ProtocolSurface({ companyName }) {
  const [companyData, setCompanyData] = useState(null);
  const [functionData, setFunctionData] = useState({});
  const [error, setError] = useState(null);
  const [hoveredFn, setHoveredFn] = useState(null);
  const [selectedFn, setSelectedFn] = useState(null);
  const svgRef = useRef(null);
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 1600, h: 900 });
  const [dragging, setDragging] = useState(false);
  const [dragStart, setDragStart] = useState(null);

  useEffect(() => {
    if (!companyName) return;
    let cancelled = false;
    async function load() {
      try {
        const res = await fetch(`/api/company/${encodeURIComponent(companyName)}`);
        if (!res.ok) throw new Error("Failed");
        const data = await res.json();
        if (cancelled) return;
        setCompanyData(data);

        const fnData = {};
        for (const c of data.contracts) {
          if (!c.job_id) continue;
          try {
            const lookupId = c.impl_job_id || c.job_id;
            const r = await fetch(`/api/analyses/${lookupId}/artifact/effective_permissions`);
            if (r.ok) {
              const p = await r.json();
              fnData[c.address] = p.functions || [];
            }
          } catch {}
        }
        if (!cancelled) setFunctionData(fnData);
      } catch (e) {
        if (!cancelled) setError(e.message);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [companyName]);

  const viz = companyData ? buildVisualization(companyData, functionData) : null;

  // Pan
  const handleMouseDown = useCallback((e) => {
    setDragging(true);
    setDragStart({ x: e.clientX, y: e.clientY, vx: viewBox.x, vy: viewBox.y });
  }, [viewBox]);

  const handleMouseMove = useCallback((e) => {
    if (!dragging || !dragStart) return;
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const scale = viewBox.w / rect.width;
    setViewBox((v) => ({
      ...v,
      x: dragStart.vx - (e.clientX - dragStart.x) * scale,
      y: dragStart.vy - (e.clientY - dragStart.y) * scale,
    }));
  }, [dragging, dragStart, viewBox.w]);

  const handleMouseUp = useCallback(() => {
    setDragging(false);
    setDragStart(null);
  }, []);

  // Zoom
  const handleWheel = useCallback((e) => {
    e.preventDefault();
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const factor = e.deltaY < 0 ? 0.88 : 1.14;
    const mx = ((e.clientX - rect.left) / rect.width) * viewBox.w + viewBox.x;
    const my = ((e.clientY - rect.top) / rect.height) * viewBox.h + viewBox.y;
    const nw = viewBox.w * factor;
    const nh = viewBox.h * factor;
    setViewBox({
      x: mx - (mx - viewBox.x) * factor,
      y: my - (my - viewBox.y) * factor,
      w: nw,
      h: nh,
    });
  }, [viewBox]);

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;
    svg.addEventListener("wheel", handleWheel, { passive: false });
    return () => svg.removeEventListener("wheel", handleWheel);
  }, [handleWheel]);

  if (error) return <p className="empty">Failed: {error}</p>;
  if (!viz) return <p className="empty">Loading surface...</p>;

  return (
    <div className="ps-container">
      <svg
        ref={svgRef}
        className="ps-svg"
        viewBox={`${viewBox.x} ${viewBox.y} ${viewBox.w} ${viewBox.h}`}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        <rect x={viewBox.x - 1000} y={viewBox.y - 1000} width={viewBox.w + 2000} height={viewBox.h + 2000} fill="#1A1A1A" />

        {/* Grid */}
        {Array.from({ length: 60 }, (_, i) => (
          <g key={i}>
            <line x1={i * 80 - 400} y1={-400} x2={i * 80 - 400} y2={viz.totalHeight + 400}
              stroke="#2D2D2D" strokeWidth={0.5} />
            <line x1={-400} y1={i * 80 - 400} x2={viz.totalWidth + 400} y2={i * 80 - 400}
              stroke="#2D2D2D" strokeWidth={0.5} />
          </g>
        ))}

        {/* Contracts */}
        {viz.contracts.map((c) => (
          <ContractShape key={c.address} contract={c}
            hoveredFn={hoveredFn} setHoveredFn={setHoveredFn} setSelectedFn={setSelectedFn} />
        ))}

        {/* Legend */}
        <g transform={`translate(${viewBox.x + 20}, ${viewBox.y + viewBox.h - 80})`}>
          <rect x={0} y={0} width={300} height={70} rx={10} fill="#2D2D2DEE" />
          <circle cx={20} cy={20} r={4} fill="#EC4899" />
          <text x={30} y={23} fill="#EC4899" fontSize={8} fontFamily="JetBrains Mono">critical (impl upgrade, ownership)</text>
          <circle cx={20} cy={38} r={4} fill="#FF6B35" />
          <text x={30} y={41} fill="#FF6B35" fontSize={8} fontFamily="JetBrains Mono">high (pause, assets, roles)</text>
          <circle cx={20} cy={56} r={4} fill="#555" />
          <text x={30} y={59} fill="#777" fontSize={8} fontFamily="JetBrains Mono">standard (config, external calls)</text>
        </g>
      </svg>

      <SelectedPanel fn={selectedFn} onClose={() => setSelectedFn(null)} />
    </div>
  );
}
