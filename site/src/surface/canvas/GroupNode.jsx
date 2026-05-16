import { Handle, Position } from "@xyflow/react";

import { formatUsd, shortAddr } from "../format.js";
import { PRINCIPAL_COLORS } from "../meta.js";

// Container node that wraps every contract a single principal owns. The
// header acts as the principal: clicking it opens the same detail panel a
// standalone PrincipalNode would. Children render inside the container's
// bounding box via React Flow's parentId mechanism, so the visual
// containment replaces the N principal→contract edges that used to fan
// out from the principal.
export function GroupNode({ data }) {
  const p = data.principal;
  const color = PRINCIPAL_COLORS[p.type] || "#64748b";
  const owners = Array.isArray(p.details?.owners) ? p.details.owners : [];
  const threshold = p.details?.threshold;
  const delay = p.details?.delay;

  let badge = (p.type || "").toUpperCase();
  if (p.type === "safe" && threshold) badge = `${threshold}/${owners.length || "?"} SAFE`;
  else if (p.type === "timelock" && delay) {
    const d = Number(delay);
    const label = d >= 86400 ? `${Math.round(d / 86400)}d` : d >= 3600 ? `${Math.round(d / 3600)}h` : `${Math.round(d / 60)}m`;
    badge = `TL · ${label}`;
  }

  const tvl = data.totalUsd > 0 ? formatUsd(data.totalUsd) : null;

  // Append 8-digit-hex alpha bytes for the body + header tints so the
  // container reads as a Safe/EOA/Timelock/ProxyAdmin at a glance,
  // not just by the border. `14` ≈ 8% alpha for the body (subtle but
  // visible against the dark canvas) and `33` ≈ 20% for the header.
  return (
    <div
      className={`ps-group-node ps-group-${p.type}${data.focused ? " ps-group-focused" : ""}${data.selected ? " ps-group-selected" : ""}`}
      style={{
        "--principal-color": color,
        "--principal-bg": `${color}14`,
        "--principal-header-bg": `${color}33`,
      }}
    >
      {/* Edges still terminate on the group itself when a cross-group
          edge points at the principal — give it real handles so React
          Flow can route them, just like the standalone PrincipalNode. */}
      <Handle type="target" position={Position.Top} id="ctrl-in" className="ps-handle" />
      <Handle type="source" position={Position.Bottom} id="ctrl-out" className="ps-handle" />
      <div
        className="ps-group-header"
        onClick={(e) => {
          e.stopPropagation();
          if (data.onSelect) data.onSelect();
        }}
      >
        <span className="ps-group-badge" style={{ background: color + "22", color }}>
          {badge}
        </span>
        <span className="ps-group-addr">{shortAddr(p.address)}</span>
        <span className="ps-group-count">
          {data.childCount} contract{data.childCount === 1 ? "" : "s"}
          {tvl ? ` · ${tvl}` : ""}
        </span>
      </div>
    </div>
  );
}
