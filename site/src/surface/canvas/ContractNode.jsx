import { Handle, Position } from "@xyflow/react";

import { formatUsd, shortAddr } from "../format.js";
import { ROLE_META } from "../meta.js";

export function ContractNode({ data }) {
  const m = data.machine;
  const roleColor = (ROLE_META[m.role] || ROLE_META.utility).color;
  const bridgeContext = m.bridge_context;
  const staticBridgeContext = m.bridge_static_context;
  const protocolStandards = new Set(["LayerZero", "CCIP", "Wormhole", "Hyperlane", "Axelar", "Connext", "OP Stack"]);
  const bridgeProtocols = [
    ...new Set([
      ...(bridgeContext?.protocols || []),
      bridgeContext?.protocol,
      ...(staticBridgeContext?.protocols || []),
      ...(m.standards || []).filter((standard) => protocolStandards.has(standard)),
    ].filter(Boolean)),
  ];
  const activeBridge = bridgeContext?.status === "resolved" && bridgeContext?.routes?.length > 0;
  const bridgeCandidate = activeBridge || Boolean(staticBridgeContext?.is_bridge || staticBridgeContext?.protocols?.length);
  const visibleStandards = (m.standards || []).filter((standard) => (
    bridgeCandidate || !["Bridge", "LayerZero", "CCIP", "Wormhole", "Hyperlane", "Axelar", "Connext"].includes(standard)
  ));
  const chip = data.selectionChip;
  return (
    <div
      className={`ps-node${data.selected ? " ps-node-selected" : ""}${data.focused ? " ps-node-focused" : ""}`}
      style={{ borderLeftColor: roleColor }}
      onClick={data.onSelect}
    >
      {chip?.out && (
        <div className="ps-node-chip ps-node-chip--out">{chip.out}</div>
      )}
      {chip?.in && (
        <div className="ps-node-chip ps-node-chip--in">{chip.in}</div>
      )}
      <Handle type="target" position={Position.Top} id="ctrl-in" className="ps-handle" />
      <Handle type="target" position={Position.Left} id="value-in" className="ps-handle" />
      <Handle type="source" position={Position.Right} id="value-out" className="ps-handle" />
      <Handle type="source" position={Position.Bottom} id="ctrl-out" className="ps-handle" />
      <div className="ps-node-header">
        <span className="ps-node-name">{m.name || shortAddr(m.address)}</span>
      </div>
      {m.capabilities && m.capabilities.length > 0 && (
        <div className="ps-node-caps">
          {m.capabilities.map((cap) => (
            <span key={cap} className="ps-node-cap">{cap}</span>
          ))}
        </div>
      )}
      {visibleStandards.length > 0 && (
        <div className="ps-node-standards">{visibleStandards.join(" · ")}</div>
      )}
      {bridgeCandidate && (
        <div className="ps-node-bridge">
          {bridgeProtocols.join(" · ") || "Bridge"}
          {activeBridge ? ` · active routes ${bridgeContext.routes.length}` : " · static bridge"}
        </div>
      )}
      <div className="ps-node-addr">{shortAddr(m.address)}</div>
      <div className="ps-node-role" style={{ color: roleColor }}>{(ROLE_META[m.role] || ROLE_META.utility).label.replace(/s$/, "")}</div>
      {m.total_usd ? <div className="ps-node-balance">{formatUsd(m.total_usd)}</div> : null}
    </div>
  );
}
