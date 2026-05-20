import { useEffect, useState } from "react";
import {
  Background,
  Controls,
  Panel,
  ReactFlow,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";

import { elkLayout } from "../layout/elkLayout.js";
import { ChanneledStepEdge } from "./ChanneledStepEdge.jsx";
import { ContractNode } from "./ContractNode.jsx";
import { FocusOnNode } from "./FocusOnNode.jsx";
import { GroupNode } from "./GroupNode.jsx";
import { PrincipalNode } from "./PrincipalNode.jsx";
import { PrincipalTourNav } from "./PrincipalTourNav.jsx";

const CONTRACT_NODE_WIDTH = 260;
const CONTRACT_NODE_HEIGHT = 130;
const NETWORK_ZONE_PADDING = 52;
const NETWORK_ZONE_COLORS = {
  ethereum: "#38bdf8",
  mainnet: "#38bdf8",
  base: "#3ddc97",
  arbitrum: "#f59e0b",
  optimism: "#fb7185",
  polygon: "#c084fc",
  avalanche: "#f43f5e",
  bsc: "#facc15",
  linea: "#a3e635",
  scroll: "#22d3ee",
  blast: "#f97316",
};

function chainLabel(chain) {
  const value = String(chain || "").trim();
  if (!value) return "Unknown";
  const known = {
    ethereum: "Ethereum",
    mainnet: "Ethereum",
    base: "Base",
    arbitrum: "Arbitrum",
    optimism: "Optimism",
    polygon: "Polygon",
    avalanche: "Avalanche",
    bsc: "BSC",
    linea: "Linea",
    scroll: "Scroll",
    blast: "Blast",
  };
  return known[value.toLowerCase()] || value;
}

function chainColor(chain) {
  const key = String(chain || "").toLowerCase();
  if (NETWORK_ZONE_COLORS[key]) return NETWORK_ZONE_COLORS[key];
  let hash = 0;
  for (const ch of key || "unknown") hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
  const palette = ["#38bdf8", "#3ddc97", "#f59e0b", "#fb7185", "#c084fc", "#22d3ee"];
  return palette[hash % palette.length];
}

function zoneId(chain) {
  return `network-zone-${String(chain || "unknown").toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
}

function NetworkZoneNode({ data }) {
  return (
    <div className="ps-network-zone">
      <span className="ps-network-zone-label">{data.label}</span>
    </div>
  );
}

const nodeTypes = {
  contract: ContractNode,
  principal: PrincipalNode,
  group: GroupNode,
  networkZone: NetworkZoneNode,
};
const edgeTypes = { channeled: ChanneledStepEdge };

export function buildNetworkZones(layoutNodes) {
  const byId = new Map(layoutNodes.map((node) => [node.id, node]));
  const boundsByChain = new Map();

  for (const node of layoutNodes) {
    const machine = node.data?.machine;
    if (node.type !== "contract" || !machine) continue;
    const chain = machine.chain || "unknown";
    const parent = node.parentId ? byId.get(node.parentId) : null;
    const x = (parent?.position?.x || 0) + (node.position?.x || 0);
    const y = (parent?.position?.y || 0) + (node.position?.y || 0);
    const width = node.style?.width || CONTRACT_NODE_WIDTH;
    const height = node.style?.height || CONTRACT_NODE_HEIGHT;
    const current = boundsByChain.get(chain) || {
      minX: Number.POSITIVE_INFINITY,
      minY: Number.POSITIVE_INFINITY,
      maxX: Number.NEGATIVE_INFINITY,
      maxY: Number.NEGATIVE_INFINITY,
      count: 0,
    };
    current.minX = Math.min(current.minX, x);
    current.minY = Math.min(current.minY, y);
    current.maxX = Math.max(current.maxX, x + width);
    current.maxY = Math.max(current.maxY, y + height);
    current.count += 1;
    boundsByChain.set(chain, current);
  }

  return [...boundsByChain.entries()].map(([chain, bounds]) => {
    const color = chainColor(chain);
    return {
      id: zoneId(chain),
      type: "networkZone",
      position: {
        x: bounds.minX - NETWORK_ZONE_PADDING,
        y: bounds.minY - NETWORK_ZONE_PADDING,
      },
      selectable: false,
      draggable: false,
      connectable: false,
      focusable: false,
      zIndex: 0,
      style: {
        width: bounds.maxX - bounds.minX + NETWORK_ZONE_PADDING * 2,
        height: bounds.maxY - bounds.minY + NETWORK_ZONE_PADDING * 2,
        "--zone-color": color,
      },
      data: {
        label: `${chainLabel(chain)} · ${bounds.count}`,
      },
    };
  });
}

// Selection-time legend. Renders only while a contract is selected so
// the chip-color convention (warm = selected acts outward, cool =
// other acts on selected) doesn't have to be memorised — the legend
// is right there with the chips it explains. Uses "acts on" because
// the edges represent any directed relationship (controls / calls /
// sends value / owns / proxies-to); the chip text spells out which
// specifically.
function SelectionLegend() {
  return (
    <div className="ps-selection-legend">
      <div className="ps-selection-legend-row">
        <span className="ps-selection-legend-swatch ps-selection-legend-swatch--out" />
        <span>selected acts on this contract</span>
      </div>
      <div className="ps-selection-legend-row">
        <span className="ps-selection-legend-swatch ps-selection-legend-swatch--in" />
        <span>this contract acts on selected</span>
      </div>
    </div>
  );
}

export function SurfaceCanvas({ machines, fundFlows, principals, selectedAddress, focusAddress, focusedAddress, highlightedAddresses, onSelectMachine, onSelectPrincipal, principalTour, onTourGo, onTourBack }) {
  const [initNodes, setInitNodes] = useState([]);
  const [initEdges, setInitEdges] = useState([]);

  // Run elk layout (async)
  useEffect(() => {
    let cancelled = false;
    elkLayout(machines, fundFlows, principals).then(({ nodes: n, edges: e }) => {
      if (!cancelled) {
        setInitNodes(n);
        setInitEdges(e);
      }
    });
    return () => { cancelled = true; };
  }, [machines, fundFlows, principals]);

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (!initNodes.length) return;
    const sel = selectedAddress?.toLowerCase();
    // Find all nodes connected to the selected node AND, in the same
    // pass, the per-contract chip data. Owner-grouping moves the
    // principal→contract relationship from an edge into the
    // parent/child hierarchy — so the parent group of the selected
    // node AND every child of a selected group both count as
    // "connected" even though no edge exists between them.
    //
    // selectionChips: Map<addrLc, { out?: string, in?: string }> —
    // each related contract can carry up to two chips, one per
    // direction, because bidirectional relationships are common in
    // this data (101 pairs in the etherfi protocol). "out" means
    // `sel` acts on this contract, "in" means this contract acts on
    // `sel`. Renders as banners above (out) and below (in) the card,
    // restoring what the old per-edge chip layout showed when two
    // edges existed between the same pair.
    const connectedNodes = new Set();
    const selectionChips = new Map();
    if (sel) {
      connectedNodes.add(sel);
      const addChip = (addrLc, caps, direction) => {
        if (!addrLc || addrLc === sel || !caps) return;
        let entry = selectionChips.get(addrLc);
        if (!entry) {
          entry = {};
          selectionChips.set(addrLc, entry);
        }
        const existing = entry[direction];
        if (existing) {
          // Same direction seen twice — happens when the same
          // (other, sel) pair surfaces through multiple aggregated
          // bundles. Union the caps within the direction.
          const set = new Set(existing.split(", ").filter(Boolean));
          for (const c of caps.split(", ")) if (c) set.add(c);
          entry[direction] = [...set].join(", ");
        } else {
          entry[direction] = caps;
        }
      };
      // Aggregated edges carry the underlying sample list in
      // data.samples; walk those instead of the bundle endpoints so
      // clicking a child contract still lights up the actual contracts
      // it touches, not just the parent groups. Each chip's caps come
      // from its OWN sample, not the bundle's union — bundles can mix
      // flow shapes (e.g. one sample has `value-in`, another has
      // `ownership`) and a union'd chip would falsely imply every child
      // has the same relationship to the selected node.
      for (const e of initEdges) {
        const samples = e.data?.samples;
        const fallbackCaps = e.data?.capabilities || [];
        const fallbackFlowType = e.data?.flowType;
        const items = samples && samples.length > 0
          ? samples.map((s) => ({
              from: s.from?.toLowerCase(),
              to: s.to?.toLowerCase(),
              caps: s.capabilities || fallbackCaps,
              flowType: s.flowType || fallbackFlowType,
            }))
          : [{
              from: e.source?.toLowerCase(),
              to: e.target?.toLowerCase(),
              caps: fallbackCaps,
              flowType: fallbackFlowType,
            }];
        for (const { from, to, caps, flowType } of items) {
          const capsText = (caps || []).join(", ") || flowType || "";
          if (from === sel) {
            connectedNodes.add(to);
            addChip(to, capsText, "out");
          }
          if (to === sel) {
            connectedNodes.add(from);
            addChip(from, capsText, "in");
          }
        }
      }
      // Principal clicks: chips on every child the principal owns.
      // The principal-source fund-flow edges were pruned at
      // elkLayout's fundFlow loop ("if (principalByAddr.has(from))
      // continue") to keep the canvas clean, so the sample walk above
      // can't see these relationships — synthesize them from the
      // parent/child hierarchy instead, with cap text derived from the
      // principal's type (safe-controlled / timelock-controlled / ...).
      const selPrincipal = (principals || []).find(
        (p) => p.address?.toLowerCase() === sel,
      );
      for (const n of initNodes) {
        const nid = n.id?.toLowerCase();
        const pid = n.parentId?.toLowerCase();
        if (pid === sel) {
          connectedNodes.add(nid);
          if (selPrincipal) {
            addChip(nid, `${selPrincipal.type || "principal"}-controlled`, "out");
          }
        }
        if (nid === sel && pid) connectedNodes.add(pid);
      }
    }

    // Audit-coverage highlight takes precedence when active: non-covered
    // nodes dim, covered ones get a green ring so the user sees exactly
    // which contracts an audit touched. Falls back to the connected-node
    // dimming when no audit is selected.
    const hiActive = highlightedAddresses && highlightedAddresses.size > 0;

    const foc = focusedAddress?.toLowerCase();
    const nextNodes = initNodes.map((n) => {
      const nid = n.id?.toLowerCase();
      const inAudit = hiActive && highlightedAddresses.has(nid);
      const dimmed = hiActive ? !inAudit : (sel && !connectedNodes.has(nid));
      const focused = foc && nid === foc;
      // Merge — don't replace — n.style. Group containers carry
      // ELK-computed width/height in n.style and we'd otherwise blow
      // them away each time selection changes.
      const baseStyle = n.style || {};
      const style = dimmed
        ? { ...baseStyle, opacity: 0.2 }
        : inAudit
        ? { ...baseStyle, boxShadow: "0 0 0 2px #22c55e, 0 0 12px rgba(34,197,94,0.55)", borderRadius: 6 }
        : baseStyle;
      return {
        ...n,
        zIndex: n.zIndex ?? 1,
        style,
        data: {
          ...n.data,
          selected: n.id === selectedAddress,
          focused,
          selectionChip: selectionChips.get(nid) || null,
          // Dispatch by node kind: contract nodes carry .machine,
          // principal AND group nodes both carry .principal. A click on
          // a group's header (the only pointer-events-active region)
          // opens the principal detail just like a standalone
          // PrincipalNode click, so users get the same drill-in either
          // way.
          onSelect: n.data.principal
            ? () => onSelectPrincipal && onSelectPrincipal(n.data.principal)
            : () => onSelectMachine(n.data.machine),
        },
      };
    });
    setNodes([...buildNetworkZones(initNodes), ...nextNodes]);

    const nextEdges = initEdges.map((e) => {
      const src = e.source?.toLowerCase();
      const tgt = e.target?.toLowerCase();
      // Aggregated bundles already terminate at group endpoints, so
      // the simple endpoint check matches even when the underlying
      // sample edges have different addresses. The both-in-connected
      // clause keeps intra-group child↔child edges visible when the
      // group itself is selected — without it, clicking a group dims
      // every internal wire because none of them touch the group
      // address directly.
      const edgeInAudit = hiActive && highlightedAddresses.has(src) && highlightedAddresses.has(tgt);
      const directlyConnected = src === sel || tgt === sel;
      const related = hiActive
        ? edgeInAudit
        : (!sel || directlyConnected || (connectedNodes.has(src) && connectedNodes.has(tgt)));
      return {
        ...e,
        style: {
          ...e.style,
          opacity: related ? 1 : 0.08,
          strokeWidth: related && sel ? 2 : (e.style?.strokeWidth || 1),
        },
        animated: related && e.animated,
      };
    });

    setEdges(nextEdges);
  }, [initNodes, initEdges, principals, selectedAddress, focusedAddress, highlightedAddresses, onSelectMachine, onSelectPrincipal]);

  return (
    <div className="ps-canvas-wrap">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onPaneClick={() => onSelectMachine(null)}
        fitView
        minZoom={0.2}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1e293b" gap={24} size={1} />
        <Controls showInteractive={false} />
        <FocusOnNode address={focusAddress?.address} focusKey={focusAddress?.key} />
        {principalTour && principalTour.principals.length > 1 && (
          <Panel position="top-right">
            <PrincipalTourNav tour={principalTour} onGo={onTourGo} onBack={onTourBack} />
          </Panel>
        )}
        {selectedAddress && (
          <Panel position="top-center">
            <SelectionLegend />
          </Panel>
        )}
      </ReactFlow>
    </div>
  );
}
