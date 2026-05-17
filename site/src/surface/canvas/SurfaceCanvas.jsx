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

const nodeTypes = { contract: ContractNode, principal: PrincipalNode, group: GroupNode };
const edgeTypes = { channeled: ChanneledStepEdge };

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
    // Find all nodes connected to the selected node. Owner-grouping
    // moves the principal→contract relationship from an edge into the
    // parent/child hierarchy — so the parent group of the selected
    // node AND every child of a selected group both count as
    // "connected" even though no edge exists between them.
    const connectedNodes = new Set();
    if (sel) {
      connectedNodes.add(sel);
      // Aggregated edges carry the underlying sample list in
      // data.samples; walk those instead of the bundle endpoints so
      // clicking a child contract still lights up the actual contracts
      // it touches, not just the parent groups.
      for (const e of initEdges) {
        const samples = e.data?.samples;
        const pairs = samples && samples.length > 0
          ? samples.map((s) => [s.from?.toLowerCase(), s.to?.toLowerCase()])
          : [[e.source?.toLowerCase(), e.target?.toLowerCase()]];
        for (const [src, tgt] of pairs) {
          if (src === sel) connectedNodes.add(tgt);
          if (tgt === sel) connectedNodes.add(src);
        }
      }
      for (const n of initNodes) {
        const nid = n.id?.toLowerCase();
        const pid = n.parentId?.toLowerCase();
        if (pid === sel) connectedNodes.add(nid);
        if (nid === sel && pid) connectedNodes.add(pid);
      }
    }

    // Audit-coverage highlight takes precedence when active: non-covered
    // nodes dim, covered ones get a green ring so the user sees exactly
    // which contracts an audit touched. Falls back to the connected-node
    // dimming when no audit is selected.
    const hiActive = highlightedAddresses && highlightedAddresses.size > 0;

    const foc = focusedAddress?.toLowerCase();
    setNodes(
      initNodes.map((n) => {
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
          style,
          data: {
            ...n.data,
            selected: n.id === selectedAddress,
            focused,
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
      })
    );

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
        const caps = e.data?.capabilities || [];
        // Default label is whatever buildGraphLayout assigned (the
        // bundle count for aggregated edges). On selection we replace
        // that with the per-flow capability summary — but only for
        // edges directly attached to the selected node, so selecting a
        // whole group doesn't paint every internal wire with a label.
        const defaultLabel = e.label || "";
        const isSelectionLabel = sel && directlyConnected;
        const labelText = isSelectionLabel
          ? (caps.join(", ") || e.data?.flowType || defaultLabel)
          : defaultLabel;
        // Selection labels: tell the edge component which end is the
        // user's selected node so it can anchor the label near the
        // OTHER end (the contract whose relationship the label
        // describes). Without this, every connected edge's label
        // lands on the shared bus trunk near the selected node and
        // they pile up on top of each other.
        const selectedEnd = isSelectionLabel
          ? (src === sel ? "source" : "target")
          : undefined;
        return {
          ...e,
          label: labelText,
          data: { ...e.data, selectedEnd },
          // Selection labels render via EdgeLabelRenderer (HTML
          // overlay) so labelStyle / labelBgStyle / labelBgPadding
          // are unused for those — the chip styling lives in CSS on
          // .ps-edge-label. Bundle-count labels keep the SVG path
          // through BaseEdge.
          labelStyle: e.labelStyle || { fill: "#f8fafc", fontSize: 12, fontWeight: 700 },
          labelBgStyle: labelText
            ? (e.labelBgStyle || { fill: "#0f1218", fillOpacity: 0.95 })
            : undefined,
          labelBgPadding: labelText ? (e.labelBgPadding || [4, 7]) : undefined,
          style: {
            ...e.style,
            opacity: related ? 1 : 0.08,
            strokeWidth: related && sel ? 2 : (e.style?.strokeWidth || 1),
          },
          animated: related && e.animated,
        };
      });

    // Paint-order trick: React Flow renders edges in the order of the
    // edges array inside a single SVG layer, so any edge appearing
    // later draws over earlier ones. Push selection-labeled edges to
    // the end so their lines AND chips sit above every other edge that
    // happens to cross the label area.
    nextEdges.sort((a, b) => {
      const aSel = a.data?.selectedEnd ? 1 : 0;
      const bSel = b.data?.selectedEnd ? 1 : 0;
      return aSel - bSel;
    });
    setEdges(nextEdges);
  }, [initNodes, initEdges, selectedAddress, focusedAddress, highlightedAddresses, onSelectMachine, onSelectPrincipal]);

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
      </ReactFlow>
    </div>
  );
}
