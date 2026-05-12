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
import { ContractNode } from "./ContractNode.jsx";
import { FocusOnNode } from "./FocusOnNode.jsx";
import { PrincipalNode } from "./PrincipalNode.jsx";
import { PrincipalTourNav } from "./PrincipalTourNav.jsx";

const nodeTypes = { contract: ContractNode, principal: PrincipalNode };

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
    // Find all nodes connected to the selected node
    const connectedNodes = new Set();
    if (sel) {
      connectedNodes.add(sel);
      for (const e of initEdges) {
        const src = e.source?.toLowerCase();
        const tgt = e.target?.toLowerCase();
        if (src === sel) connectedNodes.add(tgt);
        if (tgt === sel) connectedNodes.add(src);
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
        const style = dimmed
          ? { opacity: 0.2 }
          : inAudit
          ? { boxShadow: "0 0 0 2px #22c55e, 0 0 12px rgba(34,197,94,0.55)", borderRadius: 6 }
          : {};
        return {
          ...n,
          style,
          data: {
            ...n.data,
            selected: n.id === selectedAddress,
            focused,
            // Dispatch by node kind: contract nodes carry .machine, principal
            // nodes carry .principal. This lets a click on a Safe/Timelock/EOA
            // node open its detail panel instead of doing nothing.
            onSelect: n.data.principal
              ? () => onSelectPrincipal && onSelectPrincipal(n.data.principal)
              : () => onSelectMachine(n.data.machine),
          },
        };
      })
    );

    setEdges(
      initEdges.map((e) => {
        const src = e.source?.toLowerCase();
        const tgt = e.target?.toLowerCase();
        // When audit highlight is active, fade edges that touch a
        // non-covered endpoint so the covered subgraph reads clearly.
        const edgeInAudit = hiActive && highlightedAddresses.has(src) && highlightedAddresses.has(tgt);
        const related = hiActive ? edgeInAudit : (!sel || src === sel || tgt === sel);
        const showLabel = sel && related;
        const caps = e.data?.capabilities || [];
        const labelText = showLabel ? (caps.join(", ") || e.data?.flowType || "") : "";
        return {
          ...e,
          label: labelText,
          labelStyle: { fill: "#94a3b8", fontSize: 9 },
          labelBgStyle: labelText ? { fill: "#0f1218", fillOpacity: 0.85 } : undefined,
          labelBgPadding: labelText ? [4, 6] : undefined,
          style: {
            ...e.style,
            opacity: related ? 1 : 0.08,
            strokeWidth: related && sel ? 2 : (e.style?.strokeWidth || 1),
          },
          animated: related && e.animated,
        };
      })
    );
  }, [initNodes, initEdges, selectedAddress, focusedAddress, highlightedAddresses, onSelectMachine, onSelectPrincipal]);

  return (
    <div className="ps-canvas-wrap">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
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
