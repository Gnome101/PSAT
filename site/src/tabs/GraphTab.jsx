import { useEffect, useMemo, useRef, useState } from "react";

import {
  ADDRESS_GRAPH_COLUMNS,
  PRINCIPAL_COLUMNS,
  buildVisualAddressGraph,
  buildVisualPermissionGraph,
  layoutVisualAddressGraph,
  layoutVisualPermissionGraph,
  wrapText,
} from "../graph.js";
import { api } from "../api/client.js";
import { formatJson } from "../router.js";
import { StatCard } from "../ui/StatCard.jsx";

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

export default function GraphTab({ detail }) {
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
