// Fallback dependency-graph payload built purely from the machine's metadata,
// used when the stored dependency_graph_viz artifact is missing or empty.

import { isHexAddress, shortAddr } from "../format.js";

export function buildFallbackDependencyGraph(machine) {
  if (!machine?.address) return null;
  const nodes = new Map();
  const edges = [];
  const targetId = `contract:${machine.address.toLowerCase()}`;

  function addNode(address, values = {}) {
    if (!isHexAddress(address)) return null;
    const id = `contract:${address.toLowerCase()}`;
    if (!nodes.has(id)) {
      nodes.set(id, {
        id,
        address,
        label: values.label || shortAddr(address),
        type: values.type || "regular",
        source: values.source || [],
        proxy_type: values.proxy_type || null,
        is_target: Boolean(values.is_target),
        is_proxy_context: Boolean(values.is_proxy_context),
      });
    } else {
      nodes.set(id, { ...nodes.get(id), ...values });
    }
    return id;
  }

  addNode(machine.address, {
    label: machine.name || shortAddr(machine.address),
    type: machine.is_proxy ? "proxy" : "regular",
    proxy_type: machine.proxy_type,
    is_target: true,
    source: ["selected"],
  });

  if (isHexAddress(machine.implementation)) {
    const implId = addNode(machine.implementation, {
      label: `${machine.name || "Implementation"} impl`,
      type: "implementation",
      source: ["proxy"],
    });
    edges.push({
      from: targetId,
      to: implId,
      op: "DELEGATES_TO",
      function_name: machine.proxy_type || "implementation",
    });
  }

  if (isHexAddress(machine.owner)) {
    const ownerId = addNode(machine.owner, {
      label: "owner",
      type: "regular",
      source: ["owner"],
    });
    edges.push({
      from: targetId,
      to: ownerId,
      op: "STATIC_REF",
      function_name: "owner",
    });
  }

  for (const [controllerId, value] of Object.entries(machine.controllers || {})) {
    if (!isHexAddress(value)) continue;
    const controllerNodeId = addNode(value, {
      label: controllerId.replace(/^[^:]+:/, ""),
      type: "regular",
      source: ["controller"],
    });
    edges.push({
      from: targetId,
      to: controllerNodeId,
      op: "STATIC_REF",
      function_name: controllerId.split(":").pop() || "controller",
    });
  }

  const uniqueEdges = [];
  const seenEdges = new Set();
  for (const edge of edges) {
    if (!edge.from || !edge.to || edge.from === edge.to) continue;
    const key = `${edge.from}|${edge.to}|${edge.function_name}`;
    if (seenEdges.has(key)) continue;
    seenEdges.add(key);
    uniqueEdges.push(edge);
  }

  if (nodes.size <= 1 && uniqueEdges.length === 0) return null;
  return { nodes: [...nodes.values()], edges: uniqueEdges };
}
