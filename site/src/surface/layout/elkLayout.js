// Graph layout for SurfaceCanvas. Pure helpers + an ELK instance that runs
// the async layered pass. No React — but `elkLayout` is async because ELK is.

import ELK from "elkjs/lib/elk.bundled.js";

const elk = new ELK();

// Filter for intra-group edges. The canvas data tags every flow with
// the same baseline capabilities (`upgradeable`, `pause`,
// `delegatecall`) — those describe the source contract's attributes,
// not the relationship between two specific contracts, so filtering on
// them is a no-op. The actual relationship-level signals are:
//   - flowType "controller" / "controls_value" / "controls" — an
//     explicitly named control relationship
//   - the `ownership` capability — true ownership between siblings
//   - the `value-in` capability — this contract receives value from
//     the other, the more meaningful half of value flow
// Plain "principal" flows whose only caps are source-attribute tags
// don't add info beyond what group containment already encodes (the
// Safe owns all its children), so they drop. Cross-group bundles are
// NOT filtered — at the macro view we still want every connection
// counted.
function isHighSignalIntraEdge(e) {
  // Proxy→impl edges (added by the byName loop without a data field)
  // are structurally important — always keep.
  if (!e.data) return true;
  const t = e.data.flowType;
  if (t === "controller" || t === "controls_value" || t === "controls") return true;
  const caps = e.data.capabilities || [];
  return caps.includes("ownership") || caps.includes("value-in");
}

// Every principal (Safe / Timelock / EOA / proxy admin) that owns at
// least this many contracts becomes a group container. We default to 1
// — a Safe that touches a single contract still gets a labeled box,
// matching the visual the user wants for EOAs as well. Principals that
// lose every candidate child to a higher-priority owner (see
// PRINCIPAL_PRIORITY) drop off the canvas entirely; they remain
// addressable via search and sidebar.
const MIN_GROUP_SIZE = 1;

// Priority order when a contract is owned by multiple principals. Safes
// usually carry the actual signers, so they win over a timelock that
// proxies them; timelocks beat raw EOAs because the delay is the
// load-bearing protection. proxy_admin sits below EOA because it tends
// to BE an EOA wrapped in the proxy-admin contract — picking it would
// just rename the owner.
const PRINCIPAL_PRIORITY = { safe: 4, timelock: 3, eoa: 2, proxy_admin: 1 };

export function hierarchicalLayout(machines, edgePairs) {
  const n = machines.length;
  if (n === 0) return [];
  if (n === 1) return [{ x: 0, y: 0 }];

  // Build directed adjacency: from → Set<to> (controller → target)
  const addrToIdx = new Map();
  machines.forEach((m, i) => addrToIdx.set(m.address?.toLowerCase(), i));

  const children = new Map(); // idx → Set<idx>  (who this node controls)
  const parents = new Map();  // idx → Set<idx>  (who controls this node)
  for (let i = 0; i < n; i++) { children.set(i, new Set()); parents.set(i, new Set()); }

  for (const [from, to] of edgePairs) {
    const fi = addrToIdx.get(from);
    const ti = addrToIdx.get(to);
    if (fi !== undefined && ti !== undefined && fi !== ti) {
      children.get(fi).add(ti);
      parents.get(ti).add(fi);
    }
  }

  // Assign tiers via BFS from roots (nodes with no parents)
  const tier = new Array(n).fill(-1);
  const roots = [];
  for (let i = 0; i < n; i++) {
    if (parents.get(i).size === 0) roots.push(i);
  }
  // If no roots (cycles), pick the node with most children
  if (roots.length === 0) {
    let best = 0;
    for (let i = 1; i < n; i++) {
      if (children.get(i).size > children.get(best).size) best = i;
    }
    roots.push(best);
  }

  const queue = [...roots];
  for (const r of roots) tier[r] = 0;

  const MAX_TIER = 20;
  while (queue.length > 0) {
    const curr = queue.shift();
    const nextTier = tier[curr] + 1;
    if (nextTier > MAX_TIER) continue;
    for (const child of children.get(curr)) {
      if (tier[child] < nextTier) {
        tier[child] = nextTier;
        queue.push(child);
      }
    }
  }

  // Unconnected nodes get their own tier at the bottom
  const maxTier = Math.max(0, ...tier.filter((t) => t >= 0));
  for (let i = 0; i < n; i++) {
    if (tier[i] < 0) tier[i] = maxTier + 1;
  }

  // Group nodes by tier
  const tiers = new Map();
  for (let i = 0; i < n; i++) {
    if (!tiers.has(tier[i])) tiers.set(tier[i], []);
    tiers.get(tier[i]).push(i);
  }

  // Score each node by influence
  const outCount = new Array(n).fill(0);
  const inCount = new Array(n).fill(0);
  const hasEdge = new Set();
  for (const [from, to] of edgePairs) {
    const fi = addrToIdx.get(from);
    const ti = addrToIdx.get(to);
    if (fi !== undefined) { outCount[fi]++; hasEdge.add(fi); }
    if (ti !== undefined) { inCount[ti]++; hasEdge.add(ti); }
  }

  // Split connected vs isolated
  const connected = [];
  const isolated = [];
  for (let i = 0; i < n; i++) {
    if (hasEdge.has(i)) connected.push(i);
    else isolated.push(i);
  }

  // Rank connected by influence (more outgoing = higher)
  connected.sort((a, b) => {
    const sa = outCount[a] - inCount[a];
    const sb = outCount[b] - inCount[b];
    if (sb !== sa) return sb - sa;
    return outCount[b] - outCount[a];
  });

  const NODE_W = 250;
  const NODE_H = 160;
  // Scale columns based on node count — more nodes = wider layout
  const colCount = n <= 9 ? 3 : n <= 20 ? 4 : 5;
  const spread = NODE_W * 1.15;
  const positions = new Array(n);

  // Connected nodes: multi-column stagger, spreading wider as we go down
  for (let rank = 0; rank < connected.length; rank++) {
    const idx = connected[rank];
    const col = rank % colCount;
    const row = Math.floor(rank / colCount);
    const rowSpread = spread * (1 + row * 0.08);
    let x, y;
    y = row * NODE_H;
    // Spread columns evenly around center
    const colOffset = (col - (colCount - 1) / 2) * rowSpread;
    // Deterministic jitter (subtle)
    const jx = ((rank * 7 + 13) % 30 - 15);
    const jy = ((rank * 11 + 7) % 16 - 8);
    x = colOffset + jx;
    y += jy;
    positions[idx] = { x: Math.round(x), y: Math.round(y) };
  }

  // Isolated nodes: ellipse ring around the connected core
  if (isolated.length > 0) {
    const cxs = connected.map((i) => positions[i].x);
    const cys = connected.map((i) => positions[i].y);
    const cx = connected.length > 0 ? (Math.min(...cxs) + Math.max(...cxs)) / 2 : 0;
    const cy = connected.length > 0 ? (Math.min(...cys) + Math.max(...cys)) / 2 : 0;
    const rx = connected.length > 0 ? (Math.max(...cxs) - Math.min(...cxs)) / 2 + NODE_W * 1.5 : NODE_W * 2;
    const ry = connected.length > 0 ? (Math.max(...cys) - Math.min(...cys)) / 2 + NODE_H * 1.3 : NODE_H * 2;

    for (let i = 0; i < isolated.length; i++) {
      const angle = (2 * Math.PI * i) / isolated.length - Math.PI / 2;
      positions[isolated[i]] = {
        x: Math.round(cx + Math.cos(angle) * rx),
        y: Math.round(cy + Math.sin(angle) * ry),
      };
    }
  }

  return positions;
}

// Compute the contract→group assignment used by both buildGraphLayout
// (for node parentId + edge filtering) and elkLayout (for ELK compound
// children). A contract joins a principal's group iff that principal:
//   1. Controls the contract (it's in principal.controls).
//   2. Controls at least MIN_GROUP_SIZE contracts overall (small groups
//      add visual chrome without reducing clutter).
//   3. Wins the priority tie among all principals that satisfy (1)+(2):
//      Safe > Timelock > EOA > ProxyAdmin, broken by larger group size.
//
// Returns:
//   contractToGroup: Map<contractAddr_lc, principalAddr_lc>
//   groupChildren:   Map<principalAddr_lc, contractAddr_lc[]>
//   groupedPrincipals: Set<principalAddr_lc> (principals materialized as a group)
export function assignGroups(machines, principals) {
  const contractAddrs = new Set();
  for (const m of machines) {
    if (m.address) contractAddrs.add(m.address.toLowerCase());
  }
  const principalByAddr = new Map();
  for (const p of principals || []) {
    if (p.address) principalByAddr.set(p.address.toLowerCase(), p);
  }

  // For each principal, the subset of contracts in this protocol's
  // canvas that it controls.
  const principalOwned = new Map();
  for (const [addr, p] of principalByAddr) {
    const owned = new Set();
    for (const c of p.controls || []) {
      const lc = c?.toLowerCase();
      if (lc && contractAddrs.has(lc) && lc !== addr) owned.add(lc);
    }
    principalOwned.set(addr, owned);
  }

  // Per contract, pick the best (highest-priority, then largest) principal
  // that controls it AND has at least MIN_GROUP_SIZE total controlled.
  const contractToGroup = new Map();
  for (const contractAddr of contractAddrs) {
    let best = null;
    for (const [principalAddr, owned] of principalOwned) {
      if (!owned.has(contractAddr)) continue;
      if (owned.size < MIN_GROUP_SIZE) continue;
      const p = principalByAddr.get(principalAddr);
      const priority = PRINCIPAL_PRIORITY[p?.type] || 0;
      if (
        !best
        || priority > best.priority
        || (priority === best.priority && owned.size > best.size)
      ) {
        best = { principalAddr, priority, size: owned.size };
      }
    }
    if (best) contractToGroup.set(contractAddr, best.principalAddr);
  }

  const groupChildren = new Map();
  for (const [contractAddr, principalAddr] of contractToGroup) {
    if (!groupChildren.has(principalAddr)) groupChildren.set(principalAddr, []);
    groupChildren.get(principalAddr).push(contractAddr);
  }
  // A principal that loses all its candidate children to higher-priority
  // owners (e.g. an EOA whose 3 contracts are all also Safe-owned) ends
  // up with zero children — drop the group so we don't render an empty
  // box, and the principal falls back to a standalone PrincipalNode.
  for (const [addr, kids] of Array.from(groupChildren.entries())) {
    if (kids.length < MIN_GROUP_SIZE) {
      groupChildren.delete(addr);
      for (const k of kids) contractToGroup.delete(k);
    }
  }

  return {
    contractToGroup,
    groupChildren,
    groupedPrincipals: new Set(groupChildren.keys()),
  };
}

export function buildGraphLayout(machines, fundFlows, principals) {
  const sorted = [...machines].sort((a, b) => b.totalFunctions - a.totalFunctions);
  const principalList = principals || [];
  const principalByAddr = new Map();
  for (const p of principalList) {
    if (p.address) principalByAddr.set(p.address.toLowerCase(), p);
  }

  const { contractToGroup, groupChildren, groupedPrincipals } = assignGroups(sorted, principalList);

  // Layout contracts only — principals get positioned relative to what they control
  const contractEntities = sorted.map((m) => ({ address: m.address?.toLowerCase(), kind: "contract" }));

  // Collect contract-to-contract edge pairs
  const edgePairs = [];
  const byName = new Map();
  for (const m of sorted) {
    if (!m.name) continue;
    if (!byName.has(m.name)) byName.set(m.name, []);
    byName.get(m.name).push(m);
  }
  for (const [, group] of byName) {
    if (group.length < 2) continue;
    const proxy = group.find((g) => g.is_proxy);
    const impl = group.find((g) => !g.is_proxy);
    if (proxy && impl) edgePairs.push([proxy.address?.toLowerCase(), impl.address?.toLowerCase()]);
  }
  const contractAddrs = new Set(contractEntities.map((e) => e.address));
  const allAddrs = new Set([...contractAddrs, ...principalList.map((p) => p.address?.toLowerCase())]);
  for (const flow of fundFlows || []) {
    const from = flow.from?.toLowerCase();
    const to = flow.to?.toLowerCase();
    if (from && to && contractAddrs.has(from) && contractAddrs.has(to)) {
      edgePairs.push([from, to]);
    }
  }

  // Fallback positions (only used if ELK fails) — keep the old hierarchical
  // layout for that path; it doesn't understand groups but it never
  // renders unless ELK errors out.
  const fallbackPositions = hierarchicalLayout(contractEntities, edgePairs);
  const contractPositions = new Map();

  // Total USD per group, so the group header can show a single TVL number
  // instead of every child having to be inspected. Mirrors what the
  // `Has Funds` search mode and the bottom-of-card balance line use.
  const groupTotalUsd = new Map();
  for (const [principalAddr, kids] of groupChildren) {
    let total = 0;
    for (const kid of kids) {
      const m = sorted.find((x) => x.address?.toLowerCase() === kid);
      if (m && m.total_usd) total += m.total_usd;
    }
    if (total > 0) groupTotalUsd.set(principalAddr, total);
  }

  // Build group container nodes first — React Flow needs the parent
  // in the array before its children for stable rendering.
  const nodes = [];
  for (const [principalAddr, kids] of groupChildren) {
    const p = principalByAddr.get(principalAddr);
    if (!p) continue;
    nodes.push({
      id: p.address,
      type: "group",
      position: { x: 0, y: 0 },
      // ELK fills these in; the placeholder keeps React Flow happy on
      // the first render before the async layout resolves.
      style: { width: 400, height: 200 },
      data: {
        principal: p,
        childCount: kids.length,
        totalUsd: groupTotalUsd.get(principalAddr) || 0,
      },
    });
  }

  // Contract nodes
  for (let i = 0; i < sorted.length; i++) {
    const m = sorted[i];
    const pos = fallbackPositions[i] || { x: 0, y: 0 };
    contractPositions.set(m.address?.toLowerCase(), pos);
    const groupAddr = contractToGroup.get(m.address?.toLowerCase());
    const node = {
      id: m.address,
      type: "contract",
      position: pos,
      data: { machine: m },
    };
    if (groupAddr) {
      // The principal's original-cased address is what we used as the
      // group node's id — find it so React Flow's parent lookup matches.
      const principalCanonical = principalByAddr.get(groupAddr)?.address || groupAddr;
      node.parentId = principalCanonical;
      node.extent = "parent";
    }
    nodes.push(node);
  }

  // Every non-contract principal now lives as a group container or not
  // at all — we no longer render the dashed standalone PrincipalNode.
  // Principals that lost their candidate children to higher-priority
  // owners simply disappear from the canvas; the sidebar and search
  // still expose them via companyData.principals.

  const edges = [];
  for (const [, group] of byName) {
    if (group.length < 2) continue;
    const proxy = group.find((g) => g.is_proxy);
    const impl = group.find((g) => !g.is_proxy);
    if (proxy && impl) {
      edges.push({
        id: `${proxy.address}-${impl.address}`,
        source: proxy.address,
        target: impl.address,
        sourceHandle: "ctrl-out",
        targetHandle: "ctrl-in",
        type: "smoothstep",
        style: { stroke: "#64748b", strokeWidth: 1 },
        animated: false,
      });
    }
  }

  // Fund flow / control edges with semantic handle routing. Any edge
  // whose source is a non-contract principal is silently dropped — the
  // ownership relationship now lives in the group containment, and the
  // cross-group principal fanout was the dominant source of canvas
  // spaghetti. Only contract→contract edges (proxy→impl, controls,
  // controller, contract-as-principal CGN edges) survive.
  const LANE_HANDLES = {
    control: { sourceHandle: "ctrl-out", targetHandle: "ctrl-in" },
    inflow:  { sourceHandle: "value-out", targetHandle: "value-in" },
    outflow: { sourceHandle: "value-out", targetHandle: "value-in" },
  };
  for (const flow of fundFlows || []) {
    const from = flow.from?.toLowerCase();
    const to = flow.to?.toLowerCase();
    if (!from || !to || !allAddrs.has(from) || !allAddrs.has(to)) continue;
    if (principalByAddr.has(from)) continue;
    const edgeId = `flow-${from}-${to}`;
    if (edges.some((e) => e.id === edgeId)) continue;
    const isValue = flow.type === "controls_value";
    const handles = LANE_HANDLES[flow.lane || "control"] || LANE_HANDLES.control;
    edges.push({
      id: edgeId,
      source: from,
      target: to,
      sourceHandle: handles.sourceHandle,
      targetHandle: handles.targetHandle,
      type: "smoothstep",
      style: { stroke: isValue ? "#7fc4b6" : "#94a3b8", strokeWidth: isValue ? 1.5 : 1 },
      animated: false,
      data: { capabilities: (flow.capabilities || []).slice(0, 3), flowType: flow.type },
    });
  }

  // Split intra-group edges out of the aggregation pass — they're
  // what gives each box its caller→callee hierarchy when rendered
  // inside the group, and we don't want them bundled away.
  const intraGroupEdgesByGroup = new Map();
  const crossGroupEdges = [];
  for (const e of edges) {
    const fromLc = (e.source || "").toLowerCase();
    const toLc = (e.target || "").toLowerCase();
    const fromGroup = contractToGroup.get(fromLc);
    const toGroup = contractToGroup.get(toLc);
    if (fromGroup && toGroup && fromGroup === toGroup) {
      if (!intraGroupEdgesByGroup.has(fromGroup)) intraGroupEdgesByGroup.set(fromGroup, []);
      intraGroupEdgesByGroup.get(fromGroup).push(e);
    } else {
      crossGroupEdges.push(e);
    }
  }

  const aggregatedCrossEdges = aggregateEdges(crossGroupEdges, contractToGroup, principalList, sorted);

  // Filter intra-group edges down to high-signal ones (control
  // effects, value transfers, proxy→impl) before aggregation. With 15
  // children each touching ~10 others, the interior reads as
  // spaghetti when every read/view flow gets a line. Keeping only the
  // structurally meaningful edges drops it to a manageable handful
  // per contract without losing what someone auditing the protocol
  // cares about.
  //
  // Then aggregate the remaining intra-group edges the same way we
  // do cross-group ones. aggregateEdges' endpoint() resolves a
  // contract to its group when given a populated contractToGroup —
  // which would collapse every child↔child pair into a same-group
  // self-loop and drop the whole batch. Handing it an empty map
  // preserves the raw contract addresses so each (childA, childB)
  // pair collapses to one bundle with a count label, mirroring the
  // outside-the-groups view.
  const NO_GROUP_RESOLVE = new Map();
  const aggregatedIntraByGroup = new Map();
  const intraGroupRendered = [];
  for (const [groupAddr, list] of intraGroupEdgesByGroup) {
    const filtered = list.filter(isHighSignalIntraEdge);
    const aggregated = aggregateEdges(filtered, NO_GROUP_RESOLVE, principalList, sorted);
    aggregatedIntraByGroup.set(groupAddr, aggregated);
    for (const e of aggregated) {
      intraGroupRendered.push({
        ...e,
        data: { ...(e.data || {}), intraGroup: true },
      });
    }
  }
  const finalEdges = [...intraGroupRendered, ...aggregatedCrossEdges];
  return {
    nodes,
    edges: finalEdges,
    groupChildren,
    contractToGroup,
    rawEdges: edges,
    intraGroupEdgesByGroup: aggregatedIntraByGroup,
  };
}

// Collapse the raw edge list into one bundle per (endpoint-group,
// endpoint-group) pair. The "endpoint" for an address is its
// containing group's id if it's a child of a group, otherwise the
// address itself. Intra-group edges (both endpoints resolve to the
// same group) and self-loops get dropped entirely — they're invisible
// at the macro view we're optimising for.
//
// The bundle preserves the underlying sample list under `data.samples`
// so selection-dimming in SurfaceCanvas can drill back into which
// specific contract→contract pair lit up. Width grows logarithmically
// with the bundled count so a 20-edge bundle reads heavier than a
// 2-edge bundle without dwarfing the canvas.
export function aggregateEdges(rawEdges, contractToGroup, principalList, machines) {
  const canonicalByLc = new Map();
  for (const m of machines || []) {
    if (m.address) canonicalByLc.set(m.address.toLowerCase(), m.address);
  }
  for (const p of principalList || []) {
    if (p.address) canonicalByLc.set(p.address.toLowerCase(), p.address);
  }

  // Set of addresses that render as a GroupNode. Those nodes only
  // carry ctrl-in (top) / ctrl-out (bottom) handles — if an
  // aggregated value-flow edge keeps its original value-in/value-out
  // handle on a group endpoint, React Flow can't resolve it and falls
  // back to the node centre, drawing the edge from somewhere inside
  // the container. Force ctrl handles for group endpoints to fix that.
  const groupAddrs = new Set();
  for (const g of contractToGroup.values()) {
    if (g) groupAddrs.add(String(g).toLowerCase());
  }

  function endpoint(lcAddr) {
    const g = contractToGroup.get(lcAddr);
    if (g) return canonicalByLc.get(g) || g;
    return canonicalByLc.get(lcAddr) || lcAddr;
  }

  const bundles = new Map();
  for (const e of rawEdges) {
    const fromLc = (e.source || "").toLowerCase();
    const toLc = (e.target || "").toLowerCase();
    const fromEnd = endpoint(fromLc);
    const toEnd = endpoint(toLc);
    if (fromEnd.toLowerCase() === toEnd.toLowerCase()) continue;

    const srcHandle = groupAddrs.has(fromEnd.toLowerCase()) ? "ctrl-out" : e.sourceHandle;
    const tgtHandle = groupAddrs.has(toEnd.toLowerCase()) ? "ctrl-in" : e.targetHandle;

    const key = `${fromEnd.toLowerCase()}->${toEnd.toLowerCase()}`;
    if (!bundles.has(key)) {
      bundles.set(key, {
        source: fromEnd,
        target: toEnd,
        samples: [],
        sourceHandle: srcHandle,
        targetHandle: tgtHandle,
        hasValue: false,
      });
    }
    const b = bundles.get(key);
    b.samples.push(e);
    if (e.data?.flowType === "controls_value") b.hasValue = true;
  }

  const out = [];
  for (const [, b] of bundles) {
    const count = b.samples.length;
    const isBundle = count > 1;
    const width = isBundle ? Math.min(4, 1 + Math.log2(count)) : 1;
    out.push({
      id: `agg-${b.source}-${b.target}`,
      source: b.source,
      target: b.target,
      sourceHandle: b.sourceHandle,
      targetHandle: b.targetHandle,
      type: "channeled",
      style: {
        stroke: b.hasValue ? "#7fc4b6" : "#94a3b8",
        strokeWidth: width,
      },
      animated: false,
      label: isBundle ? String(count) : "",
      labelStyle: { fill: "#f8fafc", fontSize: 13, fontWeight: 800 },
      labelBgStyle: { fill: "#0f1218", fillOpacity: 0.95 },
      labelBgPadding: [4, 7],
      labelBgBorderRadius: 5,
      data: {
        count,
        aggregated: isBundle,
        flowType: b.samples[0]?.data?.flowType,
        capabilities: Array.from(
          new Set(b.samples.flatMap((s) => s.data?.capabilities || [])),
        ).slice(0, 3),
        samples: b.samples.map((s) => ({ from: s.source, to: s.target })),
      },
    });
  }
  return out;
}

// Handle id → axis the side runs along. Used by assignEdgeLanes to
// know whether to compare endpoints by x or y when sorting members of
// a single side bucket. Keep this in sync with the Handle <Position>
// in ContractNode / GroupNode / PrincipalNode.
const HANDLE_AXIS = {
  "ctrl-in": "x",   // Position.Top
  "ctrl-out": "x",  // Position.Bottom
  "value-in": "y",  // Position.Left
  "value-out": "y", // Position.Right
};

// After ELK has positioned every node, group edges by the (node,
// handle) side they exit / enter and assign each one a lane index so
// that the custom ChanneledStepEdge can fan them out across the side
// rather than stacking on the handle centre. Lane 0 is centred, ±1 is
// one slot away, etc.
//
// All members of a single bucket live in the same coordinate space —
// either both top-level (cross-group bundles) or both children of the
// same group (intra-group bundles). So a raw position.x/y comparison
// is enough; we don't need to walk parent chains.
export function assignEdgeLanes(nodes, edges) {
  const nodeById = new Map();
  for (const n of nodes) nodeById.set(n.id, n);

  const buckets = new Map();
  function add(nodeId, handle, edgeId, role, otherId) {
    const key = `${nodeId}|${handle || ""}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push({ edgeId, role, otherId });
  }
  for (const e of edges) {
    add(e.source, e.sourceHandle, e.id, "source", e.target);
    add(e.target, e.targetHandle, e.id, "target", e.source);
  }

  const laneByEdge = new Map();
  for (const [key, members] of buckets) {
    const handle = key.split("|")[1];
    const axis = HANDLE_AXIS[handle] || "x";
    members.sort((m1, m2) => {
      const a = nodeById.get(m1.otherId);
      const b = nodeById.get(m2.otherId);
      return ((a?.position?.[axis]) || 0) - ((b?.position?.[axis]) || 0);
    });
    const n = members.length;
    members.forEach((m, i) => {
      const lane = n <= 1 ? 0 : i - (n - 1) / 2;
      const entry = laneByEdge.get(m.edgeId) || {};
      entry[m.role] = lane;
      laneByEdge.set(m.edgeId, entry);
    });
  }

  return edges.map((e) => {
    const lanes = laneByEdge.get(e.id);
    if (!lanes) return e;
    return {
      ...e,
      data: {
        ...(e.data || {}),
        sourceLane: lanes.source || 0,
        targetLane: lanes.target || 0,
      },
    };
  });
}

// Group container chrome sizing. Padding leaves room for the ~46px
// header strip plus visible breathing room between the colored border
// and the children inside. Child dimensions are sized a touch larger
// than the .ps-node CSS naturally renders so rectpacking gives each
// card its own column of slack — without this, cards on neighbouring
// rows in dense groups visually butt up against each other.
const GROUP_PADDING_TOP = 60;
const GROUP_PADDING_SIDE = 24;
const GROUP_PADDING_BOTTOM = 24;
// Child cell sized for the widest contract card the .ps-node CSS
// actually renders (no max-width; long names like
// "EtherFiRedemptionManager" / "AuctionManager" stretch the card to
// ~250px). Telling the grid 200px and getting a 250px card back is
// what was making adjacent cells overlap. CHILD_H stays larger than
// the actual rendered height so vertical spacing has slack for the
// occasional double-line card.
const CHILD_W = 260;
const CHILD_H = 130;
const PRINCIPAL_W = 140;
const PRINCIPAL_H = 60;

// In-group band assignment. Three bands top-to-bottom:
//   0 = control surfaces — timelocks, role admins, governance contracts.
//   1 = value-bearing — value handlers, bridges, value-moving tokens.
//   2 = interfaces & plumbing — pure tokens, factories, utility.
// Position carries meaning: a glance tells you which contracts hold
// authority vs hold value vs are interface/plumbing, regardless of which
// protocol you're looking at.
//
// We can't rely on `role` alone. The backend classifier puts any
// contract with asset_pull / asset_send into `value_handler`, which
// captures TimelockController-style admins that also execute value
// moves during their queued operations. Has-timelock and
// control_model=governance are the more reliable "this contract issues
// commands" signals, so they win over `role` when both fire.
function bandFor(m) {
  if (!m) return 2;
  if (m.has_timelock) return 0;
  if (m.control_model === "governance") return 0;
  if (m.role === "governance") return 0;
  // Name override: the backend role classifier and the has_timelock
  // flag both fail to surface contracts that ARE themselves a control
  // surface (TimelockController, BoringGovernance, etc.) when those
  // contracts also have asset-move side effects — they end up tagged
  // value_handler and lose to the band-1 catchall. Pattern-match the
  // name so they float to the top. Has-timelock is a property
  // assigned to contracts *controlled by* a timelock, not the timelock
  // itself, which is why we need the explicit name check.
  const nameLower = (m.name || "").toLowerCase();
  if (/timelock|governance|guardian/.test(nameLower)) return 0;
  if (m.role === "token" || m.role === "factory" || m.role === "utility") return 2;
  return 1;
}

// Spacing between children inside a group. Roomier than the obvious
// minimum because:
//   - tight gaps make the bundled edge trunks visually merge with
//     adjacent cells; ~50px of horizontal slack keeps the cabling
//     readable as it forks across the band.
//   - selecting a contract reveals capability labels on its edges; a
//     larger V_GAP gives those labels somewhere to land without
//     overlapping cards in the next row.
const CHILD_H_GAP = 50;
const CHILD_V_GAP = 70;

// Within-group child layout: bucket by role band, sort each band by
// TVL desc, pack each band into a grid centred horizontally so the
// whole group reads as control-on-top / value-in-middle / interfaces-
// at-bottom regardless of which contracts happen to be in it.
//
// Returns: { positions: Map<address, {x,y}>, width, height }
// All coords are relative to the group container's origin; React Flow
// adds the parent offset.
export function layoutGroupInterior(kids, machines) {
  if (!kids || kids.length === 0) {
    return { positions: new Map(), width: CHILD_W + 2 * GROUP_PADDING_SIDE, height: GROUP_PADDING_TOP + GROUP_PADDING_BOTTOM };
  }

  const machineByAddr = new Map();
  for (const m of machines || []) {
    if (m.address) machineByAddr.set(m.address.toLowerCase(), m);
  }

  // Bucket into bands using the multi-signal classifier (role +
  // has_timelock + control_model). See bandFor for why role alone isn't
  // enough.
  const bands = [[], [], []];
  for (const kid of kids) {
    const m = machineByAddr.get(kid.id?.toLowerCase());
    bands[bandFor(m)].push({
      id: kid.id,
      tvl: m?.total_usd || 0,
      name: m?.name || kid.id || "",
    });
  }

  // Sort each band: TVL desc → name asc. TVL surfaces money-holding
  // contracts at the front of their row; name tie-break keeps the
  // layout stable across re-renders.
  for (const list of bands) {
    list.sort((a, b) => (b.tvl - a.tvl) || a.name.localeCompare(b.name));
  }

  // Pick a column count for each band that keeps the band's aspect
  // close to the 1.6 target the outer rectpacking uses. For small
  // bands (≤4 items) prefer a single row — squashing 3 contracts into
  // 2x2 feels arbitrary.
  function chooseCols(count) {
    if (count <= 4) return count;
    return Math.max(1, Math.floor(Math.sqrt(count * 1.6)));
  }

  const bandPlans = bands.map((list) => {
    if (list.length === 0) return null;
    const cols = chooseCols(list.length);
    const rows = Math.ceil(list.length / cols);
    return {
      list,
      cols,
      rows,
      width: cols * CHILD_W + (cols - 1) * CHILD_H_GAP,
      height: rows * CHILD_H + (rows - 1) * CHILD_V_GAP,
    };
  });

  // Group interior width = widest band's width. Each band gets
  // centred horizontally within that width so the layout looks
  // balanced regardless of how lopsided the role distribution is.
  const interiorW = Math.max(0, ...bandPlans.filter(Boolean).map((b) => b.width));
  const totalWidth = interiorW + 2 * GROUP_PADDING_SIDE;

  const positions = new Map();
  let curY = GROUP_PADDING_TOP;
  for (const plan of bandPlans) {
    if (!plan) continue;
    const offsetX = GROUP_PADDING_SIDE + (interiorW - plan.width) / 2;
    for (let i = 0; i < plan.list.length; i++) {
      const col = i % plan.cols;
      const row = Math.floor(i / plan.cols);
      positions.set(plan.list[i].id, {
        x: offsetX + col * (CHILD_W + CHILD_H_GAP),
        y: curY + row * (CHILD_H + CHILD_V_GAP),
      });
    }
    curY += plan.height + CHILD_V_GAP;
  }
  const totalHeight = (curY - CHILD_V_GAP) + GROUP_PADDING_BOTTOM;

  return { positions, width: totalWidth, height: totalHeight };
}

export async function elkLayout(machines, fundFlows, principals) {
  const { nodes: rawNodes, edges: rawEdges } = buildGraphLayout(machines, fundFlows, principals);

  // Split nodes into top-level vs grouped-children. ELK only sees the
  // top level now: each group is handed to it as a single sized box.
  // The inside of every group is laid out by layoutGroupInterior
  // (semantic bands: control / value / interfaces) so position carries
  // meaning regardless of how the group's children relate to each other
  // in the call graph. ELK's `layered` algorithm minimised crossings
  // but ignored role — that's what made dense Safes read as scattered.
  const childByParent = new Map();
  const topLevel = [];
  for (const n of rawNodes) {
    if (n.parentId) {
      if (!childByParent.has(n.parentId)) childByParent.set(n.parentId, []);
      childByParent.get(n.parentId).push(n);
    } else {
      topLevel.push(n);
    }
  }

  function dimsFor(n) {
    if (n.type === "principal") return { width: PRINCIPAL_W, height: PRINCIPAL_H };
    return { width: CHILD_W, height: CHILD_H };
  }

  // Pre-compute every group's interior layout. Doing this before
  // building elkChildren means the group's overall width/height — which
  // ELK uses to pack groups against each other — comes from the actual
  // role-band layout, not from ELK's own compound-layout heuristic.
  const groupInteriors = new Map();
  for (const n of topLevel) {
    if (n.type !== "group") continue;
    const kids = childByParent.get(n.id) || [];
    groupInteriors.set(n.id, layoutGroupInterior(kids, machines));
  }

  const elkChildren = topLevel.map((n) => {
    if (n.type === "group") {
      const interior = groupInteriors.get(n.id);
      return { id: n.id, width: interior.width, height: interior.height };
    }
    return { id: n.id, ...dimsFor(n) };
  });

  // ELK only does the outer rectpacking pass over groups + standalone
  // contracts. No edges fed to ELK; intra-group routing is handled
  // entirely by ChanneledStepEdge's bundled router downstream.
  const elkGraph = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "rectpacking",
      "elk.spacing.nodeNode": "140",
      "elk.aspectRatio": "1.6",
    },
    children: elkChildren,
    edges: [],
  };

  try {
    const layout = await elk.layout(elkGraph);
    const topPos = new Map();
    for (const child of layout.children || []) {
      topPos.set(child.id, { x: child.x || 0, y: child.y || 0 });
    }

    const laidOutNodes = rawNodes.map((n) => {
      if (n.parentId) {
        // Child positions come from the JS interior layout, relative
        // to the parent group's origin (React Flow adds the parent
        // offset automatically via extent="parent").
        const interior = groupInteriors.get(n.parentId);
        const pos = interior?.positions?.get(n.id) || n.position;
        return { ...n, position: pos };
      }
      const next = { ...n, position: topPos.get(n.id) || n.position };
      if (n.type === "group") {
        const interior = groupInteriors.get(n.id);
        if (interior) {
          next.style = {
            ...(n.style || {}),
            width: interior.width,
            height: interior.height,
          };
        }
      }
      return next;
    });
    const laneAdjusted = assignEdgeLanes(laidOutNodes, rawEdges);
    return { nodes: laidOutNodes, edges: attachObstacles(laneAdjusted, laidOutNodes) };
  } catch {
    // Fallback to manual positions if elk fails. Groups still get the
    // JS-computed interior dims; only the inter-group rectpacking is
    // lost (groups stack at their fallback positions).
    const laneAdjusted = assignEdgeLanes(rawNodes, rawEdges);
    return { nodes: rawNodes, edges: attachObstacles(laneAdjusted, rawNodes) };
  }
}

// Top-level node bounding boxes. ChanneledStepEdge uses these to pick
// a centerY (or centerX) that doesn't drive the edge's middle segment
// through the interior of a group container.
function collectObstacles(nodes) {
  const out = [];
  for (const n of nodes) {
    if (n.parentId) continue;
    if (n.type !== "group" && n.type !== "contract") continue;
    // Groups carry ELK-computed dims on style; contracts use the same
    // CHILD_W/CHILD_H we hand ELK for top-level layout. The rendered
    // .ps-node may be a touch smaller, but a slightly oversized
    // obstacle is fine — we'd rather route around a card than clip it.
    const w = n.style?.width ?? CHILD_W;
    const h = n.style?.height ?? CHILD_H;
    out.push({
      id: n.id,
      x: n.position?.x || 0,
      y: n.position?.y || 0,
      w,
      h,
    });
  }
  return out;
}

// Attach the right obstacle list per edge:
//   - cross-group edges get top-level groups + standalone contracts so
//     they route around other group containers
//   - intra-group edges get the OTHER children of their parent group
//     (in absolute world coords) so they don't slice through siblings
// All edges sharing the same obstacle list reference the same array so
// React Flow's prop diff doesn't churn.
function attachObstacles(edges, nodes) {
  const topLevel = collectObstacles(nodes);
  const nodeById = new Map();
  for (const n of nodes) nodeById.set(n.id, n);

  // Children grouped by parent, with positions translated to absolute
  // world coords. ReactFlow gives edge components absolute sourceX /
  // targetX, so obstacles need to match that frame.
  const siblingsByParent = new Map();
  for (const n of nodes) {
    if (!n.parentId) continue;
    const parent = nodeById.get(n.parentId);
    if (!parent) continue;
    const absX = (parent.position?.x || 0) + (n.position?.x || 0);
    const absY = (parent.position?.y || 0) + (n.position?.y || 0);
    const w = n.style?.width ?? CHILD_W;
    const h = n.style?.height ?? CHILD_H;
    if (!siblingsByParent.has(n.parentId)) siblingsByParent.set(n.parentId, []);
    siblingsByParent.get(n.parentId).push({ id: n.id, x: absX, y: absY, w, h });
  }

  return edges.map((e) => {
    let obstacles = topLevel;
    if (e.data?.intraGroup) {
      const src = nodeById.get(e.source);
      const parentId = src?.parentId;
      const siblings = parentId ? siblingsByParent.get(parentId) : null;
      if (siblings) obstacles = siblings;
    }
    return {
      ...e,
      data: { ...(e.data || {}), obstacles },
    };
  });
}
