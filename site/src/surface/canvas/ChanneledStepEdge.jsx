import { BaseEdge, getSmoothStepPath } from "@xyflow/react";

// Each edge gets a lane index (signed integer, 0 = centred on the
// handle) for its source side and its target side. assignEdgeLanes in
// elkLayout.js fills these from `data.sourceLane` / `data.targetLane`
// so that multiple edges sharing one side of a node fan out across it
// rather than stacking on top of each other.
const LANE_SPACING = 16;

// Padding from a group's edge when we have to detour around it. Big
// enough that the wire visibly sits outside the colored border, small
// enough that bundles don't sprawl.
const OBSTACLE_PADDING = 20;

// How far source/target can drift from the ELK-baked endpoint before
// we give up on the polyline waypoints. A few pixels covers rounding
// noise between ELK's port positions and ReactFlow's handle centres;
// anything larger means the user has dragged the node and we should
// recompute the path.
const ENDPOINT_TOLERANCE = 4;

const SIDE_TO_AXIS = { top: "x", bottom: "x", left: "y", right: "y" };

export function ChanneledStepEdge(props) {
  const {
    id,
    source,
    target,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    data,
    style,
    markerEnd,
    markerStart,
    label,
    labelStyle,
    labelBgStyle,
    labelBgPadding,
    labelBgBorderRadius,
    interactionWidth,
  } = props;

  const sLane = Number(data?.sourceLane || 0);
  const tLane = Number(data?.targetLane || 0);

  const sAxis = SIDE_TO_AXIS[sourcePosition];
  const tAxis = SIDE_TO_AXIS[targetPosition];

  const sx = sourceX + (sAxis === "x" ? sLane * LANE_SPACING : 0);
  const sy = sourceY + (sAxis === "y" ? sLane * LANE_SPACING : 0);
  const tx = targetX + (tAxis === "x" ? tLane * LANE_SPACING : 0);
  const ty = targetY + (tAxis === "y" ? tLane * LANE_SPACING : 0);

  // elkLayout swaps the obstacle list per edge: top-level groups for
  // cross-group bundles, siblings of the parent group for intra-group
  // ones. Both lists arrive in the same absolute world-coord frame as
  // sourceX/Y / targetX/Y, so the same routing pass handles them.
  const obstacles = data?.obstacles || [];

  let centerX;
  let centerY;
  if (sAxis === "x" && tAxis === "x") {
    centerY = pickClearCenter(sy, ty, sx, tx, obstacles, source, target, "y");
  } else if (sAxis === "y" && tAxis === "y") {
    centerY = undefined;
    centerX = pickClearCenter(sx, tx, sy, ty, obstacles, source, target, "x");
  }

  // ELK already routed intra-group edges with full orthogonal
  // obstacle avoidance (siblings are baked into its layered layout).
  // When elkLayout hands those waypoints down via data.waypoints, draw
  // a polyline through them. But the waypoints are anchored to the
  // node positions ELK saw at layout time — if the user drags a node,
  // sourceX/sourceY (or targetX/targetY) no longer match, and the
  // baked path keeps pointing at the original spot. Detect that and
  // fall back to the obstacle-aware step path so the edge moves with
  // the node.
  let path;
  let labelX = (sx + tx) / 2;
  let labelY = (sy + ty) / 2;
  const pts = data?.waypoints;
  const waypointsStillAnchored =
    pts && pts.length >= 2
    && Math.abs(pts[0].x - sourceX) < ENDPOINT_TOLERANCE
    && Math.abs(pts[0].y - sourceY) < ENDPOINT_TOLERANCE
    && Math.abs(pts[pts.length - 1].x - targetX) < ENDPOINT_TOLERANCE
    && Math.abs(pts[pts.length - 1].y - targetY) < ENDPOINT_TOLERANCE;
  if (waypointsStillAnchored) {
    path = polylinePath(pts);
    const mid = pts[Math.floor(pts.length / 2)];
    labelX = mid.x;
    labelY = mid.y;
  } else {
    const r = getSmoothStepPath({
      sourceX: sx,
      sourceY: sy,
      targetX: tx,
      targetY: ty,
      sourcePosition,
      targetPosition,
      centerX,
      centerY,
      borderRadius: 0,
    });
    path = r[0];
    labelX = r[1];
    labelY = r[2];
  }

  return (
    <BaseEdge
      id={id}
      path={path}
      labelX={labelX}
      labelY={labelY}
      label={label}
      labelStyle={labelStyle}
      labelBgStyle={labelBgStyle}
      labelBgPadding={labelBgPadding}
      labelBgBorderRadius={labelBgBorderRadius}
      style={style}
      markerEnd={markerEnd}
      markerStart={markerStart}
      interactionWidth={interactionWidth}
    />
  );
}

// SVG path with sharp 90° corners through the given orthogonal
// waypoints. ELK guarantees consecutive points share an axis, so a
// straight L between each pair is correct.
function polylinePath(pts) {
  let d = `M ${pts[0].x} ${pts[0].y}`;
  for (let i = 1; i < pts.length; i++) {
    d += ` L ${pts[i].x} ${pts[i].y}`;
  }
  return d;
}

// Pick a centre value (y for a top/bottom edge, x for a left/right
// one) such that the middle segment doesn't cut through any obstacle.
// `perpA` / `perpB` are the source / target positions on the
// PERPENDICULAR axis — the one the middle segment runs along.
// `parA` / `parB` are positions on the PARALLEL axis (the one the
// segment moves through). Returns undefined when the natural midpoint
// is already clear, so getSmoothStepPath uses its default.
function pickClearCenter(perpA, perpB, parA, parB, obstacles, sourceId, targetId, parAxis) {
  if (!obstacles.length) return undefined;
  const parMin = Math.min(parA, parB);
  const parMax = Math.max(parA, parB);
  const perpAxis = parAxis === "y" ? "x" : "y";

  function crosses(centre) {
    for (const o of obstacles) {
      if (o.id === sourceId || o.id === targetId) continue;
      // centre is on the perpendicular axis — check if it falls
      // inside the obstacle's extent on that axis.
      const lo = perpAxis === "x" ? o.x : o.y;
      const hi = perpAxis === "x" ? o.x + o.w : o.y + o.h;
      if (centre < lo || centre > hi) continue;
      // Check parallel-axis overlap with the segment's span.
      const oLo = parAxis === "x" ? o.x : o.y;
      const oHi = parAxis === "x" ? o.x + o.w : o.y + o.h;
      if (parMax < oLo || parMin > oHi) continue;
      return true;
    }
    return false;
  }

  const natural = (perpA + perpB) / 2;
  if (!crosses(natural)) return undefined;

  // Candidates: just before and just past each obstacle on the
  // perpendicular axis. The one closest to the natural midpoint that
  // clears every obstacle wins. Two passes around the centre keep the
  // detour as small as possible.
  const candidates = [];
  for (const o of obstacles) {
    if (o.id === sourceId || o.id === targetId) continue;
    const lo = perpAxis === "x" ? o.x : o.y;
    const hi = perpAxis === "x" ? o.x + o.w : o.y + o.h;
    candidates.push(lo - OBSTACLE_PADDING);
    candidates.push(hi + OBSTACLE_PADDING);
  }
  candidates.sort((a, b) => Math.abs(a - natural) - Math.abs(b - natural));
  for (const c of candidates) {
    if (!crosses(c)) return c;
  }
  return undefined;
}
