import { BaseEdge, getSmoothStepPath } from "@xyflow/react";

import { routeOrthogonal } from "./orthogonalRouter.js";

// Each edge gets a lane index (signed integer, 0 = centred on the
// handle) for its source side and its target side. assignEdgeLanes in
// elkLayout.js fills these from `data.sourceLane` / `data.targetLane`
// so that multiple edges sharing one side of a node fan out across it
// rather than stacking on top of each other.
const LANE_SPACING = 16;

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

  // Three render paths, in order of preference:
  //   1. ELK-baked waypoints (intra-group edges). ELK already routed
  //      these with full orthogonal obstacle avoidance inside the
  //      compound group during the layered pass, so we draw straight
  //      through them. If the user has dragged a node since layout,
  //      the endpoint tolerance check kicks the edge into the next
  //      branch until ELK re-runs.
  //   2. Multi-bend orthogonal router. Picks 3 or 5 waypoints so each
  //      segment clears the obstacle list. This is what keeps a
  //      cross-group edge from cutting through a Safe / Timelock group
  //      that happens to sit between its source and target columns.
  //   3. getSmoothStepPath. Last-resort fallback when neither of the
  //      above could find a clear route — preserves the previous
  //      behaviour for edges where the router gives up (e.g. mixed
  //      handle axes).
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

  let polyline = null;
  if (waypointsStillAnchored) {
    polyline = pts;
  } else {
    polyline = routeOrthogonal({
      sx, sy, tx, ty,
      sourcePos: sourcePosition,
      targetPos: targetPosition,
      obstacles,
      sourceId: source,
      targetId: target,
    });
  }

  if (polyline) {
    path = polylinePath(polyline);
    const mid = polyline[Math.floor(polyline.length / 2)];
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
