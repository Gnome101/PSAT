import { BaseEdge, getSmoothStepPath } from "@xyflow/react";

import { routeOrthogonal } from "./orthogonalRouter.js";

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
    interactionWidth,
  } = props;

  // Lane offsets are intentionally not applied. The bundling visual
  // (every edge sharing a handle traveling through the same `STUB`-px
  // perpendicular trunk before forking) only emerges when all edges
  // start at the exact same point on the handle — lane-fanning would
  // spread them across the side and break the shared-stub overlap.
  // The lane fields on `data` are still populated by assignEdgeLanes
  // for any future tooling that wants them.
  const sx = sourceX;
  const sy = sourceY;
  const tx = targetX;
  const ty = targetY;

  // elkLayout swaps the obstacle list per edge: top-level groups for
  // cross-group bundles, siblings of the parent group for intra-group
  // ones. Both lists arrive in the same absolute world-coord frame as
  // sourceX/Y / targetX/Y, so the same routing pass handles them.
  const obstacles = data?.obstacles || [];

  // Two render paths, in order of preference:
  //   1. Multi-bend orthogonal router (`routeOrthogonal`). Prefers the
  //      5-segment bus-stub shape so every edge from the same handle
  //      shares its first / last perpendicular segment — that overlap
  //      is what produces the bundled "trunk" visual. Falls back to a
  //      3-segment route when 5 can't find a clear column.
  //   2. getSmoothStepPath. Last-resort fallback when the router gives
  //      up (e.g. mixed handle axes that routeOrthogonal doesn't model).
  //
  // Edges never carry their own label any more — selection chips
  // render on the related contract nodes themselves (data.selectionChip
  // in ContractNode / GroupNode), so the geometry only has to draw the
  // path. Cable thickness at the shared trunk communicates weight.
  const polyline = routeOrthogonal({
    sx, sy, tx, ty,
    sourcePos: sourcePosition,
    targetPos: targetPosition,
    obstacles,
    sourceId: source,
    targetId: target,
  });
  const path = polyline
    ? polylinePath(polyline)
    : getSmoothStepPath({
        sourceX: sx,
        sourceY: sy,
        targetX: tx,
        targetY: ty,
        sourcePosition,
        targetPosition,
        borderRadius: 0,
      })[0];

  return (
    <BaseEdge
      id={id}
      path={path}
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
