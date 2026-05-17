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
    label,
    labelStyle,
    labelBgStyle,
    labelBgPadding,
    labelBgBorderRadius,
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
  // We intentionally do NOT consult ELK-baked waypoints any more. ELK
  // routes each edge to land on the centre of the handle without bus
  // stubs, which kills the bundling visual we're going for inside
  // dense intra-group clusters. Routing every edge through
  // routeOrthogonal trades ELK's per-edge crossing minimisation for a
  // bus-pattern that reads as a cable at a glance — a worthwhile
  // tradeoff for the surface-page goal.
  let path;
  let labelX = (sx + tx) / 2;
  let labelY = (sy + ty) / 2;

  const polyline = routeOrthogonal({
    sx, sy, tx, ty,
    sourcePos: sourcePosition,
    targetPos: targetPosition,
    obstacles,
    sourceId: source,
    targetId: target,
  });

  if (polyline) {
    path = polylinePath(polyline);
    // Spread labels along the polyline using a deterministic hash of
    // the edge id rather than always placing them at the geometric
    // midpoint. Bundling makes neighbouring edges follow very similar
    // paths near the shared bus stubs, so midpoint-based placement
    // piles labels on top of each other when a contract is selected
    // and every connected edge shows its capability label. Anchoring
    // each edge at a different fraction in [0.3, 0.7] keeps labels
    // pinned in the readable middle band of the edge while spreading
    // them so the user can actually distinguish what each one says.
    const pos = labelPositionAlong(polyline, id);
    labelX = pos.x;
    labelY = pos.y;
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

// djb2-style string hash; deterministic per edge id so the label
// position is stable across renders. We only need spread, not
// cryptographic quality.
function hashString(s) {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0;
  return Math.abs(h);
}

// Pick a point along an orthogonal polyline for label placement.
// Fraction is in [0.3, 0.7] so labels stay in the readable middle
// band of the edge (not running into source/target node chrome) while
// the per-edge hash keeps neighbouring bundle members at distinct
// positions instead of stacking on the geometric midpoint.
function labelPositionAlong(polyline, edgeId) {
  const segs = polyline.length - 1;
  if (segs < 1) return polyline[0] || { x: 0, y: 0 };
  const fraction = 0.3 + 0.4 * ((hashString(edgeId || "") % 1000) / 1000);
  const t = fraction * segs;
  const idx = Math.min(segs - 1, Math.floor(t));
  const local = t - idx;
  const a = polyline[idx];
  const b = polyline[idx + 1];
  return {
    x: a.x + (b.x - a.x) * local,
    y: a.y + (b.y - a.y) * local,
  };
}
