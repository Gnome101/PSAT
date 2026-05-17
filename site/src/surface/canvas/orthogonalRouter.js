// Multi-bend orthogonal router for cross-group edges.
//
// The previous step-path approach only adjusted the *middle* horizontal /
// vertical segment to dodge obstacles — the two stub legs at the source
// and target columns were fixed at sx / tx, so any obstacle that
// happened to sit at that column was sliced through.  This router
// returns the full polyline, picking three or five waypoints as needed
// so every segment clears the obstacle list it was handed.
//
// The router only handles the same-axis cases the bundle-aggregator
// actually emits today (ctrl→ctrl and value→value).  Mixed-axis bundles
// fall through to the caller's smoothstep fallback.

// How far past a node we set the initial / final stub.  Big enough to
// clear the colored border, small enough that the second bend is still
// inside the same rectpacking lane as the source.
const STUB = 24;

// How far past an obstacle's edge we route when we have to detour.
// Mirrors the obstacle padding the step path was using.
const OBSTACLE_PADDING = 20;

export function routeOrthogonal({
  sx, sy, tx, ty,
  sourcePos, targetPos,
  obstacles,
  sourceId, targetId,
}) {
  // Keep source/target in the obstacle list so the route doesn't dive
  // through their interior — but shrink their bbox along the handle
  // axis so the handle itself sits on the boundary.  This matters when
  // a GroupNode's top handle is rendered ~10px below the group's
  // visible top (the header strip): if we filtered the group out
  // entirely the V at xm could legitimately pass through it,
  // re-entering before the terminal stub.  See the
  // EtherFi Timelock → 3/6 SAFE case for the canonical failure.
  const obs = (obstacles || []).map((o) => {
    if (o.id === sourceId) return shrinkAtHandle(o, sourcePos, sx, sy);
    if (o.id === targetId) return shrinkAtHandle(o, targetPos, tx, ty);
    return o;
  });

  const sAxisV = sourcePos === "top" || sourcePos === "bottom";
  const tAxisV = targetPos === "top" || targetPos === "bottom";

  // Prefer the 5-segment route over the 3-segment one even when 3
  // would clear. The 5-segment shape produces the bundling visual:
  // every edge leaving the same source handle shares the first
  // `STUB`-pixel perpendicular stub (since y1 = sy ± STUB depends only
  // on the handle, not the edge), and every edge arriving at the same
  // target handle shares the last `STUB`-pixel stub the same way.
  // With many edges through one handle this reads as a single thick
  // "trunk" that forks/converges at the bus columns, rather than the
  // spaghetti the 3-segment route produces when each edge picks its
  // own midline. 3-segment still serves as the fallback for cases
  // where 5-segment can't find a clear column.
  if (sAxisV && tAxisV) {
    return (
      tryFiveSegmentV(sx, sy, tx, ty, sourcePos, targetPos, obs)
      || tryThreeSegmentV(sx, sy, tx, ty, sourcePos, targetPos, obs)
    );
  }
  if (!sAxisV && !tAxisV) {
    return (
      tryFiveSegmentH(sx, sy, tx, ty, sourcePos, targetPos, obs)
      || tryThreeSegmentH(sx, sy, tx, ty, sourcePos, targetPos, obs)
    );
  }
  // Mixed-axis cross-group edges (e.g. group ctrl-out → contract
  // value-in) are rare today and never feature in the routing-bug the
  // user reported.  Leave them on the caller's smoothstep fallback.
  return null;
}

// Reshape an obstacle so the given handle position becomes the
// obstacle's boundary along the handle axis.  A noop when the handle
// is already outside the obstacle (which is the common case — only
// GroupNode's header offset triggers the actual shrink today).
function shrinkAtHandle(o, pos, hx, hy) {
  if (pos === "top" && hy > o.y) {
    return { ...o, y: hy, h: Math.max(0, o.h - (hy - o.y)) };
  }
  if (pos === "bottom" && hy < o.y + o.h) {
    return { ...o, h: Math.max(0, hy - o.y) };
  }
  if (pos === "left" && hx > o.x) {
    return { ...o, x: hx, w: Math.max(0, o.w - (hx - o.x)) };
  }
  if (pos === "right" && hx < o.x + o.w) {
    return { ...o, w: Math.max(0, hx - o.x) };
  }
  return o;
}

function segmentClear(a, b, obstacles) {
  const xLo = Math.min(a.x, b.x);
  const xHi = Math.max(a.x, b.x);
  const yLo = Math.min(a.y, b.y);
  const yHi = Math.max(a.y, b.y);
  for (const o of obstacles) {
    if (xHi <= o.x || xLo >= o.x + o.w) continue;
    if (yHi <= o.y || yLo >= o.y + o.h) continue;
    return false;
  }
  return true;
}

function polylineClear(pts, obstacles) {
  for (let i = 1; i < pts.length; i++) {
    if (!segmentClear(pts[i - 1], pts[i], obstacles)) return false;
  }
  return true;
}

// V-H-V: (sx,sy) → (sx,cy) → (tx,cy) → (tx,ty)
// Searches a `cy` such that all three segments clear every obstacle.
// Returns null when no such midline exists — the caller should escalate
// to a 5-segment detour.  We constrain cy to the *exit* side of both
// handles: a ctrl-out source (bottom) must hand the edge down to cy,
// and a ctrl-in target (top) must receive it from above — without that
// guard a far-enough natural midpoint can produce a path that backs
// into the target from below.
function tryThreeSegmentV(sx, sy, tx, ty, sourcePos, targetPos, obstacles) {
  const sDir = sourcePos === "bottom" ? 1 : -1;
  const tDir = targetPos === "bottom" ? 1 : -1;
  const natural = (sy + ty) / 2;
  const candidates = perpendicularCandidates(natural, obstacles, "y");
  for (const cy of candidates) {
    if ((cy - sy) * sDir <= 0) continue;
    if ((cy - ty) * tDir <= 0) continue;
    const pts = [
      { x: sx, y: sy },
      { x: sx, y: cy },
      { x: tx, y: cy },
      { x: tx, y: ty },
    ];
    if (polylineClear(pts, obstacles)) return pts;
  }
  return null;
}

// H-V-H: (sx,sy) → (cx,sy) → (cx,ty) → (tx,ty)
function tryThreeSegmentH(sx, sy, tx, ty, sourcePos, targetPos, obstacles) {
  const sDir = sourcePos === "right" ? 1 : -1;
  const tDir = targetPos === "right" ? 1 : -1;
  const natural = (sx + tx) / 2;
  const candidates = perpendicularCandidates(natural, obstacles, "x");
  for (const cx of candidates) {
    if ((cx - sx) * sDir <= 0) continue;
    if ((cx - tx) * tDir <= 0) continue;
    const pts = [
      { x: sx, y: sy },
      { x: cx, y: sy },
      { x: cx, y: ty },
      { x: tx, y: ty },
    ];
    if (polylineClear(pts, obstacles)) return pts;
  }
  return null;
}

// V-H-V-H-V: (sx,sy) → (sx,y1) → (xm,y1) → (xm,y2) → (tx,y2) → (tx,ty)
//
// The stubs y1 / y2 fan out perpendicular to the handles, then a column
// xm at a clear x bridges the two.  We iterate the natural stub first,
// then alternate stub distances if a nearby obstacle blocks the
// horizontal hop at the default offset.
function tryFiveSegmentV(sx, sy, tx, ty, sourcePos, targetPos, obstacles) {
  const sDir = sourcePos === "bottom" ? 1 : -1;
  const tDir = targetPos === "bottom" ? 1 : -1;

  const y1Candidates = directionalCandidates(sy, sDir, obstacles, "y");
  const y2Candidates = directionalCandidates(ty, tDir, obstacles, "y");
  const xmCandidates = perpendicularCandidates((sx + tx) / 2, obstacles, "x");

  // Try a few of each — the natural stub is first, then nearby
  // obstacle-edge values.  The triple loop is bounded to keep the
  // per-edge cost predictable.
  const Y_CAP = Math.min(5, y1Candidates.length);
  for (let i = 0; i < Y_CAP; i++) {
    for (let j = 0; j < Y_CAP; j++) {
      const y1 = y1Candidates[i];
      const y2 = y2Candidates[j];
      for (const xm of xmCandidates) {
        const pts = [
          { x: sx, y: sy },
          { x: sx, y: y1 },
          { x: xm, y: y1 },
          { x: xm, y: y2 },
          { x: tx, y: y2 },
          { x: tx, y: ty },
        ];
        if (polylineClear(pts, obstacles)) return pts;
      }
    }
  }
  return null;
}

// H-V-H-V-H: mirror of tryFiveSegmentV for left/right handles.
function tryFiveSegmentH(sx, sy, tx, ty, sourcePos, targetPos, obstacles) {
  const sDir = sourcePos === "right" ? 1 : -1;
  const tDir = targetPos === "right" ? 1 : -1;

  const x1Candidates = directionalCandidates(sx, sDir, obstacles, "x");
  const x2Candidates = directionalCandidates(tx, tDir, obstacles, "x");
  const ymCandidates = perpendicularCandidates((sy + ty) / 2, obstacles, "y");

  const X_CAP = Math.min(5, x1Candidates.length);
  for (let i = 0; i < X_CAP; i++) {
    for (let j = 0; j < X_CAP; j++) {
      const x1 = x1Candidates[i];
      const x2 = x2Candidates[j];
      for (const ym of ymCandidates) {
        const pts = [
          { x: sx, y: sy },
          { x: x1, y: sy },
          { x: x1, y: ym },
          { x: x2, y: ym },
          { x: x2, y: ty },
          { x: tx, y: ty },
        ];
        if (polylineClear(pts, obstacles)) return pts;
      }
    }
  }
  return null;
}

// Candidate values for the centre line of a 3-segment route — natural
// midpoint plus a slot above and below each obstacle.  Sorted by
// proximity to the natural value so the smallest deviation wins.
function perpendicularCandidates(natural, obstacles, axis) {
  const out = [natural];
  for (const o of obstacles) {
    const lo = axis === "x" ? o.x : o.y;
    const hi = lo + (axis === "x" ? o.w : o.h);
    out.push(lo - OBSTACLE_PADDING);
    out.push(hi + OBSTACLE_PADDING);
  }
  out.sort((a, b) => Math.abs(a - natural) - Math.abs(b - natural));
  return out;
}

// Stub-distance candidates that respect the handle direction.  `dir`
// is +1 when the handle exits in the positive axis direction (bottom /
// right) and -1 otherwise; only values past `base` in that direction
// survive the filter, so we never propose a stub that retraces back
// through the source / target node.
function directionalCandidates(base, dir, obstacles, axis) {
  const primary = base + dir * STUB;
  const out = [primary];
  for (const o of obstacles) {
    const lo = axis === "x" ? o.x : o.y;
    const hi = lo + (axis === "x" ? o.w : o.h);
    out.push(lo - OBSTACLE_PADDING);
    out.push(hi + OBSTACLE_PADDING);
  }
  return out
    .filter((v) => (v - base) * dir > 0)
    .sort((a, b) => Math.abs(a - primary) - Math.abs(b - primary));
}
