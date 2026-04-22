import React from "react";

/*
 * HeroMesh — abstract flowing constellation.
 *
 * No labels, no narrative, no direction. A lattice of ~20 nodes connected by
 * ~30 bezier curves. Dots flow along every edge. Nodes pulse on staggered
 * phases. Background carries faint concentric arcs for topographic depth.
 * Pure pattern + motion.
 */

const VIEW_W = 880;
const VIEW_H = 500;

// ─── Palette ─────────────────────────────────────────────────────────────────
const BG = "#0b0f16";
const LATTICE = "rgba(148,163,184,0.10)";
const FAINT = "rgba(148,163,184,0.06)";
const TEAL = "#2dd4bf";
const TEAL_DIM = "rgba(45,212,191,0.55)";
const TEAL_GHOST = "rgba(45,212,191,0.18)";
const AMBER = "#fbbf24";
const AMBER_DIM = "rgba(251,191,36,0.55)";
const AMBER_GHOST = "rgba(251,191,36,0.22)";

// ─── Node layout (hand-placed for rhythm, not grid) ──────────────────────────
// Each node has an id, position, size (small/med/large), tone, and a pulse
// phase offset in seconds so halos breathe out of sync.
const NODES = [
  { id: "a", x: 68,  y: 96,  size: "s", tone: "t", phase: 0.0 },
  { id: "b", x: 206, y: 66,  size: "m", tone: "t", phase: 1.2 },
  { id: "c", x: 332, y: 128, size: "s", tone: "t", phase: 2.4 },
  { id: "d", x: 460, y: 78,  size: "m", tone: "t", phase: 0.6 },
  { id: "e", x: 588, y: 112, size: "s", tone: "t", phase: 3.1 },
  { id: "f", x: 718, y: 84,  size: "m", tone: "t", phase: 1.8 },
  { id: "g", x: 820, y: 152, size: "s", tone: "t", phase: 2.9 },

  { id: "h", x: 128, y: 210, size: "s", tone: "t", phase: 0.9 },
  { id: "i", x: 268, y: 232, size: "l", tone: "t", phase: 2.0 },
  { id: "j", x: 408, y: 200, size: "s", tone: "t", phase: 1.3 },
  { id: "k", x: 540, y: 252, size: "m", tone: "a", phase: 0.4 },
  { id: "l", x: 664, y: 214, size: "s", tone: "t", phase: 2.6 },
  { id: "m", x: 788, y: 260, size: "s", tone: "t", phase: 3.4 },

  { id: "n", x: 82,  y: 340, size: "s", tone: "t", phase: 2.2 },
  { id: "o", x: 216, y: 374, size: "m", tone: "t", phase: 0.7 },
  { id: "p", x: 358, y: 344, size: "s", tone: "t", phase: 1.5 },
  { id: "q", x: 492, y: 386, size: "s", tone: "t", phase: 3.7 },
  { id: "r", x: 624, y: 358, size: "l", tone: "t", phase: 0.3 },
  { id: "s", x: 756, y: 398, size: "s", tone: "a", phase: 2.5 },

  { id: "t", x: 326, y: 454, size: "s", tone: "t", phase: 1.1 },
  { id: "u", x: 584, y: 468, size: "s", tone: "t", phase: 3.0 },
];

// ─── Edges (picked for visual weave, not completeness) ───────────────────────
const EDGES = [
  // top chain + diagonals
  ["a", "b"], ["b", "c"], ["c", "d"], ["d", "e"], ["e", "f"], ["f", "g"],
  ["a", "h"], ["b", "h"], ["b", "i"], ["c", "i"], ["d", "j"], ["c", "j"],
  ["d", "k"], ["e", "k"], ["e", "l"], ["f", "l"], ["f", "m"], ["g", "m"],
  // middle chain
  ["h", "i"], ["i", "j"], ["j", "k"], ["k", "l"], ["l", "m"],
  // mid to bottom
  ["h", "n"], ["i", "o"], ["i", "p"], ["j", "p"], ["k", "q"], ["k", "p"],
  ["l", "r"], ["m", "r"], ["m", "s"],
  // bottom chain
  ["n", "o"], ["o", "p"], ["p", "q"], ["q", "r"], ["r", "s"],
  // stragglers to the very bottom
  ["o", "t"], ["p", "t"], ["q", "u"], ["r", "u"],
];

const NODE_BY_ID = Object.fromEntries(NODES.map((n) => [n.id, n]));

// Size radii
const R = { s: 3.2, m: 4.8, l: 6.2 };
const R_HALO = { s: 9, m: 13, l: 17 };

// ─── Helpers ─────────────────────────────────────────────────────────────────
// Build a subtly curved bezier path between two nodes. Curve amount depends
// on distance + a deterministic "wobble" based on node ids so edges don't all
// bulge the same way.
function edgePath(a, b) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len = Math.hypot(dx, dy);
  const nx = -dy / len;
  const ny = dx / len;
  // Deterministic wobble: sign + magnitude from id chars.
  const sign = ((a.id.charCodeAt(0) + b.id.charCodeAt(0)) % 2 === 0) ? 1 : -1;
  const bow = Math.min(40, len * 0.12) * sign;
  const mx = (a.x + b.x) / 2 + nx * bow;
  const my = (a.y + b.y) / 2 + ny * bow;
  return `M ${a.x} ${a.y} Q ${mx} ${my} ${b.x} ${b.y}`;
}

// Dot colors — a small fraction are amber for texture.
function edgeDotColor(aId, bId, slot) {
  const h = (aId.charCodeAt(0) * 13 + bId.charCodeAt(0) * 7 + slot * 31) % 100;
  if (h < 12) return "amber";
  return "teal";
}

// ─── Defs ────────────────────────────────────────────────────────────────────
function Defs() {
  return (
    <defs>
      {/* Faint dot-grid lattice */}
      <pattern id="hm-grid" x="0" y="0" width="24" height="24" patternUnits="userSpaceOnUse">
        <circle cx="1" cy="1" r="0.7" fill="rgba(148,163,184,0.08)" />
      </pattern>
      {/* Dot glow */}
      <filter id="hm-glow" x="-100%" y="-100%" width="300%" height="300%">
        <feGaussianBlur stdDeviation="1.6" result="b" />
        <feMerge>
          <feMergeNode in="b" />
          <feMergeNode in="SourceGraphic" />
        </feMerge>
      </filter>
      {/* Halo glow */}
      <filter id="hm-halo" x="-100%" y="-100%" width="300%" height="300%">
        <feGaussianBlur stdDeviation="3" result="b" />
        <feMerge>
          <feMergeNode in="b" />
          <feMergeNode in="SourceGraphic" />
        </feMerge>
      </filter>
      {/* Edge paths registered for mpath */}
      {EDGES.map(([u, v]) => (
        <path
          key={`ep-${u}-${v}`}
          id={`hm-e-${u}-${v}`}
          d={edgePath(NODE_BY_ID[u], NODE_BY_ID[v])}
          fill="none"
          stroke="none"
        />
      ))}
    </defs>
  );
}

// ─── Background ──────────────────────────────────────────────────────────────
function Background() {
  return (
    <g>
      <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill={BG} />
      <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="url(#hm-grid)" />
      {/* Topographic arcs — very faint, emanating from off-canvas loci so only
          arcs are visible within the frame. Two overlapping origins create a
          soft interference feel. */}
      <g opacity="0.5">
        {[240, 320, 400, 480, 560, 640, 720].map((r, i) => (
          <circle key={`t1-${i}`} cx="140" cy="580" r={r} fill="none" stroke={FAINT} strokeWidth="1" />
        ))}
        {[220, 300, 380, 460, 540, 620, 700].map((r, i) => (
          <circle key={`t2-${i}`} cx="780" cy="-80" r={r} fill="none" stroke={FAINT} strokeWidth="1" />
        ))}
      </g>
      {/* One slowly rotating outer ring — a distant "horizon" */}
      <g transform={`translate(${VIEW_W / 2}, ${VIEW_H / 2})`}>
        <circle r="360" fill="none" stroke="rgba(45,212,191,0.05)" strokeWidth="1" strokeDasharray="2 10">
          <animateTransform attributeName="transform" type="rotate" from="0" to="360" dur="120s" repeatCount="indefinite" />
        </circle>
      </g>
    </g>
  );
}

// ─── Edges (rendered as static subtle lines under the flowing dots) ──────────
function EdgeLine({ u, v }) {
  const a = NODE_BY_ID[u];
  const b = NODE_BY_ID[v];
  return (
    <path
      d={edgePath(a, b)}
      fill="none"
      stroke={LATTICE}
      strokeWidth="1"
      opacity="0.85"
    >
      {/* Gentle opacity breath so the whole mesh softly shimmers */}
      <animate
        attributeName="opacity"
        values="0.55; 0.95; 0.55"
        dur={`${7 + ((u.charCodeAt(0) + v.charCodeAt(0)) % 5)}s`}
        repeatCount="indefinite"
        begin={`-${(u.charCodeAt(0) * 0.3 + v.charCodeAt(0) * 0.17) % 6}s`}
      />
    </path>
  );
}

// ─── Node ────────────────────────────────────────────────────────────────────
function Node({ n }) {
  const r = R[n.size];
  const rh = R_HALO[n.size];
  const color = n.tone === "a" ? AMBER : TEAL;
  const ghost = n.tone === "a" ? AMBER_GHOST : TEAL_GHOST;
  return (
    <g transform={`translate(${n.x}, ${n.y})`}>
      {/* Outer halo — pulses */}
      <circle r={rh} fill={ghost} opacity="0.0">
        <animate
          attributeName="opacity"
          values="0; 0.6; 0"
          dur="3.6s"
          begin={`${-n.phase}s`}
          repeatCount="indefinite"
        />
        <animate
          attributeName="r"
          values={`${r}; ${rh * 1.6}; ${rh * 1.6}`}
          keyTimes="0; 0.5; 1"
          dur="3.6s"
          begin={`${-n.phase}s`}
          repeatCount="indefinite"
        />
      </circle>
      {/* Inner ring */}
      <circle r={r + 2.5} fill="none" stroke={color} strokeWidth="0.9" opacity="0.75" />
      {/* Core */}
      <circle r={r} fill={color} filter="url(#hm-halo)">
        <animate
          attributeName="opacity"
          values="0.75; 1; 0.75"
          dur={`${2.6 + (n.id.charCodeAt(0) % 3) * 0.4}s`}
          repeatCount="indefinite"
          begin={`${-n.phase * 0.5}s`}
        />
      </circle>
    </g>
  );
}

// ─── Flowing dot ─────────────────────────────────────────────────────────────
function FlowDot({ edgeId, colorKey, dur, begin, size = 1.8, reverse = false }) {
  const color = colorKey === "amber" ? AMBER : TEAL;
  const dim = colorKey === "amber" ? AMBER_DIM : TEAL_DIM;
  return (
    <g>
      {/* Glow halo */}
      <circle r={size * 2.2} fill={dim} opacity="0.35">
        <animateMotion
          dur={`${dur}s`}
          repeatCount="indefinite"
          begin={`${begin}s`}
          keyPoints={reverse ? "1;0" : "0;1"}
          keyTimes="0;1"
        >
          <mpath href={`#${edgeId}`} />
        </animateMotion>
      </circle>
      {/* Bright core */}
      <circle r={size} fill={color} filter="url(#hm-glow)">
        <animateMotion
          dur={`${dur}s`}
          repeatCount="indefinite"
          begin={`${begin}s`}
          keyPoints={reverse ? "1;0" : "0;1"}
          keyTimes="0;1"
        >
          <mpath href={`#${edgeId}`} />
        </animateMotion>
      </circle>
    </g>
  );
}

// ─── Root ────────────────────────────────────────────────────────────────────
export default function HeroMesh() {
  // Pre-compute the dot population. For each edge, decide how many dots
  // (0/1/2), their direction, duration, and stagger. Deterministic from ids
  // so render is stable across reloads.
  const dots = [];
  EDGES.forEach(([u, v], i) => {
    const edgeId = `hm-e-${u}-${v}`;
    const seed = (u.charCodeAt(0) * 7 + v.charCodeAt(0) * 11 + i * 3) % 100;
    const nDots = seed < 25 ? 0 : seed < 75 ? 1 : 2;
    const reverse = seed % 2 === 0;
    const baseDur = 5 + (seed % 4);
    for (let k = 0; k < nDots; k++) {
      const colorKey = edgeDotColor(u, v, k);
      const begin = -((seed + k * 53) % (baseDur * 10)) / 10;
      dots.push({
        key: `d-${u}-${v}-${k}`,
        edgeId,
        colorKey,
        dur: baseDur + k * 0.7,
        begin,
        size: 1.8 + (seed % 3) * 0.2,
        reverse,
      });
    }
  });

  return (
    <svg
      className="hero-mesh"
      viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
      preserveAspectRatio="xMidYMid meet"
      role="img"
      aria-label="Abstract flowing network"
    >
      <Defs />
      <Background />
      {/* Static edges under everything */}
      <g>
        {EDGES.map(([u, v]) => <EdgeLine key={`line-${u}-${v}`} u={u} v={v} />)}
      </g>
      {/* Flowing dots */}
      <g>
        {dots.map((d) => (
          <FlowDot key={d.key} edgeId={d.edgeId} colorKey={d.colorKey} dur={d.dur} begin={d.begin} size={d.size} reverse={d.reverse} />
        ))}
      </g>
      {/* Nodes on top */}
      <g>
        {NODES.map((n) => <Node key={n.id} n={n} />)}
      </g>
      {/* Subtle frame */}
      <rect x="0.5" y="0.5" width={VIEW_W - 1} height={VIEW_H - 1} rx="10" fill="none" stroke="rgba(148,163,184,0.14)" strokeWidth="1" />
    </svg>
  );
}
