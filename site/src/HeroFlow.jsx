import React from "react";

/*
 * HeroFlow — abstract network of flowing dots.
 *
 * Three input streams enter from the left, converge into a single horizontal
 * pipeline threaded through four checkpoint nodes (DISCOVER · RESOLVE · AUDIT
 * · SCORE), then split into two output basins (APPROVED teal, FLAGGED amber).
 * Dots flow continuously along pre-defined journey paths. Background is a
 * soft dot-grid. Checkpoints radiate pulse rings on a steady cadence.
 *
 * All motion is SMIL — no JS ticking. Each dot is a <circle> with an
 * <animateMotion> referencing one of six journey paths. Dots are pre-created
 * with staggered `begin` offsets so the stream looks endless.
 */

const VIEW_W = 880;
const VIEW_H = 500;

// ─── Palette (matches styles.css tokens) ─────────────────────────────────────
const INK = "#e2e8f0";
const MUTED = "#94a3b8";
const DIM = "#64748b";
const TEAL = "#2dd4bf";
const TEAL_DIM = "rgba(45,212,191,0.45)";
const TEAL_GLOW = "rgba(45,212,191,0.16)";
const AMBER = "#f59e0b";
const AMBER_DEEP = "#fbbf24";
const AMBER_GLOW = "rgba(245,158,11,0.18)";
const RED = "#f87171";
const LATTICE = "rgba(148,163,184,0.14)";

// ─── Geometry ────────────────────────────────────────────────────────────────
// Three input lanes on the left, converging into a single backbone through 4
// checkpoints, then splitting to two output pools on the right.
const INPUT_X = 20;
const INPUTS = [
  { id: "in-a", y: 110 },
  { id: "in-b", y: 250 },
  { id: "in-c", y: 390 },
];
const MERGE_X = 180;            // where inputs meet the backbone
const CP_X = [260, 400, 540, 680]; // DISCOVER · RESOLVE · AUDIT · SCORE
const CP_LABELS = ["DISCOVER", "RESOLVE", "AUDIT", "SCORE"];
const BACKBONE_Y = 250;
const SPLIT_X = 720;            // where the backbone splits to outputs
const OUT_APPROVED = { x: 840, y: 150 };
const OUT_FLAGGED = { x: 840, y: 350 };

// Build a journey path from input → backbone → split → output using smooth
// cubic bezier curves for the curved bends.
function journeyPath(inputY, outputTarget) {
  const out = outputTarget === "approved" ? OUT_APPROVED : OUT_FLAGGED;
  // Enter: straight from input x=20,y=inputY to (MERGE_X-40, inputY)
  // Curve: bezier to (MERGE_X, BACKBONE_Y)
  // Straight: along backbone to (SPLIT_X, BACKBONE_Y)
  // Curve: bezier down/up to (SPLIT_X+40, out.y)
  // Straight: to output
  return (
    `M ${INPUT_X} ${inputY} ` +
    `L ${MERGE_X - 60} ${inputY} ` +
    `C ${MERGE_X - 20} ${inputY}, ${MERGE_X} ${BACKBONE_Y}, ${MERGE_X + 30} ${BACKBONE_Y} ` +
    `L ${SPLIT_X - 10} ${BACKBONE_Y} ` +
    `C ${SPLIT_X + 20} ${BACKBONE_Y}, ${SPLIT_X + 40} ${out.y}, ${SPLIT_X + 70} ${out.y} ` +
    `L ${out.x - 10} ${out.y}`
  );
}

const JOURNEYS = [];
for (const inp of INPUTS) {
  for (const target of ["approved", "flagged"]) {
    JOURNEYS.push({ id: `${inp.id}-${target}`, inputY: inp.y, target, d: journeyPath(inp.y, target) });
  }
}

// Skeleton rail path (drawn once, thin dashed, for the viewer to read the
// structure even when no dot is passing).
function skeletonRail(inputY, outputTarget) {
  return journeyPath(inputY, outputTarget);
}

// ─── Dot pool ────────────────────────────────────────────────────────────────
// Pre-declare N dots per journey, staggered so the stream is continuous.
// Most dots are teal (safe). A deterministic fraction head to FLAGGED (amber).
// This gives the viewer a persistent "~85% safe, ~15% flagged" read.
const DOT_DUR = 7.2; // seconds per journey
const DOTS_PER_JOURNEY = 4;

const DOTS = [];
let dotSeed = 0;
for (const j of JOURNEYS) {
  for (let i = 0; i < DOTS_PER_JOURNEY; i++) {
    const phase = (i / DOTS_PER_JOURNEY) * DOT_DUR;
    const toFlagged = j.target === "flagged";
    // Slightly vary dot colors: small % of approved-path dots pulse amber at audit
    DOTS.push({
      id: `${j.id}-${i}`,
      pathId: `jp-${j.id}`,
      begin: `-${phase.toFixed(2)}s`,
      color: toFlagged ? AMBER : TEAL,
      glow: toFlagged ? AMBER_GLOW : TEAL_GLOW,
      seed: dotSeed++,
    });
  }
}
// Add a sprinkle of "fast" dots (shorter dur) on approved journeys to add life
const FAST_DOTS = [];
for (const j of JOURNEYS.filter((x) => x.target === "approved")) {
  FAST_DOTS.push({
    id: `${j.id}-fast`,
    pathId: `jp-${j.id}`,
    begin: `-${(Math.random() * 4).toFixed(2)}s`,
    color: TEAL,
    dur: DOT_DUR * 0.7,
  });
}

// ─── Primitives ──────────────────────────────────────────────────────────────

function Defs() {
  return (
    <defs>
      {/* Faint dot-grid lattice behind everything */}
      <pattern id="hf-grid" x="0" y="0" width="22" height="22" patternUnits="userSpaceOnUse">
        <circle cx="1" cy="1" r="0.9" fill={LATTICE} />
      </pattern>
      {/* Glow filter for dots */}
      <filter id="hf-glow" x="-80%" y="-80%" width="260%" height="260%">
        <feGaussianBlur stdDeviation="2.4" result="b" />
        <feMerge>
          <feMergeNode in="b" />
          <feMergeNode in="SourceGraphic" />
        </feMerge>
      </filter>
      <filter id="hf-glow-soft" x="-80%" y="-80%" width="260%" height="260%">
        <feGaussianBlur stdDeviation="1.2" result="b" />
        <feMerge>
          <feMergeNode in="b" />
          <feMergeNode in="SourceGraphic" />
        </feMerge>
      </filter>
      {/* Journey paths */}
      {JOURNEYS.map((j) => (
        <path key={j.id} id={`jp-${j.id}`} d={j.d} fill="none" stroke="none" />
      ))}
    </defs>
  );
}

function Background() {
  return (
    <g>
      <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="#0b0f16" />
      <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="url(#hf-grid)" />
      {/* Soft horizontal gradient wash for depth */}
      <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="url(#hf-vignette)" />
    </g>
  );
}

// Concentric rings radiate from the coordinate at a regular cadence.
function PulseRings({ cx, cy, color, period = 3, delay = 0 }) {
  return (
    <g>
      {[0, 1, 2].map((i) => (
        <circle
          key={i}
          cx={cx}
          cy={cy}
          r="6"
          fill="none"
          stroke={color}
          strokeWidth="1"
          opacity="0"
        >
          <animate
            attributeName="r"
            values="6; 34"
            dur={`${period}s`}
            begin={`${delay + i * (period / 3)}s`}
            repeatCount="indefinite"
          />
          <animate
            attributeName="opacity"
            values="0.55; 0"
            dur={`${period}s`}
            begin={`${delay + i * (period / 3)}s`}
            repeatCount="indefinite"
          />
        </circle>
      ))}
    </g>
  );
}

function Checkpoint({ cx, cy, label, index }) {
  // Hex-ish ring: outer dashed ring that rotates slowly, inner solid ring,
  // central dot that pulses opacity.
  return (
    <g transform={`translate(${cx}, ${cy})`}>
      <PulseRings cx={0} cy={0} color={TEAL_DIM} period={3} delay={index * 0.4} />
      {/* Outer rotating dashed ring */}
      <circle r="18" fill="none" stroke={TEAL} strokeWidth="1" strokeDasharray="3 5" opacity="0.55">
        <animateTransform attributeName="transform" type="rotate" from="0" to="360" dur="22s" repeatCount="indefinite" />
      </circle>
      {/* Inner solid ring */}
      <circle r="11" fill="#0b0f16" stroke={TEAL} strokeWidth="1.3" />
      {/* Center dot heartbeat */}
      <circle r="3" fill={TEAL} filter="url(#hf-glow-soft)">
        <animate attributeName="r" values="2.6; 4; 2.6" dur="1.8s" repeatCount="indefinite" />
        <animate attributeName="opacity" values="0.6; 1; 0.6" dur="1.8s" repeatCount="indefinite" />
      </circle>
      {/* Index in corner */}
      <text x="0" y="-28" textAnchor="middle" fontFamily="JetBrains Mono, monospace" fontSize="8.5" fontWeight="700" fill={MUTED} letterSpacing="1.5">
        0{index + 1}
      </text>
      {/* Label below */}
      <text x="0" y="38" textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="9.5" fontWeight="700" fill={INK} letterSpacing="2.2">
        {label}
      </text>
    </g>
  );
}

function InputPort({ x, y }) {
  return (
    <g transform={`translate(${x}, ${y})`}>
      <circle r="8" fill="none" stroke={DIM} strokeWidth="1" opacity="0.6" />
      <circle r="3" fill={TEAL} opacity="0.5">
        <animate attributeName="opacity" values="0.25; 0.75; 0.25" dur="2.4s" repeatCount="indefinite" />
      </circle>
      {/* Three small ticks on the outside */}
      <line x1="-14" y1="-6" x2="-10" y2="-6" stroke={DIM} strokeWidth="1" />
      <line x1="-14" y1="0" x2="-10" y2="0" stroke={DIM} strokeWidth="1" />
      <line x1="-14" y1="6" x2="-10" y2="6" stroke={DIM} strokeWidth="1" />
    </g>
  );
}

function OutputBasin({ x, y, label, count, color, colorGlow, seed }) {
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Glow pool */}
      <circle r="36" fill={colorGlow} />
      <circle r="24" fill="none" stroke={color} strokeWidth="1.3" />
      <circle r="14" fill={color} opacity="0.22" />
      {/* Pulse rings synced to dot arrivals (approximate) */}
      <PulseRings cx={0} cy={0} color={color} period={4.2} delay={seed} />
      {/* Label above */}
      <text x="0" y="-44" textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="10" fontWeight="700" fill={color} letterSpacing="2.5">
        {label}
      </text>
      {/* Counter */}
      <text x="0" y={4} textAnchor="middle" fontFamily="JetBrains Mono, monospace" fontSize="16" fontWeight="700" fill={INK}>
        {count}
      </text>
      <text x="0" y={18} textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="8" fill={MUTED} letterSpacing="1.5">
        TOTAL
      </text>
    </g>
  );
}

// Skeleton rail drawn once — gives the eye a structural cue.
function Rails() {
  return (
    <g>
      {/* We only draw unique segments of the path to avoid double-thick lines.
          Input lanes (3) */}
      {INPUTS.map((inp) => (
        <path
          key={`rail-in-${inp.id}`}
          d={`M ${INPUT_X} ${inp.y} L ${MERGE_X - 60} ${inp.y} C ${MERGE_X - 20} ${inp.y}, ${MERGE_X} ${BACKBONE_Y}, ${MERGE_X + 30} ${BACKBONE_Y}`}
          fill="none"
          stroke={LATTICE}
          strokeWidth="1"
          strokeDasharray="2 4"
        />
      ))}
      {/* Backbone */}
      <path
        d={`M ${MERGE_X + 30} ${BACKBONE_Y} L ${SPLIT_X - 10} ${BACKBONE_Y}`}
        fill="none"
        stroke={LATTICE}
        strokeWidth="1"
        strokeDasharray="2 4"
      />
      {/* Split — two output branches */}
      <path
        d={`M ${SPLIT_X - 10} ${BACKBONE_Y} C ${SPLIT_X + 20} ${BACKBONE_Y}, ${SPLIT_X + 40} ${OUT_APPROVED.y}, ${SPLIT_X + 70} ${OUT_APPROVED.y} L ${OUT_APPROVED.x - 10} ${OUT_APPROVED.y}`}
        fill="none"
        stroke={LATTICE}
        strokeWidth="1"
        strokeDasharray="2 4"
      />
      <path
        d={`M ${SPLIT_X - 10} ${BACKBONE_Y} C ${SPLIT_X + 20} ${BACKBONE_Y}, ${SPLIT_X + 40} ${OUT_FLAGGED.y}, ${SPLIT_X + 70} ${OUT_FLAGGED.y} L ${OUT_FLAGGED.x - 10} ${OUT_FLAGGED.y}`}
        fill="none"
        stroke={LATTICE}
        strokeWidth="1"
        strokeDasharray="2 4"
      />

      {/* Backbone "energy" overlay — thin scrolling dashed stroke */}
      <path
        d={`M ${MERGE_X + 30} ${BACKBONE_Y} L ${SPLIT_X - 10} ${BACKBONE_Y}`}
        fill="none"
        stroke={TEAL}
        strokeWidth="1"
        strokeDasharray="2 8"
        opacity="0.45"
      >
        <animate attributeName="stroke-dashoffset" from="0" to="-40" dur="2.4s" repeatCount="indefinite" />
      </path>
    </g>
  );
}

// ─── Dot ─────────────────────────────────────────────────────────────────────
function Dot({ d }) {
  const dur = d.dur ?? DOT_DUR;
  return (
    <g>
      {/* Soft glow underlay */}
      <circle r="6" fill={d.glow ?? TEAL_GLOW}>
        <animateMotion dur={`${dur}s`} repeatCount="indefinite" begin={d.begin}>
          <mpath href={`#${d.pathId}`} />
        </animateMotion>
      </circle>
      {/* Bright core */}
      <circle r="2.4" fill={d.color} filter="url(#hf-glow-soft)">
        <animateMotion dur={`${dur}s`} repeatCount="indefinite" begin={d.begin}>
          <mpath href={`#${d.pathId}`} />
        </animateMotion>
      </circle>
    </g>
  );
}

// ─── Title strip (minimal — sits at top without clashing with preview) ───────
function TopStrip() {
  return (
    <g>
      <text x="20" y="26" fontFamily="Space Grotesk, sans-serif" fontSize="10.5" fontWeight="700" fill={MUTED} letterSpacing="2.5">
        FLOW · LIVE
      </text>
      {/* Right-side meta: throughput & coverage counters */}
      <g transform={`translate(${VIEW_W - 230}, 14)`}>
        <MetaChip label="THROUGHPUT" value="128/s" />
        <g transform="translate(120, 0)">
          <MetaChip label="COVERAGE" value="87%" tone={TEAL} />
        </g>
      </g>
    </g>
  );
}

function MetaChip({ label, value, tone = INK }) {
  return (
    <g>
      <text x="0" y="8" fontFamily="Space Grotesk, sans-serif" fontSize="8" fontWeight="600" fill={DIM} letterSpacing="1.5">
        {label}
      </text>
      <text x="0" y="22" fontFamily="JetBrains Mono, monospace" fontSize="12" fontWeight="700" fill={tone}>
        {value}
      </text>
    </g>
  );
}

// ─── Root ────────────────────────────────────────────────────────────────────
export default function HeroFlow() {
  return (
    <svg
      className="hero-flow"
      viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
      preserveAspectRatio="xMidYMid meet"
      role="img"
      aria-label="Contract audit flow"
    >
      <Defs />
      <Background />
      <TopStrip />
      <Rails />

      {/* Input ports */}
      {INPUTS.map((inp) => (
        <InputPort key={inp.id} x={INPUT_X} y={inp.y} />
      ))}

      {/* Checkpoints on the backbone */}
      {CP_X.map((cx, i) => (
        <Checkpoint key={i} cx={cx} cy={BACKBONE_Y} label={CP_LABELS[i]} index={i} />
      ))}

      {/* Dots flowing (declared after rails so they render above) */}
      {DOTS.map((d) => <Dot key={d.id} d={d} />)}
      {FAST_DOTS.map((d) => <Dot key={d.id} d={d} />)}

      {/* Output basins */}
      <OutputBasin x={OUT_APPROVED.x} y={OUT_APPROVED.y} label="APPROVED" count="1,284" color={TEAL} colorGlow={TEAL_GLOW} seed={0.4} />
      <OutputBasin x={OUT_FLAGGED.x} y={OUT_FLAGGED.y} label="FLAGGED" count="42" color={AMBER_DEEP} colorGlow={AMBER_GLOW} seed={1.1} />

      {/* Frame */}
      <rect x="0.5" y="0.5" width={VIEW_W - 1} height={VIEW_H - 1} rx="10" fill="none" stroke="rgba(148,163,184,0.14)" strokeWidth="1" />
    </svg>
  );
}
