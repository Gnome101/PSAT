import React from "react";

/*
 * HeroSurface — scripted replay of a full ProtocolSurface audit.
 *
 * Designed to read as the real product in motion, not a static diagram.
 * Composition echoes the actual /company/<name>/surface page:
 *
 *   ┌────────────────────────────────────────────────┬──────────────┐
 *   │  etherfi / SURFACE    [ Discover → ... → Done ]│  Detail      │
 *   │                                                │  panel       │
 *   │   [contract grid with dotted-teal edges and    │  (slides in) │
 *   │    live dots traveling along connections]      │              │
 *   │                                                │              │
 *   │   Value 5 · Flagged 1 · Utility 2              │              │
 *   └────────────────────────────────────────────────┴──────────────┘
 *
 * Timeline (18s loop):
 *   0.00 → 0.03   chrome + progress strip mounts ("Discovering…")
 *   0.03 → 0.28   8 cards stagger in; value counters tick up during card-in
 *   0.28 → 0.32   progress → "Resolving proxies…"
 *   0.32 → 0.52   7 edges fade in with traveling dots
 *   0.52 → 0.56   progress → "Auditing…"
 *   0.56 → 0.70   risk chips pop; one card (FeeHandler) gets amber tint
 *   0.70 → 0.74   progress → "Coverage 87%"
 *   0.74 → 0.82   amber focus ring on FeeHandler; detail panel slides in
 *   0.82 → 0.96   hold — moving edge dots keep looping, panel content pulses
 *   0.96 → 1.00   fade out, restart
 */

const CYCLE = 18;
const VIEW_W = 880;
const VIEW_H = 500;
const MAIN_W = 576;
const MAIN_PAD = 14;
const PANEL_X = MAIN_W + 4;
const PANEL_W = VIEW_W - PANEL_X - 4;
const HDR_H = 40;

// ─── Palette (native to styles.css tokens) ───────────────────────────────────
const BG = "#0b0f16";
const CARD = "#111620";
const CARD_EDGE = "rgba(148,163,184,0.14)";
const INK = "#e2e8f0";
const MUTED = "#94a3b8";
const DIM = "#64748b";
const FAINT = "rgba(148,163,184,0.08)";
const TEAL = "#2dd4bf";
const TEAL_SOFT = "rgba(45,212,191,0.12)";
const AMBER = "#f59e0b";
const AMBER_DEEP = "#fbbf24";
const AMBER_SOFT = "rgba(245,158,11,0.18)";
const RED = "#f87171";
const RISK = { ok: TEAL, warn: AMBER, bad: RED, none: DIM };

// ─── Scene ───────────────────────────────────────────────────────────────────
const CARD_W = 168;
const CARD_H = 82;
// Positions relative to main surface origin (MAIN_PAD, HDR_H + MAIN_PAD).
const NODES = [
  { id: "router",    col: 0, row: 0, name: "ProtocolRouter", tags: "proxy · upgradeable", spec: "EIP1967", value: 2_400_000_000, valueLabel: "$2.4B", risk: "ok",   appearAt: 0.03, badges: ["2P"] },
  { id: "staking",   col: 1, row: 0, name: "StakingVault",   tags: "value · stakeable",   spec: "ERC20",   value: 890_000_000,   valueLabel: "$890M", risk: "ok",   appearAt: 0.05, badges: ["POOL"] },
  { id: "rewards",   col: 2, row: 0, name: "RewardsMgr",     tags: "accrual · payable",   spec: "CLAIMS",  value: 120_000_000,   valueLabel: "$120M", risk: "ok",   appearAt: 0.07, badges: ["4/7"] },
  { id: "access",    col: 0, row: 1, name: "AccessControl",  tags: "roles · pause",       spec: "GNOSIS",  value: null,          valueLabel: null,    risk: "ok",   appearAt: 0.09, badges: ["4/7"] },
  { id: "fees",      col: 1, row: 1, name: "FeeHandler",     tags: "timelock · value-in", spec: "TL·3d",   value: 41_000_000,    valueLabel: "$41M",  risk: "warn", appearAt: 0.11, badges: ["TL"] },
  { id: "withdrawq", col: 2, row: 1, name: "WithdrawQueue",  tags: "erc721 · claim",      spec: "ERC721",  value: 78_000_000,    valueLabel: "$78M",  risk: "ok",   appearAt: 0.13, badges: ["QUEUE"] },
  { id: "oracle",    col: 0, row: 2, name: "OracleV2",       tags: "price · chainlink",   spec: "FEED",    value: null,          valueLabel: null,    risk: "ok",   appearAt: 0.15, badges: ["ADMIN"] },
  { id: "bridge",    col: 2, row: 2, name: "L2Adapter",      tags: "bridge · router",     spec: "X-CHAIN", value: 310_000_000,   valueLabel: "$310M", risk: "ok",   appearAt: 0.17, badges: ["GATEWAY"] },
];

// Grid math: 3 columns, 3 rows (with the mid-row-col-1 position empty apart from FeeHandler).
function nodePos(n) {
  const colGap = (MAIN_W - MAIN_PAD * 2 - CARD_W * 3) / 2;
  const rowGap = 24;
  const x = MAIN_PAD + n.col * (CARD_W + colGap);
  const y = HDR_H + MAIN_PAD + 6 + n.row * (CARD_H + rowGap);
  return { x, y };
}

const POS = Object.fromEntries(NODES.map((n) => [n.id, { ...nodePos(n), ...n }]));

const EDGES = [
  { from: "router",  to: "access",    label: "admin",        appearAt: 0.20 },
  { from: "router",  to: "staking",   label: "delegatecall", appearAt: 0.22 },
  { from: "staking", to: "fees",      label: "value-in",     appearAt: 0.24 },
  { from: "staking", to: "rewards",   label: "accrue",       appearAt: 0.26 },
  { from: "rewards", to: "withdrawq", label: "claim",        appearAt: 0.28 },
  { from: "fees",    to: "withdrawq", label: "settle",       appearAt: 0.30 },
  { from: "oracle",  to: "fees",      label: "price",        appearAt: 0.32 },
  { from: "bridge",  to: "withdrawq", label: "forward",      appearAt: 0.34 },
];

const FOCUS = "fees";

// ─── Keyframe helpers ────────────────────────────────────────────────────────
// appearKT(f): element is hidden until fraction f, then visible, fades at the
// tail so the next cycle starts clean.
function appearKT(f, ramp = 0.025) {
  const start = Math.max(0, f - 0.001);
  const peak = Math.min(start + ramp, 0.955);
  const tailStart = Math.max(peak + 0.001, 0.96);
  return {
    keyTimes: `0; ${start.toFixed(4)}; ${peak.toFixed(4)}; ${tailStart.toFixed(4)}; 1`,
    values: "0; 0; 1; 1; 0",
  };
}
function slideKT(f, ramp = 0.03) {
  const start = Math.max(0, f - 0.001);
  const peak = Math.min(start + ramp, 0.955);
  const tailStart = Math.max(peak + 0.001, 0.96);
  return {
    keyTimes: `0; ${start.toFixed(4)}; ${peak.toFixed(4)}; ${tailStart.toFixed(4)}; 1`,
  };
}

// ─── Edge anchor selection ───────────────────────────────────────────────────
function anchor(n, side) {
  const cx = n.x + CARD_W / 2;
  const cy = n.y + CARD_H / 2;
  if (side === "top") return [cx, n.y];
  if (side === "bottom") return [cx, n.y + CARD_H];
  if (side === "left") return [n.x, cy];
  return [n.x + CARD_W, cy];
}
function edgeAnchors(fromId, toId) {
  const a = POS[fromId];
  const b = POS[toId];
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  if (Math.abs(dy) > Math.abs(dx) * 0.5) {
    return a.y < b.y ? [anchor(a, "bottom"), anchor(b, "top")] : [anchor(a, "top"), anchor(b, "bottom")];
  }
  return a.x < b.x ? [anchor(a, "right"), anchor(b, "left")] : [anchor(a, "left"), anchor(b, "right")];
}

// ─── Chrome pieces ───────────────────────────────────────────────────────────

function Chrome() {
  const { keyTimes, values } = appearKT(0.015);
  return (
    <g opacity="0">
      <animate attributeName="opacity" values={values} keyTimes={keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
      {/* Outer frame */}
      <rect x="0" y="0" width={VIEW_W} height={VIEW_H} rx="10" fill={BG} stroke={CARD_EDGE} strokeWidth="1" />
      <rect x="0.5" y="0.5" width={VIEW_W - 1} height={VIEW_H - 1} rx="9.5" fill="none" stroke="rgba(45,212,191,0.04)" strokeWidth="1" />
      {/* Header */}
      <rect x="0" y="0" width={VIEW_W} height={HDR_H} fill="rgba(17,22,32,0.7)" />
      <line x1="0" y1={HDR_H} x2={VIEW_W} y2={HDR_H} stroke={CARD_EDGE} strokeWidth="1" />
      {/* Breadcrumb */}
      <text x="18" y="25" fontFamily="Space Grotesk, sans-serif" fontSize="12" fontWeight="700" fill={INK}>
        etherfi
      </text>
      <text x="70" y="25" fontFamily="Space Grotesk, sans-serif" fontSize="10" fill={MUTED} letterSpacing="2">
        / SURFACE
      </text>
      {/* Split line between main and panel */}
      <line x1={PANEL_X - 2} y1={HDR_H} x2={PANEL_X - 2} y2={VIEW_H} stroke={CARD_EDGE} strokeWidth="1" />
      {/* Bottom legend row on main */}
      <g transform={`translate(${MAIN_PAD}, ${VIEW_H - 26})`}>
        <LegendPill x={0}   dot={TEAL} label="Value"   count="5" />
        <LegendPill x={98}  dot={AMBER} label="Flagged" count="1" />
        <LegendPill x={200} dot={DIM}  label="Utility" count="2" />
      </g>
      {/* Faint dotted grid (subtle depth) */}
      <g opacity="0.35">
        {[...Array(6)].map((_, i) => (
          <line key={`h${i}`} x1={MAIN_PAD} y1={HDR_H + 14 + i * 72} x2={MAIN_W - MAIN_PAD} y2={HDR_H + 14 + i * 72} stroke={FAINT} strokeWidth="0.8" strokeDasharray="2 4" />
        ))}
      </g>
    </g>
  );
}

function LegendPill({ x, dot, label, count }) {
  return (
    <g transform={`translate(${x}, 0)`}>
      <circle cx="5" cy="8" r="3.5" fill={dot} />
      <text x="14" y="11" fontFamily="Space Grotesk, sans-serif" fontSize="9.5" fontWeight="600" fill={MUTED} letterSpacing="0.6">
        {label}
      </text>
      <text x={76} y="11" fontFamily="JetBrains Mono, monospace" fontSize="9.5" fill={INK}>
        {count}
      </text>
    </g>
  );
}

// Progress strip: 4 steps, each highlights at its phase, ends with a value.
const PROGRESS_STEPS = [
  { label: "Discover",  start: 0.02, end: 0.18 },
  { label: "Resolve",   start: 0.18, end: 0.34 },
  { label: "Audit",     start: 0.34, end: 0.48 },
  { label: "Score",     start: 0.48, end: 0.58 },
];

function ProgressStrip() {
  const totalW = 320;
  const x0 = MAIN_W - totalW - 18;
  const y0 = 12;
  const cellW = totalW / PROGRESS_STEPS.length;
  return (
    <g>
      {PROGRESS_STEPS.map((s, i) => {
        const kt = appearKT(s.start + 0.005, 0.01);
        // "complete" color kicks in at s.end
        const completeKT = appearKT(s.end, 0.02);
        return (
          <g key={s.label} transform={`translate(${x0 + i * cellW}, ${y0})`}>
            {/* base pill (always visible after chrome) */}
            <rect x="0" y="0" width={cellW - 6} height="18" rx="3" fill="rgba(17,22,32,0.65)" stroke={CARD_EDGE} strokeWidth="1" />
            {/* active fill — teal */}
            <rect x="1" y="1" width={cellW - 8} height="16" rx="2.5" fill={TEAL_SOFT} opacity="0">
              <animate attributeName="opacity" values={kt.values} keyTimes={kt.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
            </rect>
            {/* complete fill */}
            <rect x="1" y="1" width={cellW - 8} height="16" rx="2.5" fill="rgba(45,212,191,0.24)" opacity="0">
              <animate attributeName="opacity" values={completeKT.values} keyTimes={completeKT.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
            </rect>
            {/* label */}
            <text x={(cellW - 6) / 2} y="12" textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="9" fontWeight="700" fill={INK} letterSpacing="1.5">
              {s.label.toUpperCase()}
            </text>
          </g>
        );
      })}
    </g>
  );
}

// ─── Contract card ───────────────────────────────────────────────────────────
function ContractCard({ n }) {
  const pos = POS[n.id];
  const card = appearKT(n.appearAt);
  // Risk chip appears during audit phase, at ~0.38 + stagger.
  const chipT = 0.38 + (n.appearAt - 0.03) * 0.5;
  const chip = appearKT(chipT, 0.02);
  // Flagged card keeps amber header; appears during audit phase.
  const isWarn = n.risk === "warn";
  const isFocus = n.id === FOCUS;
  const headerFill = isWarn ? AMBER_SOFT : TEAL_SOFT;

  return (
    <g transform={`translate(${pos.x}, ${pos.y})`}>
      {/* Focus ring behind card */}
      {isFocus && (
        <g opacity="0">
          <animate attributeName="opacity" values="0; 0; 1; 1; 0" keyTimes="0; 0.50; 0.54; 0.96; 1" dur={`${CYCLE}s`} repeatCount="indefinite" />
          <rect x="-6" y="-6" width={CARD_W + 12} height={CARD_H + 12} rx="8" fill="none" stroke={AMBER_DEEP} strokeWidth="1.6" strokeDasharray="5 4" />
          {/* animated dash scroll */}
          <rect x="-6" y="-6" width={CARD_W + 12} height={CARD_H + 12} rx="8" fill="none" stroke={AMBER_DEEP} strokeWidth="1.6" strokeDasharray="5 4" opacity="0.35">
            <animate attributeName="stroke-dashoffset" from="0" to="-36" dur="2.4s" repeatCount="indefinite" />
          </rect>
        </g>
      )}
      <g opacity="0">
        <animate attributeName="opacity" values={card.values} keyTimes={card.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
        <animateTransform
          attributeName="transform"
          type="translate"
          values="0 8; 0 8; 0 0; 0 0; 0 0"
          keyTimes={card.keyTimes}
          dur={`${CYCLE}s`}
          repeatCount="indefinite"
          additive="sum"
        />
        {/* shadow */}
        <rect x="0" y="2" width={CARD_W} height={CARD_H} rx="5" fill="rgba(0,0,0,0.35)" />
        {/* body */}
        <rect x="0" y="0" width={CARD_W} height={CARD_H} rx="5" fill={CARD} stroke={CARD_EDGE} strokeWidth="1" />
        {/* header tint band */}
        <path d={`M 5 0 L ${CARD_W - 5} 0 Q ${CARD_W} 0 ${CARD_W} 5 L ${CARD_W} 24 L 0 24 L 0 5 Q 0 0 5 0 Z`} fill={headerFill} />
        {/* side accent tick */}
        <rect x="0" y="0" width="3" height={CARD_H} rx="1.5" fill={isWarn ? AMBER : TEAL} opacity="0.85" />
        {/* name */}
        <text x="12" y="16" fontFamily="Space Grotesk, sans-serif" fontSize="12" fontWeight="700" fill={INK}>
          {n.name}
        </text>
        {/* badge (top-right) */}
        <g transform={`translate(${CARD_W - 10}, 12)`}>
          <rect x={-38} y={-8} width={38} height="15" rx="2.5" fill="rgba(255,255,255,0.05)" stroke={CARD_EDGE} strokeWidth="0.8" />
          <text x={-19} y="3" textAnchor="middle" fontFamily="JetBrains Mono, monospace" fontSize="8.5" fontWeight="700" fill={isWarn ? AMBER_DEEP : TEAL}>
            {n.badges[0]}
          </text>
        </g>
        {/* tags */}
        <text x="12" y="36" fontFamily="Space Grotesk, sans-serif" fontSize="9.5" fill={MUTED} letterSpacing="0.3">
          {n.tags}
        </text>
        {/* spec chip + address row */}
        <g transform="translate(12, 48)">
          <rect x="0" y="-9" width={n.spec.length * 5.4 + 10} height="14" rx="2.5" fill="rgba(148,163,184,0.08)" />
          <text x={n.spec.length * 2.7 + 5} y="1" textAnchor="middle" fontFamily="JetBrains Mono, monospace" fontSize="8.5" fontWeight="700" fill={MUTED}>
            {n.spec}
          </text>
        </g>
        <text x="12" y="72" fontFamily="JetBrains Mono, monospace" fontSize="9" fill={DIM}>
          0x{n.id.slice(0, 4).padEnd(4, "0")}…{(n.id.slice(-4) + "0000").slice(0, 4)}
        </text>
        {/* Value — counter ticks up during card appear */}
        {n.value && <Counter value={n.value} label={n.valueLabel} risk={n.risk} appearAt={n.appearAt} />}
        {/* Risk chip */}
        <g opacity="0">
          <animate attributeName="opacity" values={chip.values} keyTimes={chip.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
          <circle cx={CARD_W - 12} cy={CARD_H - 12} r="4" fill={RISK[n.risk]} />
          <circle cx={CARD_W - 12} cy={CARD_H - 12} r="7" fill="none" stroke={RISK[n.risk]} strokeWidth="1" opacity="0.45">
            <animate attributeName="r" values="7; 10; 7" dur="2.4s" repeatCount="indefinite" />
            <animate attributeName="opacity" values="0.45; 0; 0.45" dur="2.4s" repeatCount="indefinite" />
          </circle>
        </g>
      </g>
    </g>
  );
}

// Value counter: fake tick-up of the dollar value during the card's in-phase.
// Renders the final label at full opacity; a secondary "lower" label fades out.
function Counter({ value, label, risk, appearAt }) {
  const color = risk === "warn" ? AMBER_DEEP : TEAL;
  const tickEnd = appearAt + 0.04;
  const kt = appearKT(tickEnd, 0.01);
  return (
    <g>
      {/* "...LOADING" dim placeholder */}
      <text x={CARD_W - 10} y="66" textAnchor="end" fontFamily="Space Grotesk, sans-serif" fontSize="12" fontWeight="700" fill={DIM} opacity="0.55">
        <animate attributeName="opacity" values="0; 0.55; 0.55; 0; 0; 0" keyTimes={`0; ${appearAt.toFixed(4)}; ${(appearAt + 0.025).toFixed(4)}; ${tickEnd.toFixed(4)}; 0.96; 1`} dur={`${CYCLE}s`} repeatCount="indefinite" />
        •••
      </text>
      {/* Final value */}
      <text x={CARD_W - 10} y="66" textAnchor="end" fontFamily="Space Grotesk, sans-serif" fontSize="12" fontWeight="700" fill={color} opacity="0">
        <animate attributeName="opacity" values={kt.values} keyTimes={kt.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
        {label}
      </text>
    </g>
  );
}

// ─── Edge ────────────────────────────────────────────────────────────────────
function Edge({ e }) {
  const [[x1, y1], [x2, y2]] = edgeAnchors(e.from, e.to);
  const midX = (x1 + x2) / 2;
  const midY = (y1 + y2) / 2;
  const kt = appearKT(e.appearAt);
  // Dot travel: begins at edge.appearAt, repeats for remainder of cycle.
  const dotDur = 2.2;
  return (
    <g opacity="0">
      <animate attributeName="opacity" values={kt.values} keyTimes={kt.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
      <line x1={x1} y1={y1} x2={x2} y2={y2} stroke={TEAL} strokeWidth="1.1" strokeDasharray="3 4" opacity="0.7" />
      <circle r="2" fill={TEAL}>
        <animate attributeName="cx" values={`${x1}; ${x2}`} dur={`${dotDur}s`} repeatCount="indefinite" begin={`${(e.appearAt * CYCLE).toFixed(2)}s`} />
        <animate attributeName="cy" values={`${y1}; ${y2}`} dur={`${dotDur}s`} repeatCount="indefinite" begin={`${(e.appearAt * CYCLE).toFixed(2)}s`} />
        <animate attributeName="opacity" values="0; 1; 1; 0" keyTimes="0; 0.1; 0.9; 1" dur={`${dotDur}s`} repeatCount="indefinite" begin={`${(e.appearAt * CYCLE).toFixed(2)}s`} />
      </circle>
      <g transform={`translate(${midX}, ${midY - 4})`}>
        <rect x={-e.label.length * 2.7 - 4} y={-8} width={e.label.length * 5.4 + 8} height="12" rx="2" fill={BG} opacity="0.8" />
        <text
          textAnchor="middle"
          y="1"
          fontFamily="Space Grotesk, sans-serif"
          fontSize="9"
          fontWeight="600"
          fill={MUTED}
          letterSpacing="0.4"
        >
          {e.label}
        </text>
      </g>
    </g>
  );
}

// ─── Detail panel (slides in at end of cycle) ────────────────────────────────
function DetailPanel() {
  const focus = POS[FOCUS];
  const open = slideKT(0.54);
  const openOpacity = appearKT(0.54);
  return (
    <g>
      {/* Slide-in panel */}
      <g opacity="0">
        <animate attributeName="opacity" values={openOpacity.values} keyTimes={openOpacity.keyTimes} dur={`${CYCLE}s`} repeatCount="indefinite" />
        <animateTransform
          attributeName="transform"
          type="translate"
          values={`${PANEL_W} 0; ${PANEL_W} 0; 0 0; 0 0; ${PANEL_W} 0`}
          keyTimes={open.keyTimes}
          dur={`${CYCLE}s`}
          repeatCount="indefinite"
        />
        <g transform={`translate(${PANEL_X}, ${HDR_H})`}>
          {/* Panel background */}
          <rect x="0" y="0" width={PANEL_W} height={VIEW_H - HDR_H} fill="rgba(13,18,26,0.92)" />
          {/* Tabs row */}
          <g transform="translate(12, 18)">
            <rect x="0" y="0" width="52" height="22" rx="3" fill={TEAL_SOFT} stroke="rgba(45,212,191,0.4)" strokeWidth="1" />
            <text x="26" y="15" textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="10" fontWeight="700" fill={TEAL} letterSpacing="0.5">
              Detail
            </text>
            <text x="72" y="15" fontFamily="Space Grotesk, sans-serif" fontSize="10" fontWeight="600" fill={MUTED} letterSpacing="0.5">
              Audits (3)
            </text>
          </g>
          {/* Contract name */}
          <text x="12" y="66" fontFamily="Space Grotesk, sans-serif" fontSize="15" fontWeight="700" fill={INK}>
            {focus.name}
          </text>
          <text x="12" y="84" fontFamily="JetBrains Mono, monospace" fontSize="10" fill={DIM}>
            0xfe00…41fe
          </text>
          {/* Badge row */}
          <g transform="translate(12, 100)">
            <Badge x={0} label="VALUE HANDLER" tone="teal" />
            <Badge x={102} label="TIMELOCK 3d" tone="amber" />
            <Badge x={0} y={24} label="5 UPGRADES" tone="muted" />
            <Badge x={80} y={24} label="21 FUNCTIONS" tone="muted" />
            <Badge x={172} y={24} label="$41M" tone="amber" bold />
          </g>
          {/* Functions list header */}
          <line x1="12" y1="170" x2={PANEL_W - 12} y2="170" stroke={CARD_EDGE} strokeWidth="1" />
          <text x="12" y="186" fontFamily="Space Grotesk, sans-serif" fontSize="9.5" fontWeight="700" fill={MUTED} letterSpacing="2">
            CONTROL
          </text>
          <text x={PANEL_W - 12} y="186" textAnchor="end" fontFamily="JetBrains Mono, monospace" fontSize="9.5" fill={DIM}>
            8
          </text>
          <FunctionRow y={204} name="upgradeTo" meta="delegatecall path" badge="TL 3d" tone="amber" />
          <FunctionRow y={232} name="upgradeToAndCall" meta="delegatecall path" badge="TL 3d" tone="amber" />
          <FunctionRow y={260} name="transferOwnership" meta="changes owner" badge="2P sigset" tone="muted" />
          <FunctionRow y={288} name="pauseContract" meta="pause control" badge="SAFE 4/7" tone="teal" />
          <FunctionRow y={316} name="setFeeRecipient" meta="changes route" badge="TL 3d" tone="amber" />
          {/* Operations section header */}
          <line x1="12" y1="346" x2={PANEL_W - 12} y2="346" stroke={CARD_EDGE} strokeWidth="1" />
          <text x="12" y="362" fontFamily="Space Grotesk, sans-serif" fontSize="9.5" fontWeight="700" fill={MUTED} letterSpacing="2">
            OPERATIONS
          </text>
          <text x={PANEL_W - 12} y="362" textAnchor="end" fontFamily="JetBrains Mono, monospace" fontSize="9.5" fill={DIM}>
            6
          </text>
          <FunctionRow y={380} name="collectFees" meta="value out" badge="OPEN" tone="teal" />
          <FunctionRow y={408} name="settleWithdrawal" meta="value out" badge="OPEN" tone="teal" />
        </g>
      </g>
    </g>
  );
}

function Badge({ x, y = 0, label, tone = "muted", bold = false }) {
  const fg = tone === "teal" ? TEAL : tone === "amber" ? AMBER_DEEP : MUTED;
  const bg = tone === "teal" ? TEAL_SOFT : tone === "amber" ? AMBER_SOFT : "rgba(148,163,184,0.08)";
  const len = label.length;
  return (
    <g transform={`translate(${x}, ${y})`}>
      <rect x="0" y="0" width={len * 5.7 + 10} height="16" rx="2.5" fill={bg} />
      <text x={(len * 5.7 + 10) / 2} y="11" textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="9" fontWeight={bold ? "800" : "700"} fill={fg} letterSpacing="0.5">
        {label}
      </text>
    </g>
  );
}

function FunctionRow({ y, name, meta, badge, tone }) {
  const fg = tone === "teal" ? TEAL : tone === "amber" ? AMBER_DEEP : MUTED;
  const bg = tone === "teal" ? TEAL_SOFT : tone === "amber" ? AMBER_SOFT : "rgba(148,163,184,0.08)";
  return (
    <g transform={`translate(12, ${y})`}>
      {/* Row icon — small play/lock square */}
      <rect x="0" y="0" width="22" height="22" rx="3" fill="rgba(17,22,32,0.8)" stroke={CARD_EDGE} strokeWidth="0.8" />
      <path d="M 8 7 L 15 11 L 8 15 Z" fill={fg} opacity="0.85" />
      {/* Name + meta */}
      <text x="30" y="10" fontFamily="Space Grotesk, sans-serif" fontSize="10.5" fontWeight="700" fill={INK}>
        {name}
      </text>
      <text x="30" y="21" fontFamily="Space Grotesk, sans-serif" fontSize="9" fill={DIM}>
        {meta}
      </text>
      {/* Badge on the right */}
      <g transform={`translate(${PANEL_W - 12 - (badge.length * 5.7 + 12) - 12}, 3)`}>
        <rect x="0" y="0" width={badge.length * 5.7 + 12} height="16" rx="2.5" fill={bg} />
        <text x={(badge.length * 5.7 + 12) / 2} y="11" textAnchor="middle" fontFamily="Space Grotesk, sans-serif" fontSize="9" fontWeight="700" fill={fg} letterSpacing="0.5">
          {badge}
        </text>
      </g>
    </g>
  );
}

// ─── Root ────────────────────────────────────────────────────────────────────
export default function HeroSurface() {
  return (
    <svg
      className="hero-surface"
      viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
      preserveAspectRatio="xMidYMid meet"
      role="img"
      aria-label="Protocol surface preview"
    >
      <Chrome />
      <ProgressStrip />
      {/* Edges (behind cards) */}
      <g>
        {EDGES.map((e) => <Edge key={`${e.from}-${e.to}`} e={e} />)}
      </g>
      {/* Cards */}
      {NODES.map((n) => <ContractCard key={n.id} n={n} />)}
      {/* Detail panel (covers right column, slides over at 0.76) */}
      <DetailPanel />
    </svg>
  );
}
