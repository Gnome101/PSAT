import React from "react";

/*
 * Industrial audit line — a Bauhaus-disciplined SVG illustration.
 *
 * Design rules:
 *   - Everything snaps to a 24-unit grid.
 *   - Palette is austere: white (INK), yellow (active), red (fail), teal
 *     reserved exclusively for the final APPROVED stamp. Green appears only
 *     inside the dependency monitor (status semantics).
 *   - No anthropomorphism — stations have camera apertures, sensor LED bars,
 *     and pilot lights instead of "eyes".
 *   - Actuators fire ONLY during the dwell window. Idle stations read as
 *     powered-down (single soft pilot LED) and are otherwise inert.
 *   - Belt has rollers at each endpoint and a diagonal chevron tread pattern.
 *   - Floor/support grid anchors the vertical run past the fold.
 *
 * Five stations along an L-shaped belt:
 *   01 AUDIT         light-sheet scanner in a blue-collar gantry
 *   02 UPGRADES      arm + monitor: proxy → impl graphic
 *   03 RESOLUTION    reader on a rail, sensor bar + red target laser
 *   04 DEPENDENCIES  overhead lens + monitor: dep graph (5 ok / 2 flag)
 *   05 VERDICT       stamp piston + trap; APPROVED plaque
 */

// ─── Grid ────────────────────────────────────────────────────────────────────
const U = 24; // grid unit
const VIEW_W = 55 * U; // 1320
const VIEW_H = 75 * U; // 1800

// Belt (stroke-centerline). Belt thickness = 2U = 48.
const BELT_Y = 9 * U;                      // 216
const BELT_X_START = 2 * U;                // 48
const BELT_X_END = 48 * U;                 // 1152
const CORNER_R = 3 * U;                    // 72
const BELT_X_COL = 51 * U;                 // 1224
const BELT_Y_TURN = BELT_Y + CORNER_R;     // 288
const BELT_Y_END = 71 * U;                 // 1704

const BELT_PATH =
  `M ${BELT_X_START} ${BELT_Y} ` +
  `L ${BELT_X_END} ${BELT_Y} ` +
  `Q ${BELT_X_COL} ${BELT_Y} ${BELT_X_COL} ${BELT_Y_TURN} ` +
  `L ${BELT_X_COL} ${BELT_Y_END}`;

const SEG_H = BELT_X_END - BELT_X_START;
const SEG_CORNER = (Math.PI * CORNER_R) / 2;
const SEG_V = BELT_Y_END - BELT_Y_TURN;
const PATH_TOTAL = SEG_H + SEG_CORNER + SEG_V;

// Station anchor points (always ON the belt centerline, snapped to U).
const STN = {
  scanner:      { x: 14 * U, y: BELT_Y },            // 336
  upgrades:     { x: 34 * U, y: BELT_Y },            // 816
  resolution:   { x: BELT_X_COL, y: 30 * U },        // 720
  dependencies: { x: BELT_X_COL, y: 50 * U },        // 1200
  verdict:      { x: BELT_X_COL, y: BELT_Y_END },    // 1704
};

// Fractional positions along the belt for each station.
const pathFrac = (x, y) => {
  if (y === BELT_Y) return (x - BELT_X_START) / PATH_TOTAL;
  return (SEG_H + SEG_CORNER + (y - BELT_Y_TURN)) / PATH_TOTAL;
};
const FRAC = {
  scanner: pathFrac(STN.scanner.x, STN.scanner.y),
  upgrades: pathFrac(STN.upgrades.x, STN.upgrades.y),
  resolution: pathFrac(STN.resolution.x, STN.resolution.y),
  dependencies: pathFrac(STN.dependencies.x, STN.dependencies.y),
  verdict: 1,
};

// ─── Timing ──────────────────────────────────────────────────────────────────
// Full trip = 24s; boxes staggered by 8s. Each station fires on an 8s loop
// with `begin = (dwell_start_in_cycle) % 8`, so its active window lines up
// with every box arrival.
const CYCLE = 24;
const STATION_LOOP = CYCLE / 3; // 8s
const DWELL_DUR = 0.08 * CYCLE; // 1.92s (~2s)

// Box motion timeline (fractions of CYCLE). Dwell = 0.08.
const TL = {
  scannerIn: 0.08, scannerOut: 0.16,
  upgradesIn: 0.27, upgradesOut: 0.35,
  resolutionIn: 0.55, resolutionOut: 0.63,
  depsIn: 0.75, depsOut: 0.83,
  verdictIn: 0.95, verdictOut: 1.00,
};

const BOX_KEY_TIMES = [
  0, TL.scannerIn, TL.scannerOut,
  TL.upgradesIn, TL.upgradesOut,
  TL.resolutionIn, TL.resolutionOut,
  TL.depsIn, TL.depsOut,
  TL.verdictIn, 1,
].map((v) => v.toFixed(4)).join("; ");

const BOX_KEY_POINTS = [
  0,
  FRAC.scanner, FRAC.scanner,
  FRAC.upgrades, FRAC.upgrades,
  FRAC.resolution, FRAC.resolution,
  FRAC.dependencies, FRAC.dependencies,
  FRAC.verdict, FRAC.verdict,
].map((v) => v.toFixed(5)).join("; ");

const BEGIN = {
  scanner: TL.scannerIn * CYCLE,                        // 1.92
  upgrades: TL.upgradesIn * CYCLE,                      // 6.48
  resolution: (TL.resolutionIn * CYCLE) % STATION_LOOP, // 13.2 % 8 = 5.2
  dependencies: (TL.depsIn * CYCLE) % STATION_LOOP,     // 18 % 8 = 2
  verdict: (TL.verdictIn * CYCLE) % STATION_LOOP,       // 22.8 % 8 = 6.8
};

// keyTimes inside the 8s station loop: idle, active (dwell), idle.
const STATION_KT = [
  0,
  0.008,
  DWELL_DUR / STATION_LOOP,
  DWELL_DUR / STATION_LOOP + 0.008,
  1,
].map((v) => v.toFixed(4)).join("; ");
const V_OFF = "0; 1; 1; 0; 0";
const V_ON = "1; 0; 0; 1; 1"; // inverse for pilot lights that DIM during dwell

// ─── Palette ─────────────────────────────────────────────────────────────────
const INK = "#f1f5f9";
const INK_SOFT = "#94a3b8";
const STROKE_SOFT = "#475569";
const BELT_FILL = "#0d121b";
const BELT_TREAD = "#1b2434";
const PANEL = "#151b26";
const PANEL_DARK = "#0a0f17";
const YELLOW = "#fbbf24";
const YELLOW_DIM = "#7a5f17";
const RED = "#ef4444";
const RED_DIM = "#7a2121";
const TEAL = "#2dd4bf";
const GREEN = "#22c55e";
const BOX_FACE = "#e9e2cc";
const BOX_SHADOW = "#b6ad92";

// ─── Defs ────────────────────────────────────────────────────────────────────

function Defs() {
  return (
    <defs>
      {/* Scrolling chevron tread pattern, horizontal */}
      <pattern id="al-treads-h" x="0" y="0" width="24" height="48" patternUnits="userSpaceOnUse">
        <rect width="24" height="48" fill={BELT_FILL} />
        <path d="M 0 8 L 12 24 L 0 40" fill="none" stroke={BELT_TREAD} strokeWidth="2.5" strokeLinejoin="miter" />
        <animateTransform attributeName="patternTransform" type="translate" from="0 0" to="-24 0" dur="0.8s" repeatCount="indefinite" />
      </pattern>
      {/* Scrolling chevron tread pattern, vertical (chevron points down) */}
      <pattern id="al-treads-v" x="0" y="0" width="48" height="24" patternUnits="userSpaceOnUse">
        <rect width="48" height="24" fill={BELT_FILL} />
        <path d="M 8 0 L 24 12 L 40 0" fill="none" stroke={BELT_TREAD} strokeWidth="2.5" strokeLinejoin="miter" />
        <animateTransform attributeName="patternTransform" type="translate" from="0 0" to="0 24" dur="0.8s" repeatCount="indefinite" />
      </pattern>
      {/* Subtle floor grid for the lower half */}
      <pattern id="al-floor" x="0" y="0" width="96" height="96" patternUnits="userSpaceOnUse">
        <path d="M 0 96 L 96 96 M 96 0 L 96 96" fill="none" stroke={INK} strokeWidth="0.5" opacity="0.05" />
      </pattern>
      <path id="assembly-belt" d={BELT_PATH} />
    </defs>
  );
}

// ─── Belt ────────────────────────────────────────────────────────────────────
// Belt is drawn in three pieces: horizontal slab, quarter-arc corner, vertical
// slab — each with clean rail strokes on top + bottom edges, and rollers at
// the endpoints.

function Belt() {
  return (
    <g>
      {/* Faint floor grid behind everything */}
      <rect x="0" y={BELT_Y + 48} width={VIEW_W} height={VIEW_H - BELT_Y - 48} fill="url(#al-floor)" />

      {/* Horizontal belt body */}
      <rect
        x={BELT_X_START}
        y={BELT_Y - U}
        width={BELT_X_END - BELT_X_START}
        height={2 * U}
        fill="url(#al-treads-h)"
      />
      {/* Horizontal rails */}
      <line x1={BELT_X_START} y1={BELT_Y - U} x2={BELT_X_END} y2={BELT_Y - U} stroke={INK} strokeWidth="1.5" />
      <line x1={BELT_X_START} y1={BELT_Y + U} x2={BELT_X_END} y2={BELT_Y + U} stroke={INK} strokeWidth="1.5" />

      {/* Corner — quarter annulus: outer edge = top rail curve, inner = bottom rail curve */}
      <path
        d={
          `M ${BELT_X_END} ${BELT_Y - U} ` +
          `Q ${BELT_X_COL + U} ${BELT_Y - U} ${BELT_X_COL + U} ${BELT_Y_TURN} ` +
          `L ${BELT_X_COL - U} ${BELT_Y_TURN} ` +
          `Q ${BELT_X_COL - U} ${BELT_Y + U} ${BELT_X_END} ${BELT_Y + U} Z`
        }
        fill={BELT_FILL}
        stroke={INK}
        strokeWidth="1.5"
      />

      {/* Vertical belt body */}
      <rect
        x={BELT_X_COL - U}
        y={BELT_Y_TURN}
        width={2 * U}
        height={BELT_Y_END - BELT_Y_TURN}
        fill="url(#al-treads-v)"
      />
      {/* Vertical rails */}
      <line x1={BELT_X_COL - U} y1={BELT_Y_TURN} x2={BELT_X_COL - U} y2={BELT_Y_END} stroke={INK} strokeWidth="1.5" />
      <line x1={BELT_X_COL + U} y1={BELT_Y_TURN} x2={BELT_X_COL + U} y2={BELT_Y_END} stroke={INK} strokeWidth="1.5" />

      {/* Rollers — start of horizontal belt */}
      <Roller cx={BELT_X_START} cy={BELT_Y} />
      {/* End of vertical belt (terminal) */}
      <Roller cx={BELT_X_COL} cy={BELT_Y_END} />

      {/* Support struts under the vertical belt, every 8U between stations */}
      {[36, 40, 44, 56, 60, 64].map((uy) => (
        <g key={uy} transform={`translate(${BELT_X_COL + U}, ${uy * U})`}>
          <line x1="0" y1="0" x2={1.5 * U} y2="0" stroke={INK} strokeWidth="1" opacity="0.35" />
        </g>
      ))}
    </g>
  );
}

function Roller({ cx, cy }) {
  return (
    <g transform={`translate(${cx}, ${cy})`}>
      <circle r={1.5 * U} fill={PANEL_DARK} stroke={INK} strokeWidth="1.5" />
      <circle r={0.75 * U} fill="none" stroke={INK} strokeWidth="1" />
      <circle r={0.2 * U} fill={INK} />
      {/* Axle spokes */}
      <line x1={-1.5 * U} y1="0" x2={1.5 * U} y2="0" stroke={INK} strokeWidth="0.7" opacity="0.45" />
      <line x1="0" y1={-1.5 * U} x2="0" y2={1.5 * U} stroke={INK} strokeWidth="0.7" opacity="0.45" />
    </g>
  );
}

// ─── Reusable pieces ─────────────────────────────────────────────────────────

// Industrial sensor module: camera lens + 5 status LEDs. One LED lights
// yellow during dwell; otherwise the strip is dim. Pilot light in the
// lens catchlight blinks steady (alive but inert).
function SensorModule({ w = 4 * U, h = 2 * U, begin, facing = "down" }) {
  // `facing="down"`: lens on lower edge, aiming at belt below
  // `facing="right"`: lens on right edge, aiming at belt to the right
  const isDown = facing === "down";
  const cx = 0;
  const cy = isDown ? -0.25 * h : 0;
  const lensCx = cx;
  const lensCy = isDown ? h * 0.1 - h * 0.5 : 0;
  return (
    <g>
      {/* Housing */}
      <rect x={-w / 2} y={-h} width={w} height={h} fill={PANEL} stroke={INK} strokeWidth="2" />
      <rect x={-w / 2 + 3} y={-h + 3} width={w - 6} height={h - 6} fill="none" stroke={INK} strokeWidth="0.5" opacity="0.25" />
      {/* Lens aperture */}
      <g transform={`translate(${lensCx}, ${lensCy})`}>
        <circle r="10" fill={PANEL_DARK} stroke={INK} strokeWidth="1.4" />
        <circle r="6.5" fill="#05080d" stroke={INK} strokeWidth="0.6" />
        <circle r="2.4" fill={INK} opacity="0.9" />
        {/* Pilot heartbeat (always alive) */}
        <circle cx="-3.5" cy="-3.5" r="1.2" fill={INK}>
          <animate attributeName="opacity" values="0.2; 1; 0.2" dur="2.4s" repeatCount="indefinite" />
        </circle>
        {/* Active ring — brightens during dwell */}
        <circle r="12.5" fill="none" stroke={YELLOW} strokeWidth="1.4" opacity="0">
          <animate attributeName="opacity" values={V_OFF} keyTimes={STATION_KT} dur={`${STATION_LOOP}s`} begin={`${begin}s`} repeatCount="indefinite" />
        </circle>
      </g>
      {/* LED strip at the bottom of the housing */}
      <g transform={`translate(${-w / 2 + 8}, ${-4})`}>
        {[0, 1, 2, 3, 4].map((i) => (
          <rect key={i} x={i * 10} y={-5} width="6" height="4" fill={STROKE_SOFT} opacity="0.5" />
        ))}
        {/* The "active" LED slot — lights yellow during dwell */}
        <rect x={20} y={-5} width="6" height="4" fill={YELLOW} opacity="0">
          <animate attributeName="opacity" values={V_OFF} keyTimes={STATION_KT} dur={`${STATION_LOOP}s`} begin={`${begin}s`} repeatCount="indefinite" />
        </rect>
      </g>
    </g>
  );
}

// Station number + name plate. Strict Bauhaus typographic rhythm.
function StationTag({ x, y, index, name, anchor = "middle" }) {
  return (
    <g transform={`translate(${x}, ${y})`}>
      <text
        textAnchor={anchor}
        fontFamily="Space Grotesk, sans-serif"
        fontSize="11"
        fontWeight="700"
        letterSpacing="3"
        fill={INK}
      >
        <tspan fill={YELLOW}>{index}</tspan>
        <tspan dx="12">{name}</tspan>
      </text>
    </g>
  );
}

// ─── Contract box ────────────────────────────────────────────────────────────
function Box({ label }) {
  return (
    <g>
      {/* Drop shadow on belt */}
      <ellipse cx="0" cy="22" rx="24" ry="3" fill="#000" opacity="0.35" />
      {/* Box back plate */}
      <rect x="-26" y="-20" width="52" height="40" fill={BOX_SHADOW} />
      {/* Box front face */}
      <rect x="-26" y="-20" width="52" height="40" fill={BOX_FACE} stroke={INK} strokeWidth="1.6" />
      {/* Faux "document lines" (contract bytecode look) */}
      <line x1="-20" y1="-12" x2="12" y2="-12" stroke={INK_SOFT} strokeWidth="1" opacity="0.4" />
      <line x1="-20" y1="-6" x2="8" y2="-6" stroke={INK_SOFT} strokeWidth="1" opacity="0.4" />
      {/* Divider */}
      <line x1="-26" y1="-4" x2="26" y2="-4" stroke={BOX_SHADOW} strokeWidth="0.8" />
      {/* Address label */}
      <text
        x="0"
        y="12"
        textAnchor="middle"
        fontFamily="JetBrains Mono, monospace"
        fontSize="11"
        fontWeight="700"
        fill="#0b0f16"
      >
        {label}
      </text>
    </g>
  );
}

function MovingBox({ begin, label }) {
  return (
    <g>
      <Box label={label} />
      <animateMotion
        dur={`${CYCLE}s`}
        repeatCount="indefinite"
        begin={begin}
        keyPoints={BOX_KEY_POINTS}
        keyTimes={BOX_KEY_TIMES}
        calcMode="linear"
      >
        <mpath href="#assembly-belt" />
      </animateMotion>
    </g>
  );
}

// ─── Station 01: AUDIT ───────────────────────────────────────────────────────
function ScannerStation({ x, y }) {
  const b = `${BEGIN.scanner}s`;
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Twin support posts */}
      <rect x={-5 * U} y={-8 * U} width="4" height={8 * U} fill={INK} />
      <rect x={5 * U - 4} y={-8 * U} width="4" height={8 * U} fill={INK} />
      {/* Horizontal beam */}
      <rect x={-5 * U} y={-8 * U} width={10 * U} height={0.33 * U} fill={INK} />

      {/* Emitter housing */}
      <g transform={`translate(0, ${-6.5 * U})`}>
        <SensorModule w={5 * U} h={2.5 * U} begin={BEGIN.scanner} />
      </g>

      {/* Light sheet — fires during dwell, sweeps across box */}
      <g opacity="0">
        <animate attributeName="opacity" values={V_OFF} keyTimes={STATION_KT} dur={`${STATION_LOOP}s`} begin={b} repeatCount="indefinite" />
        <g>
          <animateTransform
            attributeName="transform"
            type="translate"
            values={`${-U} 0; ${U} 0; ${U} 0; ${-U} 0`}
            keyTimes="0; 0.45; 0.55; 1"
            dur={`${DWELL_DUR}s`}
            begin={b}
            repeatCount="indefinite"
          />
          {/* Fan beam from emitter down to belt */}
          <path d={`M -2 ${-4 * U} L ${-0.5 * U} ${U} L ${0.5 * U} ${U} L 2 ${-4 * U} Z`} fill={YELLOW} opacity="0.10" />
          <line x1="0" y1={-4 * U} x2="0" y2={U} stroke={YELLOW} strokeWidth="1" opacity="0.9" />
          <rect x="-1" y={U - 2} width="2" height="4" fill={YELLOW} />
        </g>
      </g>

      <StationTag x={0} y={-9 * U - 4} index="01" name="AUDIT" />
    </g>
  );
}

// ─── Station 02: UPGRADES ────────────────────────────────────────────────────
function UpgradesStation({ x, y }) {
  const b = `${BEGIN.upgrades}s`;
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Frame */}
      <rect x={-3 * U} y={-8 * U} width="4" height={8 * U} fill={INK} />
      <rect x={3 * U - 4} y={-8 * U} width="4" height={8 * U} fill={INK} />
      <rect x={-3 * U} y={-8 * U} width={6 * U} height={0.33 * U} fill={INK} />

      {/* Sensor module (no face) */}
      <g transform={`translate(0, ${-5 * U})`}>
        <SensorModule w={4 * U} h={2 * U} begin={BEGIN.upgrades} />
      </g>

      {/* Articulated arm — descends during dwell */}
      <g>
        <animateTransform
          attributeName="transform"
          type="translate"
          values={`0 0; 0 ${1.5 * U}; 0 ${1.5 * U}; 0 0; 0 0`}
          keyTimes={STATION_KT}
          dur={`${STATION_LOOP}s`}
          begin={b}
          repeatCount="indefinite"
          additive="sum"
        />
        <rect x="-2" y={-3 * U} width="4" height={1.5 * U} fill={INK} />
        <circle cx="0" cy={-1.5 * U} r="4" fill={YELLOW} stroke={INK} strokeWidth="1.4" />
        <rect x="-2" y={-1.5 * U} width="4" height={1.25 * U} fill={INK} />
        {/* Clamp */}
        <rect x={-0.6 * U} y={0.25 * U} width="4" height={0.6 * U} fill={INK} />
        <rect x={0.25 * U} y={0.25 * U} width="4" height={0.6 * U} fill={INK} />
      </g>

      {/* Monitor — impl/proxy diagram (legible, no hex soup) */}
      <Monitor
        x={4 * U}
        y={-8 * U}
        w={10 * U}
        h={6 * U}
        title="UPGRADES"
        begin={BEGIN.upgrades}
      >
        <UpgradesScreen begin={BEGIN.upgrades} />
      </Monitor>
    </g>
  );
}

// ─── Station 03: RESOLUTION ──────────────────────────────────────────────────
function ResolutionStation({ x, y }) {
  const b = `${BEGIN.resolution}s`;
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Vertical column */}
      <rect x={-7 * U} y={-3 * U} width="4" height={6 * U} fill={INK} />

      {/* Reader assembly rides a horizontal rail that extends to box */}
      <g>
        <animateTransform
          attributeName="transform"
          type="translate"
          values={`0 0; ${1.5 * U} 0; ${1.5 * U} 0; 0 0; 0 0`}
          keyTimes={STATION_KT}
          dur={`${STATION_LOOP}s`}
          begin={b}
          repeatCount="indefinite"
          additive="sum"
        />
        {/* Rail */}
        <rect x={-7 * U} y="-2" width={5 * U} height="4" fill={STROKE_SOFT} />
        {/* Reader body with sensor module on its right-facing side */}
        <g transform={`translate(${-4.5 * U}, 0)`}>
          <SensorModule w={2.5 * U} h={1.8 * U} begin={BEGIN.resolution} facing="right" />
        </g>
        {/* Red target laser during dwell */}
        <g opacity="0">
          <animate attributeName="opacity" values={V_OFF} keyTimes={STATION_KT} dur={`${STATION_LOOP}s`} begin={b} repeatCount="indefinite" />
          <line x1={-3 * U} y1="0" x2="-24" y2="0" stroke={RED} strokeWidth="1.2" strokeDasharray="4 3" />
          <circle cx="-24" cy="0" r="3" fill={RED} />
        </g>
      </g>

      <StationTag x={-5 * U} y={3 * U + 20} index="03" name="RESOLUTION" />
    </g>
  );
}

// ─── Station 04: DEPENDENCIES ────────────────────────────────────────────────
function DependenciesStation({ x, y }) {
  const b = `${BEGIN.dependencies}s`;
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Vertical column + cross-bar */}
      <rect x={-9 * U} y={-4 * U} width="4" height={8 * U} fill={INK} />
      <rect x={-9 * U} y="-2" width={6 * U} height="4" fill={INK} />

      {/* Overhead sensor module facing down */}
      <g transform={`translate(${-3.5 * U}, 0)`}>
        <SensorModule w={3 * U} h={2 * U} begin={BEGIN.dependencies} facing="down" />
      </g>

      {/* Downward scan beam — narrow, bright, only during dwell */}
      <g opacity="0">
        <animate attributeName="opacity" values={V_OFF} keyTimes={STATION_KT} dur={`${STATION_LOOP}s`} begin={b} repeatCount="indefinite" />
        <path d={`M ${-3.5 * U - 6} 0 L ${-U - 6} ${U} L ${-U + 6} ${U} L ${-3.5 * U + 6} 0 Z`} fill={YELLOW} opacity="0.10" />
        <line x1={-3.5 * U} y1="0" x2={-U} y2={U} stroke={YELLOW} strokeWidth="1" />
      </g>

      {/* Monitor */}
      <Monitor
        x={-9 * U - 1}
        y={-5 * U}
        w={6 * U}
        h={5 * U}
        title="DEPENDENCIES"
        anchorRight
        begin={BEGIN.dependencies}
      >
        <DependenciesScreen begin={BEGIN.dependencies} />
      </Monitor>
    </g>
  );
}

// ─── Station 05: VERDICT ─────────────────────────────────────────────────────
function VerdictStation({ x, y }) {
  const b = `${BEGIN.verdict}s`;
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Frame */}
      <rect x={-4 * U} y={-5 * U} width="4" height={3 * U} fill={INK} />
      <rect x={4 * U - 4} y={-5 * U} width="4" height={3 * U} fill={INK} />
      <rect x={-4 * U} y={-5 * U} width={8 * U} height="4" fill={INK} />

      {/* Stamp piston — slams down during dwell */}
      <g>
        <animateTransform
          attributeName="transform"
          type="translate"
          values={`0 0; 0 ${1.75 * U}; 0 ${1.75 * U}; 0 0; 0 0`}
          keyTimes={STATION_KT}
          dur={`${STATION_LOOP}s`}
          begin={b}
          repeatCount="indefinite"
          additive="sum"
        />
        <rect x="-3" y={-5 * U} width="6" height={3 * U} fill={INK} />
        {/* Stamp head (yellow brick) */}
        <rect x={-1.5 * U} y={-2 * U} width={3 * U} height={U} fill={YELLOW} stroke={INK} strokeWidth="2" />
        {/* Bauhaus cross on the face */}
        <rect x={-0.75 * U} y={-1.75 * U} width={1.5 * U} height="2" fill={INK} />
        <rect x="-1" y={-1.75 * U} width="2" height={U - 4} fill={INK} />
      </g>

      {/* Trap door to the LEFT */}
      <g transform={`translate(${-2.75 * U}, 0)`}>
        <rect x={-1.1 * U} y={-U} width={2.2 * U} height={2 * U} fill={PANEL_DARK} stroke={RED} strokeWidth="2" />
        <line x1={-1.1 * U} y1={-U} x2={1.1 * U} y2={U} stroke={RED} strokeWidth="2" />
        <line x1={1.1 * U} y1={-U} x2={-1.1 * U} y2={U} stroke={RED} strokeWidth="2" />
        <text
          x="0"
          y={U + 20}
          textAnchor="middle"
          fontFamily="Space Grotesk, sans-serif"
          fontSize="10"
          fontWeight="700"
          letterSpacing="2.5"
          fill={RED}
        >
          REJECT
        </text>
      </g>

      <StationTag x={0} y={-5 * U - 12} index="05" name="VERDICT" />

      {/* APPROVED plaque — teal only, below the belt, well separated from REJECT */}
      <g transform={`translate(${3 * U}, ${3 * U + 12})`}>
        <rect x={-2 * U} y={-0.5 * U} width={4 * U} height={U} fill={TEAL} />
        <rect x={-2 * U + 2} y={-0.5 * U + 2} width={4 * U - 4} height={U - 4} fill="none" stroke="#0b0f16" strokeWidth="1" opacity="0.25" />
        <text
          x="0"
          y="4"
          textAnchor="middle"
          fontFamily="Space Grotesk, sans-serif"
          fontSize="11"
          fontWeight="700"
          letterSpacing="2.5"
          fill="#0b0f16"
        >
          APPROVED
        </text>
      </g>
    </g>
  );
}

// ─── Monitor shell ───────────────────────────────────────────────────────────
function Monitor({ x, y, w, h, title, begin, children, anchorRight = false }) {
  return (
    <g transform={`translate(${x}, ${y})`}>
      {/* Outer bezel */}
      <rect x="0" y="0" width={w} height={h} fill={PANEL} stroke={INK} strokeWidth="2" />
      {/* Title bar — subtle, no loud blue */}
      <line x1="0" y1={U - 2} x2={w} y2={U - 2} stroke={INK} strokeWidth="0.8" opacity="0.3" />
      <text
        x={anchorRight ? w - 10 : 10}
        y="16"
        textAnchor={anchorRight ? "end" : "start"}
        fontFamily="Space Grotesk, sans-serif"
        fontSize="10"
        fontWeight="700"
        letterSpacing="2.5"
        fill={INK}
      >
        {title}
      </text>
      {/* Power LED — white dim when idle, green when polling */}
      <circle cx={anchorRight ? 10 : w - 10} cy="11" r="2.5" fill={STROKE_SOFT} />
      <circle cx={anchorRight ? 10 : w - 10} cy="11" r="2.5" fill={GREEN}>
        <animate attributeName="opacity" values="0.35; 1; 0.35" dur="2.2s" repeatCount="indefinite" />
      </circle>
      {/* Screen inset */}
      <rect x="6" y={U} width={w - 12} height={h - U - 6} fill={PANEL_DARK} stroke={INK} strokeWidth="0.5" opacity="0.85" />
      {/* Subtle scanlines */}
      <g opacity="0.12">
        {[...Array(Math.floor((h - U - 6) / 5))].map((_, i) => (
          <line key={i} x1="6" y1={U + i * 5 + 3} x2={w - 6} y2={U + i * 5 + 3} stroke={INK} strokeWidth="0.35" />
        ))}
      </g>
      {/* Content */}
      <g transform={`translate(0, ${U})`}>{children}</g>
    </g>
  );
}

// ─── Monitor content: UPGRADES (proxy → impl diagram) ────────────────────────
function UpgradesScreen({ begin }) {
  return (
    <g fontFamily="Space Grotesk, sans-serif">
      {/* Two boxed labels with arrow between */}
      <g transform="translate(16, 28)">
        <rect width="62" height="22" fill="none" stroke={INK} strokeWidth="1.4" />
        <text x="31" y="15" textAnchor="middle" fontSize="10" fontWeight="700" fill={INK} letterSpacing="1.5">PROXY</text>
      </g>
      <g transform="translate(86, 28)">
        <line x1="0" y1="11" x2="26" y2="11" stroke={INK} strokeWidth="1.4" />
        <path d="M 22 7 L 26 11 L 22 15" fill="none" stroke={INK} strokeWidth="1.4" />
      </g>
      <g transform="translate(116, 28)">
        <rect width="62" height="22" fill={YELLOW} stroke={INK} strokeWidth="1.4" />
        <text x="31" y="15" textAnchor="middle" fontSize="10" fontWeight="700" fill="#0b0f16" letterSpacing="1.5">IMPL</text>
      </g>
      {/* Address line under */}
      <text x="16" y="68" fontFamily="JetBrains Mono, monospace" fontSize="10" fill={INK_SOFT}>
        0x7a3f…c91b
      </text>
      <text x="16" y="82" fontFamily="JetBrains Mono, monospace" fontSize="10" fill={INK}>
        0x1d82…0aa0
        <animate attributeName="opacity" values="0.35; 1; 1; 0.35; 0.35" keyTimes={STATION_KT} dur={`${STATION_LOOP}s`} begin={`${begin}s`} repeatCount="indefinite" />
      </text>
      {/* Counter */}
      <text x={10 * U - 20} y={4 * U + 14} textAnchor="end" fontFamily="JetBrains Mono, monospace" fontSize="9" fill={YELLOW}>
        1 IMPL
      </text>
    </g>
  );
}

// ─── Monitor content: DEPENDENCIES (tree graph) ──────────────────────────────
function DependenciesScreen({ begin }) {
  const nodes = [
    { id: "a", cx: 20, cy: 46, status: "ok" },
    { id: "b", cx: 58, cy: 26, status: "ok" },
    { id: "c", cx: 58, cy: 66, status: "bad" },
    { id: "d", cx: 104, cy: 14, status: "ok" },
    { id: "e", cx: 104, cy: 38, status: "ok" },
    { id: "f", cx: 104, cy: 66, status: "bad" },
    { id: "g", cx: 104, cy: 86, status: "ok" },
  ];
  const edges = [["a", "b"], ["a", "c"], ["b", "d"], ["b", "e"], ["c", "f"], ["c", "g"]];
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));
  return (
    <g>
      {edges.map(([u, v], i) => (
        <line
          key={i}
          x1={byId[u].cx}
          y1={byId[u].cy}
          x2={byId[v].cx}
          y2={byId[v].cy}
          stroke={STROKE_SOFT}
          strokeWidth="1"
        />
      ))}
      {nodes.map((n) => {
        const color = n.status === "ok" ? GREEN : RED;
        return (
          <g key={n.id}>
            <circle cx={n.cx} cy={n.cy} r="5.5" fill={PANEL_DARK} stroke={color} strokeWidth="1.6" />
            <circle cx={n.cx} cy={n.cy} r="2.8" fill={color} />
            {/* Pulse on dwell */}
            <circle cx={n.cx} cy={n.cy} r="5.5" fill="none" stroke={color} strokeWidth="1.2" opacity="0">
              <animate attributeName="r" values="5.5; 11" keyTimes="0; 1" dur={`${DWELL_DUR}s`} begin={`${begin}s`} repeatCount="indefinite" />
              <animate attributeName="opacity" values="0.85; 0" keyTimes="0; 1" dur={`${DWELL_DUR}s`} begin={`${begin}s`} repeatCount="indefinite" />
            </circle>
          </g>
        );
      })}
      {/* Legend */}
      <g transform="translate(10, 108)" fontFamily="JetBrains Mono, monospace" fontSize="9">
        <rect x="0" y="-7" width="7" height="7" fill={GREEN} />
        <text x="12" y="-1" fill={INK}>OK 5</text>
        <rect x="52" y="-7" width="7" height="7" fill={RED} />
        <text x="64" y="-1" fill={INK}>FLAG 2</text>
      </g>
    </g>
  );
}

// ─── Root ────────────────────────────────────────────────────────────────────
export default function AssemblyLine() {
  return (
    <svg
      className="assembly-line"
      viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
      preserveAspectRatio="xMaxYMin meet"
      role="img"
      aria-label="Smart contract audit assembly line"
    >
      <Defs />
      <Belt />

      <ScannerStation {...STN.scanner} />
      <UpgradesStation {...STN.upgrades} />
      <ResolutionStation {...STN.resolution} />
      <DependenciesStation {...STN.dependencies} />
      <VerdictStation {...STN.verdict} />

      <MovingBox begin="0s" label="0x1a" />
      <MovingBox begin={`-${STATION_LOOP}s`} label="0x7c" />
      <MovingBox begin={`-${(STATION_LOOP * 2).toFixed(2)}s`} label="0xff" />
    </svg>
  );
}
