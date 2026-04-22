import React, { useEffect, useMemo, useRef, useState } from "react";
import { animate } from "animejs";

const COLS = 14;
const ROWS = 8;
const TILE_SIZE = 46;
const TILE_GAP = 8;
const GRID_PAD_X = 24;
const GRID_PAD_Y = 20;

const GRID_W = COLS * TILE_SIZE + (COLS - 1) * TILE_GAP;
const GRID_H = ROWS * TILE_SIZE + (ROWS - 1) * TILE_GAP;
const VIEW_W = GRID_W + GRID_PAD_X * 2;
const VIEW_H = GRID_H + GRID_PAD_Y * 2;
const BEAM_RADIUS = TILE_SIZE * 1.1;

function seededRandom(seed) {
  let s = seed | 0 || 1;
  return () => {
    s = (s * 1664525 + 1013904223) | 0;
    return ((s >>> 0) % 100000) / 100000;
  };
}

function buildTiles() {
  const rand = seededRandom(Date.now());
  const tiles = [];
  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const x = GRID_PAD_X + c * (TILE_SIZE + TILE_GAP);
      const y = GRID_PAD_Y + r * (TILE_SIZE + TILE_GAP);
      tiles.push({
        id: `${r}-${c}`,
        col: c,
        row: r,
        x,
        y,
        cx: x + TILE_SIZE / 2,
        cy: y + TILE_SIZE / 2,
        broken: rand() < 0.16,
      });
    }
  }
  return tiles;
}

export default function ScannerAnimation() {
  const svgRef = useRef(null);
  const beamRef = useRef(null);
  const tileRefs = useRef(new Map());
  const badgeRefs = useRef(new Map());
  const flaggedRef = useRef(new Set());
  const [flaggedCount, setFlaggedCount] = useState(0);

  const tiles = useMemo(buildTiles, []);
  const total = tiles.length;

  useEffect(() => {
    const state = { x: GRID_PAD_X };
    const anim = animate(state, {
      x: [GRID_PAD_X, GRID_PAD_X + GRID_W],
      duration: 4800,
      ease: "inOutSine",
      alternate: true,
      loop: true,
      onUpdate: () => {
        const beam = beamRef.current;
        if (beam) beam.setAttribute("transform", `translate(${state.x}, ${GRID_PAD_Y})`);

        for (const t of tiles) {
          const distance = Math.abs(t.cx - state.x);
          const tileEl = tileRefs.current.get(t.id);
          const badgeEl = badgeRefs.current.get(t.id);
          if (!tileEl) continue;

          const isFlagged = flaggedRef.current.has(t.id);
          const inBeam = distance < BEAM_RADIUS;
          const intensity = Math.max(0, 1 - distance / (BEAM_RADIUS * 1.6));

          if (inBeam && t.broken && !isFlagged) {
            flaggedRef.current.add(t.id);
            setFlaggedCount(flaggedRef.current.size);
            if (badgeEl) {
              animate(badgeEl, {
                opacity: [0, 1],
                scale: [0.3, 1.25, 1],
                duration: 520,
                ease: "out(3)",
              });
            }
          }

          if (isFlagged) {
            tileEl.setAttribute("data-state", "flagged");
            tileEl.style.opacity = (0.88 + intensity * 0.12).toFixed(3);
          } else if (inBeam) {
            tileEl.setAttribute("data-state", "scanning");
            tileEl.style.opacity = (0.55 + intensity * 0.45).toFixed(3);
          } else {
            tileEl.setAttribute("data-state", "idle");
            tileEl.style.opacity = (0.35 + Math.max(0, intensity * 0.2)).toFixed(3);
          }
        }
      },
    });

    return () => {
      anim.pause();
    };
  }, [tiles]);

  return (
    <div className="scanner">
      <div className="scanner-meta">
        <span className="scanner-dot" />
        <span className="scanner-meta-text">Scanning control surface</span>
        <span className="scanner-meta-sep">·</span>
        <span className="scanner-meta-flag">{flaggedCount} flagged</span>
        <span className="scanner-meta-sep">/</span>
        <span className="scanner-meta-total">{total} contracts</span>
      </div>
      <svg
        ref={svgRef}
        className="scanner-svg"
        viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-label="Audit coverage scanner"
      >
        <defs>
          <linearGradient id="scanner-beam" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="rgba(248, 113, 113, 0)" />
            <stop offset="35%" stopColor="rgba(248, 113, 113, 0.28)" />
            <stop offset="50%" stopColor="rgba(252, 165, 165, 0.72)" />
            <stop offset="65%" stopColor="rgba(248, 113, 113, 0.28)" />
            <stop offset="100%" stopColor="rgba(248, 113, 113, 0)" />
          </linearGradient>
          <filter id="scanner-glow" x="-30%" y="-30%" width="160%" height="160%">
            <feGaussianBlur stdDeviation="4" result="glow" />
            <feMerge>
              <feMergeNode in="glow" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <pattern id="scanner-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <line x1="0" y1="0" x2="0" y2="6" stroke="rgba(148,163,184,0.08)" strokeWidth="1" />
          </pattern>
        </defs>

        <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="#0d121b" />
        <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="url(#scanner-hatch)" />

        {/* Grid axis guides */}
        {Array.from({ length: ROWS + 1 }, (_, i) => {
          const y = GRID_PAD_Y + i * (TILE_SIZE + TILE_GAP) - TILE_GAP / 2;
          return (
            <line
              key={`h-${i}`}
              x1={GRID_PAD_X - 6}
              x2={GRID_PAD_X + GRID_W + 6}
              y1={y}
              y2={y}
              stroke="rgba(148,163,184,0.06)"
              strokeDasharray="2 6"
            />
          );
        })}

        {/* Tiles */}
        {tiles.map((t) => (
          <g key={t.id}>
            <rect
              ref={(el) => {
                if (el) tileRefs.current.set(t.id, el);
              }}
              className="scanner-tile"
              data-broken={t.broken ? "1" : "0"}
              data-state="idle"
              x={t.x}
              y={t.y}
              width={TILE_SIZE}
              height={TILE_SIZE}
              rx={6}
              ry={6}
              style={{ transformOrigin: `${t.cx}px ${t.cy}px` }}
            />
            <line
              className="scanner-tile-stripe"
              x1={t.x + 8}
              x2={t.x + TILE_SIZE - 8}
              y1={t.y + 14}
              y2={t.y + 14}
            />
            <line
              className="scanner-tile-stripe dim"
              x1={t.x + 8}
              x2={t.x + TILE_SIZE - 18}
              y1={t.y + 22}
              y2={t.y + 22}
            />
            {t.broken && (
              <g
                ref={(el) => {
                  if (el) badgeRefs.current.set(t.id, el);
                }}
                className="scanner-badge"
                style={{ opacity: 0, transformOrigin: `${t.cx}px ${t.cy}px` }}
              >
                <circle cx={t.x + TILE_SIZE - 10} cy={t.y + 10} r={7} fill="#f87171" />
                <text
                  x={t.x + TILE_SIZE - 10}
                  y={t.y + 14}
                  textAnchor="middle"
                  fill="#1b0404"
                  fontSize="11"
                  fontWeight="900"
                  fontFamily="Space Grotesk, system-ui, sans-serif"
                >
                  !
                </text>
              </g>
            )}
          </g>
        ))}

        {/* Scanning beam */}
        <g ref={beamRef} transform={`translate(${GRID_PAD_X}, ${GRID_PAD_Y})`}>
          <rect
            x={-BEAM_RADIUS}
            y={-12}
            width={BEAM_RADIUS * 2}
            height={GRID_H + 24}
            fill="url(#scanner-beam)"
            filter="url(#scanner-glow)"
          />
          <line
            x1={0}
            x2={0}
            y1={-12}
            y2={GRID_H + 12}
            stroke="#fecaca"
            strokeWidth={1.6}
            strokeOpacity={0.92}
            filter="url(#scanner-glow)"
          />
        </g>
      </svg>
    </div>
  );
}
