import React, { useMemo } from "react";

const RISK_COLOR = { high: "#f87171", medium: "#fbbf24", low: "#4ade80", unknown: "#64748b" };

function layoutGrid(contracts) {
  const n = contracts.length;
  if (!n) return [];
  const cols = Math.ceil(Math.sqrt(n * 1.7));
  const rows = Math.ceil(n / cols);
  return contracts.map((c, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    return { ...c, col, row, cols, rows };
  });
}

export default function SurfacePreview({ contracts, hierarchy, onOpenFullscreen }) {
  const positioned = useMemo(() => layoutGrid(contracts || []), [contracts]);
  const width = 900;
  const height = 506;
  const padX = 40;
  const padY = 40;

  const hasAny = positioned.length > 0;

  let cellW = 140;
  let cellH = 80;
  let gridW = 0;
  let gridH = 0;
  if (hasAny) {
    const cols = positioned[0].cols;
    const rows = positioned[0].rows;
    cellW = Math.min(160, Math.max(80, (width - padX * 2) / cols - 12));
    cellH = Math.min(70, Math.max(50, (height - padY * 2) / rows - 12));
    gridW = cols * (cellW + 12) - 12;
    gridH = rows * (cellH + 12) - 12;
  }
  const offsetX = (width - gridW) / 2;
  const offsetY = (height - gridH) / 2;

  const posByAddr = useMemo(() => {
    const m = new Map();
    for (const c of positioned) {
      const x = offsetX + c.col * (cellW + 12);
      const y = offsetY + c.row * (cellH + 12);
      m.set((c.address || "").toLowerCase(), { x, y, w: cellW, h: cellH, contract: c });
    }
    return m;
  }, [positioned, offsetX, offsetY, cellW, cellH]);

  const edges = useMemo(() => {
    const lines = [];
    for (const group of hierarchy || []) {
      const ownerAddr = (group.owner || "").toLowerCase();
      for (const child of group.contracts || []) {
        const fromPos = posByAddr.get(ownerAddr);
        const toPos = posByAddr.get((child.address || "").toLowerCase());
        if (!toPos) continue;
        if (fromPos) {
          lines.push({
            x1: fromPos.x + fromPos.w / 2,
            y1: fromPos.y + fromPos.h / 2,
            x2: toPos.x + toPos.w / 2,
            y2: toPos.y + toPos.h / 2,
          });
        }
      }
    }
    return lines;
  }, [hierarchy, posByAddr]);

  return (
    <section className="panel surface-preview">
      <div className="surface-preview-frame">
        <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="xMidYMid slice" width="100%" height="100%">
          <defs>
            <radialGradient id="surface-preview-bg" cx="50%" cy="50%" r="70%">
              <stop offset="0%" stopColor="rgba(45,212,191,0.06)" />
              <stop offset="100%" stopColor="rgba(11,15,22,0)" />
            </radialGradient>
          </defs>
          <rect x="0" y="0" width={width} height={height} fill="#12151a" />
          <rect x="0" y="0" width={width} height={height} fill="url(#surface-preview-bg)" />

          {edges.map((e, i) => (
            <line
              key={`edge-${i}`}
              x1={e.x1}
              y1={e.y1}
              x2={e.x2}
              y2={e.y2}
              stroke="rgba(148,163,184,0.22)"
              strokeWidth={1.4}
            />
          ))}

          {[...posByAddr.values()].map(({ x, y, w, h, contract }) => {
            const risk = RISK_COLOR[contract.risk_level] || RISK_COLOR.unknown;
            return (
              <g key={contract.address}>
                <rect
                  x={x}
                  y={y}
                  width={w}
                  height={h}
                  rx={8}
                  ry={8}
                  fill="#141a24"
                  stroke="rgba(148,163,184,0.2)"
                  strokeWidth={1}
                />
                <rect x={x} y={y} width={3} height={h} fill={risk} rx={1.5} />
                <text
                  x={x + 12}
                  y={y + 20}
                  fill="#e2e8f0"
                  fontSize="11"
                  fontWeight="700"
                  fontFamily="Space Grotesk, system-ui, sans-serif"
                >
                  {(contract.name || "Contract").slice(0, 18)}
                </text>
                <text
                  x={x + 12}
                  y={y + 36}
                  fill="#64748b"
                  fontSize="9"
                  fontFamily="JetBrains Mono, monospace"
                >
                  {(contract.address || "").slice(0, 10)}…
                </text>
                {contract.is_proxy && (
                  <g>
                    <rect
                      x={x + w - 38}
                      y={y + 8}
                      width={30}
                      height={12}
                      rx={3}
                      fill="rgba(245,158,11,0.14)"
                      stroke="rgba(245,158,11,0.3)"
                    />
                    <text
                      x={x + w - 23}
                      y={y + 17}
                      fill="#fbbf24"
                      fontSize="7"
                      fontWeight="700"
                      textAnchor="middle"
                      letterSpacing="0.08em"
                    >
                      PROXY
                    </text>
                  </g>
                )}
              </g>
            );
          })}
        </svg>
      </div>
      <div className="surface-preview-overlay">
        <span className="surface-preview-label">
          <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#2dd4bf" }} />
          Control surface · {positioned.length} contract{positioned.length === 1 ? "" : "s"}
        </span>
        <button className="surface-preview-fullscreen" onClick={onOpenFullscreen}>
          Open fullscreen ↗
        </button>
      </div>
    </section>
  );
}
