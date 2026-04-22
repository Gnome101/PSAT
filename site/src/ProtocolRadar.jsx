import React from "react";

function polarPoint(cx, cy, radius, angle) {
  return [cx + radius * Math.cos(angle), cy + radius * Math.sin(angle)];
}

export default function ProtocolRadar({ axes, size = 320 }) {
  const cx = size / 2;
  const cy = size / 2;
  const chartRadius = size * 0.36;
  const labelRadius = size * 0.46;
  const rings = 4;
  const count = axes.length;
  if (!count) return null;

  const angles = axes.map((_, i) => (Math.PI * 2 * i) / count - Math.PI / 2);
  const points = axes.map((axis, i) => {
    const r = chartRadius * Math.max(0, Math.min(1, axis.value));
    return polarPoint(cx, cy, r, angles[i]);
  });
  const pathD = points.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ") + " Z";

  return (
    <svg className="protocol-radar" viewBox={`0 0 ${size} ${size}`} role="img" aria-label="Protocol radar">
      {Array.from({ length: rings }, (_, r) => {
        const ringR = (chartRadius * (r + 1)) / rings;
        const ringPts = angles.map((a) => polarPoint(cx, cy, ringR, a));
        const d = ringPts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ") + " Z";
        return <path key={r} className="radar-grid" d={d} />;
      })}

      {angles.map((a, i) => {
        const [x, y] = polarPoint(cx, cy, chartRadius, a);
        return <line key={`axis-${i}`} className="radar-axis" x1={cx} y1={cy} x2={x} y2={y} />;
      })}

      <path className="radar-fill" d={pathD} />

      {points.map(([x, y], i) => (
        <circle key={`dot-${i}`} className="radar-dot" cx={x} cy={y} r={3.5} />
      ))}

      {axes.map((axis, i) => {
        const [lx, ly] = polarPoint(cx, cy, labelRadius, angles[i]);
        const anchor = Math.abs(Math.cos(angles[i])) < 0.25 ? "middle" : Math.cos(angles[i]) > 0 ? "start" : "end";
        return (
          <g key={`label-${i}`}>
            <text className="radar-label" x={lx} y={ly} textAnchor={anchor} dominantBaseline="middle">
              {axis.label}
            </text>
            {axis.display != null && (
              <text
                className="radar-value"
                x={lx}
                y={ly + 12}
                textAnchor={anchor}
                dominantBaseline="middle"
              >
                {axis.display}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}
