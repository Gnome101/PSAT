import React, { useEffect, useRef, useState } from "react";

function polarPoint(cx, cy, radius, angle) {
  return [cx + radius * Math.cos(angle), cy + radius * Math.sin(angle)];
}

function axisIndexFromUrl(axes) {
  const params = new URLSearchParams(window.location.search);
  const key = params.get("scoreAxis");
  if (!key) return null;
  const index = axes.findIndex((axis) => axis.key === key);
  return index >= 0 ? index : null;
}

function setUrlAxis(axisKey) {
  const url = new URL(window.location.href);
  if (axisKey) url.searchParams.set("scoreAxis", axisKey);
  else url.searchParams.delete("scoreAxis");
  window.history.replaceState({}, "", url.toString());
}

export default function ProtocolRadar({
  axes,
  size = 320,
  labelRadius: labelRadiusCoef = 0.46,
  labelInset = 0,
  labelInsetX = labelInset,
  labelInsetY = labelInset,
  onExampleClick,
}) {
  const wrapRef = useRef(null);
  const [selectedIndex, setSelectedIndex] = useState(() => axisIndexFromUrl(axes));
  const viewWidth = size + labelInsetX * 2;
  const viewHeight = size + labelInsetY * 2;
  const cx = labelInsetX + size / 2;
  const cy = labelInsetY + size / 2;
  const chartRadius = size * 0.36;
  const labelRadius = size * labelRadiusCoef;
  const rings = 4;
  const count = axes.length;
  if (!count) return null;

  const safeSelectedIndex = selectedIndex == null ? null : Math.min(selectedIndex, count - 1);
  const selectedAxis = safeSelectedIndex == null ? null : axes[safeSelectedIndex];
  const selectedDetail = selectedAxis?.tooltip;
  const negativeExamples = selectedDetail?.negativeExamples || [];

  useEffect(() => {
    setSelectedIndex((current) => {
      if (current != null && axes[current]) return current;
      return axisIndexFromUrl(axes);
    });
  }, [axes]);

  useEffect(() => {
    function onPointerDown(event) {
      if (selectedIndex == null) return;
      const target = event.target;
      if (
        wrapRef.current?.contains(target)
        && target.closest(".radar-label-group, .radar-negative-item")
      ) {
        return;
      }
      setSelectedIndex(null);
      setUrlAxis(null);
    }
    document.addEventListener("pointerdown", onPointerDown, true);
    return () => document.removeEventListener("pointerdown", onPointerDown, true);
  }, [selectedIndex]);

  useEffect(() => {
    function onPopState() {
      setSelectedIndex(axisIndexFromUrl(axes));
    }
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [axes]);

  const angles = axes.map((_, i) => (Math.PI * 2 * i) / count - Math.PI / 2);
  const points = axes.map((axis, i) => {
    const r = chartRadius * Math.max(0, Math.min(1, axis.value));
    return polarPoint(cx, cy, r, angles[i]);
  });
  const pathD = points.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ") + " Z";
  const labels = axes.map((axis, i) => {
    const [lx, ly] = polarPoint(cx, cy, labelRadius, angles[i]);
    return {
      axis,
      x: lx,
      y: ly,
      anchor: Math.abs(Math.cos(angles[i])) < 0.25 ? "middle" : Math.cos(angles[i]) > 0 ? "start" : "end",
    };
  });

  function activateIndex(index) {
    if (selectedIndex === index) {
      setSelectedIndex(null);
      setUrlAxis(null);
      return;
    }
    setSelectedIndex(index);
    setUrlAxis(axes[index]?.key || null);
  }

  return (
    <div className="protocol-radar-wrap" ref={wrapRef}>
      <svg
        className="protocol-radar"
        viewBox={`0 0 ${viewWidth} ${viewHeight}`}
        style={viewWidth === viewHeight ? undefined : { aspectRatio: `${viewWidth} / ${viewHeight}` }}
        role="img"
        aria-label="Protocol radar"
      >
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

        {labels.map(({ axis, x, y, anchor }, i) => {
          const selected = i === safeSelectedIndex;
          return (
            <g
              key={`label-${axis.key || i}`}
              className={`radar-label-group${selected ? " selected" : ""}`}
              tabIndex={0}
              role="button"
              aria-pressed={selected}
              aria-label={`${axis.label}: ${axis.display || ""}. ${axis.tooltip?.description || ""}`}
              onClick={() => activateIndex(i)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  activateIndex(i);
                }
              }}
            >
              <circle className="radar-label-hit" cx={x} cy={y + 6} r={26} />
              {selected ? <circle className="radar-label-selected-ring" cx={x} cy={y + 6} r={15} /> : null}
              <text className="radar-label" x={x} y={y} textAnchor={anchor} dominantBaseline="middle">
                {axis.label}
              </text>
              {axis.display != null ? (
                <text
                  className="radar-value"
                  x={x}
                  y={y + 12}
                  textAnchor={anchor}
                  dominantBaseline="middle"
                >
                  {axis.display}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>

      {selectedDetail ? (
        <div className="radar-detail-panel">
          <div className="radar-detail-top">
            <span className="radar-detail-title">{selectedAxis.label}</span>
            {selectedAxis.display != null ? (
              <span className="radar-detail-score">{selectedAxis.display}</span>
            ) : null}
          </div>
          <div className="radar-detail-desc">{selectedDetail.description}</div>
          <div className="radar-detail-row radar-detail-row--good">
            <span className="radar-detail-mark">+</span>
            <span>{selectedDetail.positive}</span>
          </div>
          <div className="radar-detail-row radar-detail-row--bad">
            <span className="radar-detail-mark">-</span>
            <span>{selectedDetail.negative}</span>
          </div>

          <div className="radar-negative-examples">
            <div className="radar-negative-examples-hdr">
              <span>Negative examples</span>
              <span>{negativeExamples.length}</span>
            </div>
            {negativeExamples.length ? (
              <div className="radar-negative-list" role="list">
                {negativeExamples.map((example, i) => (
                  <button
                    type="button"
                    className="radar-negative-item"
                    role="listitem"
                    key={`${example.title}-${example.meta}-${i}`}
                    onClick={() => onExampleClick && onExampleClick(example)}
                    disabled={!onExampleClick || !example.contractAddress}
                  >
                    <div className="radar-negative-item-title">{example.title}</div>
                    <div className="radar-negative-item-detail">{example.detail}</div>
                    {example.meta ? <div className="radar-negative-item-meta">{example.meta}</div> : null}
                  </button>
                ))}
              </div>
            ) : (
              <div className="radar-negative-empty">No concrete negative examples in the current data.</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
