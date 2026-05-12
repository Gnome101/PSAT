import ProtocolRadar from "../../ProtocolRadar.jsx";
import { computeProtocolScore } from "../../protocolScore.js";

// Detail tab's empty state — when nothing is selected, show the same
// composite-score block + radar that the company hero uses, so the
// sidebar isn't a blank "click something" prompt. Falls back to a quiet
// stub if companyData hasn't loaded yet.
export function DetailEmptyState({ companyName, companyData, coverageData, onExampleClick }) {
  if (!companyData) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Loading protocol overview…</div>
      </section>
    );
  }
  const { axes, composite, grade } = computeProtocolScore(
    companyData,
    coverageData,
  );
  return (
    <section className="ps-detail-empty">
      <div className="ps-detail-empty-hdr">{companyName}</div>
      <div className={`company-hero-score grade-${grade}`}>
        <span className="company-hero-score-value">{composite}</span>
        <span className="company-hero-score-unit">/ 100</span>
      </div>
      <div className="company-hero-score-label">Grade {grade.toUpperCase()}</div>
      <div className={`company-hero-grade-bar grade-${grade}`}>
        <div
          className="company-hero-grade-bar-fill"
          style={{ width: `${Math.max(4, composite)}%` }}
        />
      </div>
      <div className="ps-detail-empty-radar">
        <ProtocolRadar
          axes={axes}
          size={240}
          labelRadius={0.40}
          labelInsetX={64}
          labelInsetY={12}
          onExampleClick={onExampleClick}
        />
      </div>
      <div className="ps-detail-empty-hint">
        Click a contract or principal on the canvas for its detail.
      </div>
    </section>
  );
}
