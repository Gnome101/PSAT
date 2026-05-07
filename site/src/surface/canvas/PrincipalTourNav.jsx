import { shortAddr } from "../format.js";
import { TYPE_META } from "../meta.js";

export function PrincipalTourNav({ tour, onGo, onBack }) {
  if (!tour || tour.principals.length < 2) return null;
  const current = tour.principals[tour.index];
  const type = TYPE_META[current.resolvedType] || TYPE_META.unknown;
  return (
    <div className="ps-tour-nav">
      <button
        className="ps-tour-back"
        onClick={onBack}
        title="Back to contract"
      >
        ← {tour.sourceFunction || "back"}
      </button>
      <div className="ps-tour-controls">
        <button
          onClick={() => onGo(tour.index > 0 ? tour.index - 1 : tour.principals.length - 1)}
          title="Previous principal"
        >
          ◀
        </button>
        <span className="ps-tour-label">
          <span className="ps-tour-type" style={{ color: type.accent }}>{type.label}</span>
          <span className="ps-tour-addr">{shortAddr(current.address)}</span>
          <span className="ps-tour-counter">{tour.index + 1} / {tour.principals.length}</span>
        </span>
        <button
          onClick={() => onGo(tour.index < tour.principals.length - 1 ? tour.index + 1 : 0)}
          title="Next principal"
        >
          ▶
        </button>
      </div>
    </div>
  );
}
