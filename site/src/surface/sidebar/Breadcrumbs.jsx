import { shortAddr } from "../format.js";

export function Breadcrumbs({ items, onNavigate }) {
  if (!items.length) return null;
  return (
    <div className="ps-breadcrumbs">
      {items.map((item, i) => (
        <span key={i} className="ps-breadcrumb" onClick={() => onNavigate(item, i)}>
          <span className="ps-breadcrumb-type">{item.type}</span>
          <span className="ps-breadcrumb-label">{item.label || shortAddr(item.address)}</span>
          {i < items.length - 1 && <span className="ps-breadcrumb-sep">›</span>}
        </span>
      ))}
    </div>
  );
}
