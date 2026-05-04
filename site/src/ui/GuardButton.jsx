import { GuardGlyph } from "./GuardGlyph.jsx";

export function GuardButton({ fnView, onSelect, onNavigate }) {
  const kind = fnView.guard.kind;
  const principals = fnView.guard.principals || [];
  const isNavigable = onNavigate && principals.length > 0
    && kind !== "unknown" && kind !== "open";

  const handleClick = (e) => {
    if (isNavigable) {
      e.stopPropagation();
      const sorted = [...principals].sort((a, b) => a.address.localeCompare(b.address));
      const first = sorted[0];
      onNavigate({
        type: first.resolvedType || kind,
        address: first.address,
        label: first.label,
        details: first.details,
        _allPrincipals: sorted.length > 1 ? sorted : null,
        _sourceFunction: fnView.name,
        _sourceContract: fnView.contractAddress,
      });
    } else {
      onSelect(fnView);
    }
  };

  return (
    <button
      type="button"
      className={`ps-guard-button${kind === "unknown" ? " ps-guard-icon-only" : ""}${isNavigable ? " ps-guard-navigable" : ""}`}
      style={{ "--guard-accent": fnView.guard.accent }}
      onClick={handleClick}
      title={isNavigable ? `Go to ${fnView.guard.label}` : kind === "unknown" ? "Unresolved guard" : `Inspect guard details for ${fnView.name}`}
    >
      <span className="ps-guard-icon">
        <GuardGlyph kind={kind} accent={fnView.guard.accent} title={fnView.guard.label} />
      </span>
      {kind !== "unknown" && (
        <span className="ps-guard-copy">
          <span className="ps-guard-label">{fnView.guard.label}</span>
          <span className="ps-guard-meta">{fnView.guard.sublabel}</span>
        </span>
      )}
    </button>
  );
}

export default GuardButton;
