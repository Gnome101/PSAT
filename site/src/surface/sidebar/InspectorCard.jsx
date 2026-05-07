import { formatDelay, shortAddr } from "../format.js";
import { LANE_META, TYPE_META } from "../meta.js";

function principalDetail(principal) {
  const ownerCount = Array.isArray(principal.details?.owners) ? principal.details.owners.length : 0;
  const threshold = Number(principal.details?.threshold);
  const delay = formatDelay(principal.details?.delay);

  if (principal.resolvedType === "safe" && ownerCount) {
    return Number.isFinite(threshold) && threshold > 0 ? `${threshold}-of-${ownerCount} safe` : `${ownerCount} safe signers`;
  }
  if (principal.resolvedType === "timelock" && delay) {
    return `${delay} delay`;
  }
  if (principal.resolvedType === "eoa") {
    return "Externally owned account";
  }
  if (principal.resolvedType === "contract") {
    return "Contract-controlled principal";
  }
  if (principal.resolvedType === "proxy_admin") {
    return "Proxy admin principal";
  }
  return "Controller path";
}

export function InspectorCard({ selected, onNavigate }) {
  if (!selected) {
    return null;
  }

  return (
    <aside className="ps-inspector">
      <div className="ps-inspector-eyebrow">Guard Inspector</div>
      <h3>{selected.name}</h3>
      <div className="ps-inspector-subtitle">
        <span>{selected.contractName}</span>
        <span>{shortAddr(selected.contractAddress)}</span>
      </div>

      <div className="ps-inspector-badges">
        <span className="ps-badge" style={{ "--badge-accent": LANE_META[selected.lane].tone }}>{LANE_META[selected.lane].label}</span>
        <span className="ps-badge" style={{ "--badge-accent": selected.guard.accent }}>{selected.guard.label}</span>
        {selected.effectLabels.map((label) => (
          <span key={label} className="ps-badge" style={{ "--badge-accent": "#475569" }}>{label}</span>
        ))}
      </div>

      <div className="ps-inspector-block">
        <div className="ps-inspector-label">Signature</div>
        <code className="ps-inspector-code">{selected.signature}</code>
      </div>

      <div className="ps-inspector-block">
        <div className="ps-inspector-label">Action</div>
        <p className="ps-inspector-body">{selected.action || "Permissioned path"}</p>
      </div>

      <div className="ps-inspector-block">
        <div className="ps-inspector-label">
          Direct Callers
          <span className="ps-inspector-sublabel">
            msg.sender set from function-level permissions
          </span>
        </div>
        {selected.principals.length ? (
          <div className="ps-principal-list">
            {selected.principals.map((principal) => {
              const type = TYPE_META[principal.resolvedType] || TYPE_META.unknown;
              return (
                <div
                  key={principal.address}
                  className="ps-principal-card ps-principal-clickable"
                  onClick={() => onNavigate && onNavigate({ type: principal.resolvedType, address: principal.address, label: principalDetail(principal), details: principal.details })}
                >
                  <div className="ps-principal-top">
                    <span className="ps-principal-type" style={{ "--principal-accent": type.accent }}>{type.label}</span>
                    <span className="ps-principal-address">{shortAddr(principal.address)}</span>
                    <span className="ps-principal-goto">→</span>
                  </div>
                  <div className="ps-principal-meta">{principalDetail(principal)}</div>
                  <div className="ps-principal-origin">{principal.origins.join(" · ")}</div>
                </div>
              );
            })}
          </div>
        ) : (
          <p className="ps-inspector-empty">
            {selected.authorityPublic ? "This function is marked public in the authority state." : "No controlling principal was resolved for this path."}
          </p>
        )}
      </div>

      {(selected.indirectPrincipals || []).length > 0 && (
        <div className="ps-inspector-block">
          <div className="ps-inspector-label">
            Indirect Control Path
            <span className="ps-inspector-sublabel">
              governance context — not a direct call right
            </span>
          </div>
          <div className="ps-principal-list">
            {selected.indirectPrincipals.map((principal) => {
              const type = TYPE_META[principal.resolvedType] || TYPE_META.unknown;
              return (
                <div
                  key={principal.address}
                  className="ps-principal-card ps-principal-clickable ps-principal-indirect"
                  onClick={() => onNavigate && onNavigate({ type: principal.resolvedType, address: principal.address, label: principalDetail(principal), details: principal.details })}
                >
                  <div className="ps-principal-top">
                    <span className="ps-principal-type" style={{ "--principal-accent": type.accent }}>{type.label}</span>
                    <span className="ps-principal-address">{shortAddr(principal.address)}</span>
                    <span className="ps-principal-goto">→</span>
                  </div>
                  <div className="ps-principal-meta">{principalDetail(principal)}</div>
                  <div className="ps-principal-origin">
                    via {principal.path
                      .slice(0, -1)
                      .map((p) => shortAddr(p.address))
                      .join(" → ")}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </aside>
  );
}
