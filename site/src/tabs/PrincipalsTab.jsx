import { prettyFunctionName, shortenAddress } from "../graph.js";

export default function PrincipalsTab({ detail }) {
  const payload = detail?.principal_labels;
  if (!payload?.principals?.length) {
    return <p className="empty">No principal labels available.</p>;
  }
  return (
    <div className="card-grid">
      {payload.principals.map((principal) => (
        <article className="card" key={principal.address}>
          <div className="card-header-row">
            <h3>{principal.display_name || shortenAddress(principal.address)}</h3>
            <span className="chip alt">{principal.resolved_type}</span>
          </div>
          <div className="mono muted">{principal.address}</div>
          <div className="chips" style={{ marginTop: 12 }}>
            {(principal.labels || []).map((label) => (
              <span className="chip" key={label}>
                {label}
              </span>
            ))}
          </div>
          {principal.permissions?.length ? (
            <div className="subsection">
              <div className="subsection-title">Permissions</div>
              <div className="chips">
                {principal.permissions.map((permission, index) => (
                  <span className="chip" key={`${principal.address}-${index}`}>
                    {prettyFunctionName(permission.function)}
                    {permission.role != null ? ` · role ${permission.role}` : ""}
                  </span>
                ))}
              </div>
            </div>
          ) : null}
        </article>
      ))}
    </div>
  );
}
