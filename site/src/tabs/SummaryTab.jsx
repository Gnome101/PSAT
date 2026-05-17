import { StatCard } from "../ui/StatCard.jsx";
import { displayName } from "../displayName.js";

function asArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function readableBridgeLabel(value) {
  return String(value || "").replace(/_/g, " ");
}

function bridgeFunctionNames(items) {
  return asArray(items)
    .map((item) => item?.function || item?.signature || item?.name)
    .filter(Boolean);
}

function BridgeChipList({ label, values, fallback }) {
  const items = asArray(values);
  return (
    <div className="bridge-context-section">
      <div className="bridge-context-label">{label}</div>
      <div className="chips">
        {items.length
          ? items.map((item) => (
            <span className="chip alt" key={item}>{readableBridgeLabel(item)}</span>
          ))
          : <span className="chip alt">{fallback}</span>}
      </div>
    </div>
  );
}

function BridgeFunctionList({ label, values }) {
  const names = asArray(values);
  return (
    <div className="bridge-context-section">
      <div className="bridge-context-label">{label}</div>
      {names.length ? (
        <div className="bridge-function-list">
          {names.map((name) => (
            <span className="bridge-function-name" key={name}>{name}</span>
          ))}
        </div>
      ) : (
        <span className="muted">None detected</span>
      )}
    </div>
  );
}

function BridgeContextCard({ bridgeContext }) {
  if (bridgeContext?.status === "resolved" && bridgeContext?.routes?.length) {
    const protocols = asArray(bridgeContext.protocols?.length ? bridgeContext.protocols : [bridgeContext.protocol || "Bridge"]);
    const routes = asArray(bridgeContext.routes).map((route) => (
      `${route.chain_display_name || route.chain || route.network || route.eid || route.domain} -> ${route.peer || route.peer_address || route.route_type || "configured"}`
    ));
    const policies = asArray(bridgeContext.policies).map((policy) => (
      policy.address ? `${policy.label}: ${policy.address}` : `${policy.label}: ${policy.value}`
    ));
    const limits = asArray(bridgeContext.limits).map((limit) => `${limit.label}: ${limit.value}`);
    return (
      <div className="card">
        <div className="card-header-row">
          <h3>Resolved Bridge Context</h3>
          <span className="chip alt">{bridgeContext.routes.length} active routes</span>
        </div>
        <BridgeChipList label="Protocols" values={protocols} fallback="Bridge" />
        <BridgeFunctionList label="Routes" values={routes} />
        {policies.length ? <BridgeFunctionList label="Policies" values={policies} /> : null}
        {limits.length ? <BridgeFunctionList label="Limits" values={limits} /> : null}
      </div>
    );
  }
  if (!bridgeContext?.is_bridge) return null;
  const upgrade = bridgeContext.upgrade_context || {};
  const protocols = asArray(bridgeContext.protocols);
  const movementModels = asArray(bridgeContext.movement_models);
  const securityModels = asArray(bridgeContext.security_models);
  const sendFunctions = bridgeFunctionNames(bridgeContext.send_functions);
  const receiveFunctions = bridgeFunctionNames(bridgeContext.receive_functions);
  const configFunctions = bridgeFunctionNames(bridgeContext.config_functions);
  const securityConfigFunctions = bridgeFunctionNames(bridgeContext.security_config_functions);
  const upgradeFunctions = [
    ...bridgeFunctionNames(upgrade.upgrade_functions),
    ...asArray(upgrade.admin_paths),
  ];

  return (
    <div className="card">
      <div className="card-header-row">
        <h3>Bridge Context</h3>
        <span className={`chip ${upgrade.can_change_bridge_logic ? "warn" : "alt"}`}>
          {upgrade.can_change_bridge_logic ? "Upgradeable bridge logic" : "Static bridge logic"}
        </span>
      </div>
      <BridgeChipList label="Protocols" values={protocols} fallback="Bridge" />
      <BridgeChipList label="Movement" values={movementModels} fallback="Unknown movement model" />
      <BridgeChipList label="Security" values={securityModels} fallback="Unknown security model" />
      <BridgeFunctionList label="Sends" values={sendFunctions} />
      <BridgeFunctionList label="Receives" values={receiveFunctions} />
      <BridgeFunctionList label="Route config" values={configFunctions} />
      <BridgeFunctionList label="Security config" values={securityConfigFunctions} />
      <BridgeFunctionList label="Upgrade path" values={upgradeFunctions} />
      <BridgeFunctionList label="Implementation slots" values={asArray(upgrade.implementation_slots)} />
    </div>
  );
}

export default function SummaryTab({ detail }) {
  const summary = detail?.contract_analysis?.summary || detail?.summary || {};
  const subject = detail?.contract_analysis?.subject || {};
  const standards = summary.standards || [];
  const bridgeContext = detail?.bridge_context || detail?.contract_analysis?.bridge_context;
  return (
    <div className="stack">
      <div className="summary-grid">
        <StatCard label="Contract" value={displayName(detail) || subject.name || "Unknown"} />
        <StatCard label="Control Model" value={summary.control_model || "unknown"} />
        <StatCard label="Risk" value={summary.static_risk_level || "unknown"} />
        <StatCard label="Standards" value={standards.length || 0} />
      </div>
      <div className="card">
        <h3>Summary</h3>
        <div className="kv-grid">
          <div className="kv-row">
            <span className="key">Address</span>
            <span className="mono">{detail?.address || subject.address || "Unknown"}</span>
          </div>
          <div className="kv-row">
            <span className="key">Upgradeable</span>
            <span>{String(Boolean(summary.is_upgradeable))}</span>
          </div>
          <div className="kv-row">
            <span className="key">Pausable</span>
            <span>{String(Boolean(summary.is_pausable))}</span>
          </div>
          <div className="kv-row">
            <span className="key">Timelock</span>
            <span>{String(Boolean(summary.has_timelock))}</span>
          </div>
          <div className="kv-row">
            <span className="key">Standards</span>
            <span>{standards.join(", ") || "None"}</span>
          </div>
        </div>
      </div>
      <BridgeContextCard bridgeContext={bridgeContext} />
    </div>
  );
}
