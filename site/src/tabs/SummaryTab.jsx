import { StatCard } from "../ui/StatCard.jsx";
import { displayName } from "../displayName.js";
import { shortAddr } from "../surface/format.js";

const STATUS_LABELS = {
  analyzed: "analyzed",
  queued: "queued",
  processing: "processing",
  missing_rpc: "missing RPC",
  unsupported_chain: "unsupported",
  non_evm_peer: "non-EVM",
  missing_chain: "missing chain",
  not_queued: "not queued",
  not_applicable: "n/a",
};

function chainLabel(chain) {
  const value = String(chain || "").trim();
  if (!value) return "";
  const known = {
    ethereum: "Ethereum",
    mainnet: "Ethereum",
    base: "Base",
    arbitrum: "Arbitrum",
    optimism: "Optimism",
    polygon: "Polygon",
    avalanche: "Avalanche",
    bsc: "BSC",
    linea: "Linea",
    scroll: "Scroll",
    blast: "Blast",
  };
  return known[value.toLowerCase()] || value;
}

function peerLabel(route) {
  return route?.peer || shortAddr(route?.peer_address) || "unknown";
}

function peerStatusLabel(status) {
  return STATUS_LABELS[status] || status || "not queued";
}

export default function SummaryTab({ detail }) {
  const summary = detail?.contract_analysis?.summary || detail?.summary || {};
  const subject = detail?.contract_analysis?.subject || {};
  const standards = summary.standards || [];
  const bridge = detail?.bridge_summary;
  const bridgeRoutes = (bridge?.routes || []).filter(Boolean);
  const bridgeOverflow = Number(bridge?.route_overflow || 0);
  const routeChains = bridgeRoutes.map((route) => chainLabel(route.chain)).filter(Boolean);
  const localChain = chainLabel(detail?.chain || subject.chain);
  const routeSummary = routeChains.length
    ? `${localChain ? `${localChain} -> ` : ""}${routeChains.join(", ")}${bridgeOverflow ? `, +${bridgeOverflow} more` : ""}`
    : bridge?.status || "unresolved";
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
      {bridge ? (
        <div className="card bridge-card">
          <div className="bridge-card-header">
            <h3>{bridge.protocol || "Bridge"} bridge</h3>
            <span className="bridge-status-chip">{bridge.status || "unresolved"}</span>
          </div>
          <div className="kv-grid bridge-card-main">
            <div className="kv-row">
              <span className="key">Routes</span>
              <span>{routeSummary}</span>
            </div>
            <div className="kv-row">
              <span className="key">Peers</span>
              <span>{bridge.peers || "none analyzed"}</span>
            </div>
            <div className="kv-row">
              <span className="key">Config control</span>
              <span>{bridge.config_control || "Unknown"}</span>
            </div>
          </div>
          {bridgeRoutes.length ? (
            <div className="bridge-route-list">
              {bridgeRoutes.map((route, index) => (
                <div className="bridge-route-row" key={`${route.chain || "route"}-${index}`}>
                  <div className="bridge-route-line">
                    <span>{chainLabel(route.chain) || "Remote"} -&gt; {peerLabel(route)}</span>
                    <span className="bridge-peer-status">Peer: {peerStatusLabel(route.peer_status)}</span>
                  </div>
                  {route.security ? <div className="bridge-route-security">Security: {route.security}</div> : null}
                </div>
              ))}
              {bridgeOverflow ? <div className="bridge-route-more">+{bridgeOverflow} more</div> : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
