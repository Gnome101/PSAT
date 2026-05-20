import { useEffect, useMemo, useState } from "react";

import { formatUsd, shortAddr } from "../format.js";
import { machineFunctions, tabForLane } from "../lane.js";
import { LANE_META, MACHINE_TABS, ROLE_META } from "../meta.js";
import { BalanceTable } from "./BalanceTable.jsx";
import { LaneColumn } from "./LaneColumn.jsx";
import { OpsLane } from "./OpsLane.jsx";

const BRIDGE_STATUS_LABELS = {
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
  return BRIDGE_STATUS_LABELS[status] || status || "not queued";
}

function bridgeRouteCount(bridge) {
  const routeCount = Number(bridge?.route_count);
  if (Number.isFinite(routeCount) && routeCount > 0) return routeCount;
  return (bridge?.routes || []).filter(Boolean).length;
}

function bridgeOverflowCount(bridge) {
  const overflow = Number(bridge?.route_overflow);
  return Number.isFinite(overflow) && overflow > 0 ? overflow : 0;
}

function BridgeOverview({ machine }) {
  const bridge = machine.bridge_summary;
  const routes = (bridge?.routes || []).filter(Boolean);
  const overflow = bridgeOverflowCount(bridge);
  const routeChains = routes.map((route) => chainLabel(route.chain)).filter(Boolean);
  const localChain = chainLabel(machine.chain);
  const routeSummary = routeChains.length
    ? `${localChain ? `${localChain} -> ` : ""}${routeChains.join(", ")}${overflow ? `, +${overflow} more` : ""}`
    : bridge?.status || "unresolved";

  return (
    <section className="ps-machine-bridge">
      <div className="ps-machine-bridge-summary">
        <div className="ps-machine-bridge-heading">
          <div>
            <div className="ps-machine-bridge-title">{bridge?.protocol || "Bridge"} bridge</div>
            <div className="ps-machine-bridge-route-summary">{routeSummary}</div>
          </div>
          <span className="ps-machine-bridge-status">{bridge?.status || "unresolved"}</span>
        </div>
        <div className="ps-machine-bridge-facts">
          <div>
            <span>Peers</span>
            <strong>{bridge?.peers || "none analyzed"}</strong>
          </div>
          <div>
            <span>Config control</span>
            <strong>{bridge?.config_control || "Unknown"}</strong>
          </div>
        </div>
      </div>
      {routes.length ? (
        <div className="ps-machine-bridge-routes">
          {routes.map((route, index) => (
            <div className="ps-machine-bridge-route" key={`${route.chain || "route"}-${index}`}>
              <div className="ps-machine-bridge-route-head">
                <span>{chainLabel(route.chain) || "Remote"}</span>
                <span>{peerLabel(route)}</span>
              </div>
              <div className="ps-machine-bridge-route-meta">
                <span>Peer: {peerStatusLabel(route.peer_status)}</span>
                {route.security ? <span>{route.security}</span> : null}
              </div>
            </div>
          ))}
          {overflow ? <div className="ps-machine-bridge-more">+{overflow} more routes</div> : null}
        </div>
      ) : null}
    </section>
  );
}

export function ContractMachine({
  machine,
  onSelectGuard,
  onNavigate,
  companyName,
  highlightedFunctionKey,
  highlightedContract = false,
  onOpenDependencyGraph,
}) {
  const [activeTab, setActiveTab] = useState("control");
  const usdLabel = formatUsd(machine.total_usd);
  const bridge = machine.bridge_summary;
  const machineTabs = bridge
    ? [...MACHINE_TABS, { key: "bridges", label: "Bridges" }]
    : MACHINE_TABS;
  const highlightedFunction = useMemo(
    () => machineFunctions(machine).find((fnView) => fnView.key === highlightedFunctionKey) || null,
    [machine, highlightedFunctionKey],
  );

  useEffect(() => {
    if (highlightedFunction) setActiveTab(tabForLane(highlightedFunction.lane));
  }, [highlightedFunction]);

  useEffect(() => {
    if (activeTab === "bridges" && !bridge) setActiveTab("control");
  }, [activeTab, bridge]);

  const tabCounts = {
    control: machine.lanes.top.length + machine.lanes.ops.length,
    inflows: machine.lanes.left.length,
    outflows: machine.lanes.right.length,
    balances: machine.balances?.length || 0,
    bridges: bridgeRouteCount(bridge),
  };

  return (
    <article
      className={`ps-machine${highlightedContract ? " ps-machine-score-highlight" : ""}`}
      style={machine.total_usd ? { borderLeft: "2px solid #f59e0b33" } : undefined}
    >
      <header className="ps-machine-header">
        <div className="ps-machine-header-row">
          <div className="ps-machine-title-wrap">
            <div className="ps-machine-name">{machine.name || shortAddr(machine.address)}</div>
            <div className="ps-machine-address">{shortAddr(machine.address)}</div>
          </div>
        </div>
        <div className="ps-machine-badges">
          <span className="ps-badge" style={{ "--badge-accent": (ROLE_META[machine.role] || ROLE_META.utility).color }}>{(ROLE_META[machine.role] || ROLE_META.utility).label.replace(/s$/, "")}</span>
          {/* Deposit-destination call-out for value_handler contracts that
              actually hold funds. The role badge above is jargon — this one
              answers the user-facing question "where does my money go?"
              directly. Gated on total_usd>0 so we don't mislabel zero-TVL
              receivers (e.g. a router that pulls then forwards). */}
          {machine.role === "value_handler" && Number(machine.total_usd) > 0 ? (
            <span className="ps-badge" style={{ "--badge-accent": "#22c55e" }}>Deposit destination</span>
          ) : null}
          {machine.is_proxy ? <span className="ps-badge" style={{ "--badge-accent": "#9a8a6e" }}>{machine.proxy_type || "proxy"}</span> : null}
          {machine.upgrade_count != null ? <span className="ps-badge" style={{ "--badge-accent": "#8b92a8" }}>{machine.upgrade_count} upgrades</span> : null}
          <span className="ps-badge" style={{ "--badge-accent": "#6b7590" }}>{machine.totalFunctions} functions</span>
          {usdLabel && <span className="ps-badge" style={{ "--badge-accent": "#f59e0b" }}>{usdLabel}</span>}
        </div>
        {onOpenDependencyGraph && (
          <div className="ps-machine-actions">
            <button
              type="button"
              className="ps-machine-header-action"
              onClick={() => onOpenDependencyGraph(machine)}
            >
              Dependency graph
            </button>
          </div>
        )}
      </header>

      <div className="ps-machine-tabs">
        {machineTabs.map((t) => (
          <button
            key={t.key}
            className={`ps-machine-tab${activeTab === t.key ? " active" : ""}`}
            onClick={() => setActiveTab(t.key)}
          >
            {t.label}
            {tabCounts[t.key] > 0 && <span className="ps-machine-tab-count">{tabCounts[t.key]}</span>}
          </button>
        ))}
      </div>

      {activeTab === "control" && (
        <>
          <LaneColumn
            title={LANE_META.top.label}
            laneKey="top"
            items={machine.lanes.top}
            onSelect={onSelectGuard}
            onNavigate={onNavigate}
            highlightedFunctionKey={highlightedFunctionKey}
          />
          {machine.lanes.ops.length > 0 && (
            <OpsLane
              items={machine.lanes.ops}
              onSelect={onSelectGuard}
              onNavigate={onNavigate}
              highlightedFunctionKey={highlightedFunctionKey}
            />
          )}
        </>
      )}
      {activeTab === "inflows" && (
        <LaneColumn
          title={LANE_META.left.label}
          laneKey="left"
          items={machine.lanes.left}
          onSelect={onSelectGuard}
          onNavigate={onNavigate}
          highlightedFunctionKey={highlightedFunctionKey}
        />
      )}
      {activeTab === "outflows" && (
        <LaneColumn
          title={LANE_META.right.label}
          laneKey="right"
          items={machine.lanes.right}
          onSelect={onSelectGuard}
          onNavigate={onNavigate}
          highlightedFunctionKey={highlightedFunctionKey}
        />
      )}
      {activeTab === "balances" && (
        <BalanceTable machine={machine} />
      )}
      {activeTab === "bridges" && bridge && (
        <BridgeOverview machine={machine} />
      )}
    </article>
  );
}
