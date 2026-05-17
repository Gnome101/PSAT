import { useEffect, useMemo, useState } from "react";

import { formatUsd, shortAddr } from "../format.js";
import { machineFunctions, tabForLane } from "../lane.js";
import { LANE_META, MACHINE_TABS, ROLE_META } from "../meta.js";
import { BalanceTable } from "./BalanceTable.jsx";
import { LaneColumn } from "./LaneColumn.jsx";
import { OpsLane } from "./OpsLane.jsx";

const BRIDGE_EFFECT_LABELS = new Set([
  "cross_chain_message",
  "bridge_transfer",
  "bridge_receive",
  "bridge_config_update",
  "bridge_security_config",
]);

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function bridgeFunctionName(fnView) {
  return fnView.signature || fnView.name;
}

function bridgeFunctions(machine, matcher) {
  return unique(
    machineFunctions(machine)
      .filter((fnView) => {
        const effects = new Set(fnView.effectLabels || []);
        return matcher(effects);
      })
      .map(bridgeFunctionName),
  );
}

function readableBridgeLabel(value) {
  return String(value || "").replace(/_/g, " ");
}

function BridgeFunctionBlock({ label, values }) {
  return (
    <div className="ps-bridge-section">
      <div className="ps-bridge-section-label">{label}</div>
      {values.length ? (
        <div className="ps-bridge-function-list">
          {values.map((value) => (
            <span className="ps-bridge-function" key={value}>{value}</span>
          ))}
        </div>
      ) : (
        <span className="ps-bridge-empty">None detected</span>
      )}
    </div>
  );
}

function BridgeContextPanel({ machine }) {
  const bridgeContext = machine.bridge_context;
  const allBridgeFunctions = bridgeFunctions(machine, (effects) =>
    [...BRIDGE_EFFECT_LABELS].some((label) => effects.has(label))
  );
  if (!bridgeContext && allBridgeFunctions.length === 0) return null;

  const protocols = bridgeContext?.protocols?.length ? bridgeContext.protocols : ["Bridge"];
  const movement = bridgeFunctions(machine, (effects) =>
    (effects.has("cross_chain_message") || effects.has("bridge_transfer")) && !effects.has("bridge_config_update")
  );
  const receives = bridgeFunctions(machine, (effects) => effects.has("bridge_receive"));
  const routeConfig = bridgeFunctions(machine, (effects) =>
    effects.has("bridge_config_update") && !effects.has("bridge_security_config")
  );
  const securityConfig = bridgeFunctions(machine, (effects) => effects.has("bridge_security_config"));
  const upgradePath = bridgeFunctions(machine, (effects) => effects.has("implementation_update"));
  const effectLabels = (bridgeContext?.effect_labels || []).filter(Boolean);

  return (
    <section className="ps-bridge-panel">
      <div className="ps-bridge-panel-top">
        <div className="ps-bridge-title">Bridge Context</div>
        {bridgeContext?.can_change_bridge_logic && (
          <span className="ps-bridge-status">upgrade path changes bridge</span>
        )}
      </div>
      <div className="ps-bridge-chip-row">
        {protocols.map((protocol) => (
          <span className="ps-bridge-chip" key={protocol}>{protocol}</span>
        ))}
        {effectLabels.map((label) => (
          <span className="ps-bridge-chip ps-bridge-chip-muted" key={label}>
            {readableBridgeLabel(label)}
          </span>
        ))}
      </div>
      <BridgeFunctionBlock label="Token / message movement" values={movement} />
      <BridgeFunctionBlock label="Receives" values={receives} />
      <BridgeFunctionBlock label="Route config" values={routeConfig} />
      <BridgeFunctionBlock label="Security config" values={securityConfig} />
      <BridgeFunctionBlock label="Upgrade path" values={upgradePath} />
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
  const highlightedFunction = useMemo(
    () => machineFunctions(machine).find((fnView) => fnView.key === highlightedFunctionKey) || null,
    [machine, highlightedFunctionKey],
  );
  const bridgeContext = machine.bridge_context;
  const bridgeProtocols = bridgeContext?.protocols || [];

  useEffect(() => {
    if (highlightedFunction) setActiveTab(tabForLane(highlightedFunction.lane));
  }, [highlightedFunction]);

  const tabCounts = {
    control: machine.lanes.top.length + machine.lanes.ops.length,
    inflows: machine.lanes.left.length,
    outflows: machine.lanes.right.length,
    balances: machine.balances?.length || 0,
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
          {bridgeContext && (
            <>
              {(bridgeProtocols.length ? bridgeProtocols : ["Bridge"]).map((protocol) => (
                <span className="ps-badge" style={{ "--badge-accent": "#9e8a6a" }} key={protocol}>{protocol}</span>
              ))}
              {bridgeContext.has_security_config && <span className="ps-badge" style={{ "--badge-accent": "#a08a70" }}>bridge security</span>}
              {bridgeContext.can_change_bridge_logic && <span className="ps-badge" style={{ "--badge-accent": "#d4a017" }}>upgrade path changes bridge</span>}
            </>
          )}
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

      <BridgeContextPanel machine={machine} />

      <div className="ps-machine-tabs">
        {MACHINE_TABS.map((t) => (
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
    </article>
  );
}
