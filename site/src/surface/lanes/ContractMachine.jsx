import { useEffect, useMemo, useState } from "react";

import { formatUsd, shortAddr } from "../format.js";
import { machineFunctions, tabForLane } from "../lane.js";
import { LANE_META, MACHINE_TABS, ROLE_META } from "../meta.js";
import { BalanceTable } from "./BalanceTable.jsx";
import { LaneColumn } from "./LaneColumn.jsx";
import { OpsLane } from "./OpsLane.jsx";

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function bridgeFunctionName(fnView) {
  return fnView.signature || fnView.name;
}

function bridgeStaticFunctionNames(values) {
  return unique(
    (values || []).map((value) => {
      if (typeof value === "string") return value;
      return value?.function || value?.signature || value?.name;
    }),
  );
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

function bridgeProtocols(machine, runtimeContext, staticContext) {
  const protocolStandards = new Set(["LayerZero", "CCIP", "Wormhole", "Hyperlane", "Axelar", "Connext", "OP Stack"]);
  return unique([
    ...(runtimeContext?.protocols || []),
    runtimeContext?.protocol,
    ...(staticContext?.protocols || []),
    ...(machine.standards || []).filter((standard) => protocolStandards.has(standard)),
  ]);
}

function dvnSummary(config) {
  if (!config) return "DVNs unresolved";
  const required = config.required_dvns || [];
  const optional = config.optional_dvns || [];
  const confirmations = config.confirmations != null ? `${config.confirmations} conf` : "confirmations unresolved";
  const requiredText = required.length ? `${required.length} required DVN${required.length === 1 ? "" : "s"}` : "no required DVNs";
  const optionalText = optional.length ? `, ${optional.length} optional` : "";
  return `${confirmations}, ${requiredText}${optionalText}`;
}

function RuntimeRouteList({ routes }) {
  if (!routes?.length) return null;
  return (
    <div className="ps-bridge-section">
      <div className="ps-bridge-section-label">Resolved routes</div>
      <div className="ps-bridge-function-list">
        {routes.map((route) => {
          const maxSize = route.executor?.max_message_size;
          const peer = route.peer || route.peer_address || route.route_type || "route configured";
          const peerStatus = route.peer_analysis?.status;
          return (
            <span className="ps-bridge-function" key={`${route.eid || route.domain || route.chain}:${peer}`}>
              {route.chain_display_name || route.chain || route.network || route.eid || route.domain}{" -> "}{peer}
              {peerStatus ? <><br />peer analysis: {readableBridgeLabel(peerStatus)}</> : null}
              <br />
              send: {dvnSummary(route.send_uln)}
              <br />
              receive: {dvnSummary(route.receive_uln)}
              {route.ism ? <><br />ISM: {route.ism}</> : null}
              {route.l2_chain_id ? <><br />L2 chain: {route.l2_chain_id}</> : null}
              {maxSize != null ? <><br />max message: {maxSize}</> : null}
            </span>
          );
        })}
      </div>
    </div>
  );
}

function BridgeFunctionBlock({ label, values }) {
  if (!values.length) return null;
  return (
    <div className="ps-bridge-section">
      <div className="ps-bridge-section-label">{label}</div>
      <div className="ps-bridge-function-list">
        {values.map((value) => (
          <span className="ps-bridge-function" key={value}>{value}</span>
        ))}
      </div>
    </div>
  );
}

function BridgeContextPanel({ machine }) {
  const bridgeContext = machine.bridge_context;
  const staticContext = machine.bridge_static_context;
  const hasRuntime = Boolean(bridgeContext?.status);
  const hasResolvedRuntime = bridgeContext?.status === "resolved" && bridgeContext.routes?.length > 0;
  const hasStaticContext = Boolean(staticContext?.is_bridge || staticContext?.protocols?.length);
  if (!hasRuntime && !hasStaticContext && machine.role !== "bridge") return null;

  const protocols = bridgeProtocols(machine, bridgeContext, staticContext);
  const receives = bridgeFunctions(machine, (effects) => effects.has("bridge_receive"));
  const routeConfig = bridgeFunctions(machine, (effects) =>
    effects.has("bridge_config_update") && !effects.has("bridge_security_config")
  );
  const securityConfig = bridgeFunctions(machine, (effects) => effects.has("bridge_security_config"));
  const sendFunctions = bridgeStaticFunctionNames(staticContext?.send_functions);
  const receiveFunctions = unique([...bridgeStaticFunctionNames(staticContext?.receive_functions), ...receives]);
  const configFunctions = unique([
    ...bridgeStaticFunctionNames(staticContext?.config_functions),
    ...(bridgeContext?.config_functions || []),
    ...routeConfig,
  ]);
  const securityConfigFunctions = unique([
    ...bridgeStaticFunctionNames(staticContext?.security_config_functions),
    ...securityConfig,
  ]);
  const policies = (bridgeContext?.policies || []).map((policy) => (
    policy.address ? `${policy.label}: ${policy.address}` : policy.label
  ));
  const assets = (bridgeContext?.assets || []).map((asset) => `${asset.label}: ${asset.address}`);
  const relationships = (bridgeContext?.relationships || []).map((item) => `${item.label}: ${item.address}`);
  const endpoint = bridgeContext?.endpoint?.address
    ? [`endpoint: ${bridgeContext.endpoint.address}${bridgeContext.endpoint.local_eid ? ` (local eid ${bridgeContext.endpoint.local_eid})` : ""}`]
    : [];
  const mailbox = bridgeContext?.mailbox?.address
    ? [`mailbox: ${bridgeContext.mailbox.address}${bridgeContext.mailbox.local_domain != null ? ` (domain ${bridgeContext.mailbox.local_domain})` : ""}`]
    : [];
  const libraries = unique((bridgeContext?.routes || []).flatMap((route) => [
    route.send_library ? `${route.chain} send lib: ${route.send_library}` : null,
    route.receive_library ? `${route.chain} receive lib: ${route.receive_library}` : null,
  ]));
  const runtimeStatus = hasResolvedRuntime
    ? `${bridgeContext.routes.length} active routes`
    : hasRuntime
      ? readableBridgeLabel(bridgeContext.status)
      : "static bridge";
  const runtimeReason = hasRuntime && !hasResolvedRuntime
    ? [bridgeContext.reason || "Runtime context was not resolved by the pipeline."]
    : [];

  return (
    <section className="ps-bridge-panel">
      <div className="ps-bridge-panel-top">
        <div className="ps-bridge-title">{hasResolvedRuntime ? "Resolved Bridge Context" : "Bridge Context"}</div>
        <span className="ps-bridge-status">{runtimeStatus}</span>
      </div>
      <div className="ps-bridge-chip-row">
        {(protocols.length ? protocols : ["Bridge"]).map((protocol) => (
          <span className="ps-bridge-chip" key={protocol}>{protocol}</span>
        ))}
        {bridgeContext?.source && <span className="ps-bridge-chip ps-bridge-chip-muted">{readableBridgeLabel(bridgeContext.source)}</span>}
      </div>
      <BridgeFunctionBlock label="Runtime status" values={runtimeReason} />
      <BridgeFunctionBlock label="Movement" values={staticContext?.movement_models || []} />
      <BridgeFunctionBlock label="Security" values={staticContext?.security_models || []} />
      <BridgeFunctionBlock label="Endpoint" values={endpoint} />
      <BridgeFunctionBlock label="Mailbox" values={mailbox} />
      <BridgeFunctionBlock label="Policies" values={policies} />
      <BridgeFunctionBlock label="Relationships" values={relationships} />
      <BridgeFunctionBlock label="Assets" values={assets} />
      <RuntimeRouteList routes={bridgeContext?.routes || []} />
      <BridgeFunctionBlock label="Sends" values={sendFunctions} />
      <BridgeFunctionBlock label="Receives" values={receiveFunctions} />
      <BridgeFunctionBlock label="Route config" values={configFunctions} />
      <BridgeFunctionBlock label="Security config" values={securityConfigFunctions} />
      <BridgeFunctionBlock label="Message libraries" values={libraries} />
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
  const staticBridgeContext = machine.bridge_static_context;
  const bridgeProtocolLabels = bridgeProtocols(machine, bridgeContext, staticBridgeContext);
  const activeBridge = bridgeContext?.status === "resolved" && bridgeContext?.routes?.length > 0;
  const bridgeCandidate = activeBridge || Boolean(staticBridgeContext?.is_bridge || staticBridgeContext?.protocols?.length);

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
          {bridgeCandidate && (
            <>
              {(bridgeProtocolLabels.length ? bridgeProtocolLabels : ["Bridge"]).map((protocol) => (
                <span className="ps-badge" style={{ "--badge-accent": "#9e8a6a" }} key={protocol}>{protocol}</span>
              ))}
              <span className="ps-badge" style={{ "--badge-accent": "#a08a70" }}>
                {activeBridge ? `${bridgeContext.routes.length} routes` : "static bridge"}
              </span>
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
