import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import ELK from "elkjs/lib/elk.bundled.js";
import {
  ReactFlow,
  Background,
  Controls,
  Panel,
  useNodesState,
  useEdgesState,
  useReactFlow,
  ReactFlowProvider,
  Handle,
  Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { GuardGlyph } from "./ui/GuardGlyph.jsx";
import { GuardButton } from "./ui/GuardButton.jsx";
import { UpgradesPanel } from "./surface/inspector/UpgradesPanel.jsx";
import { AgentPanel } from "./surface/inspector/AgentPanel.jsx";
import ProtocolRadar from "./ProtocolRadar.jsx";
import DependencyGraphTab from "./DependencyGraphTab.jsx";
import { computeProtocolScore } from "./protocolScore.js";

import { bytecodeVerifiedAudits, isBytecodeVerifiedAudit } from "./auditCoverage.js";
import { blockExplorerAddressUrl } from "./blockExplorer.js";
import { getCoverage, getTimeline } from "./api/audits.js";
import { api } from "./api/client.js";
import { listAddressLabels } from "./api/addressLabels.js";
import AddressLabelInline from "./AddressLabelInline.jsx";
import {
  AUDIT_STATUS_META,
  DRIFT_FALSE_META,
  DRIFT_TRUE_META,
  EQUIVALENCE_META,
  formatAuditDate,
  MATCH_TYPE_META,
  MetaBadge,
  proofKindTitle,
  PROOF_KIND_META,
  SEVERITY_META,
  STATUS_LABELS,
} from "./auditUi.jsx";

const CONTROL_EFFECTS = new Set([
  "implementation_update",
  "delegatecall_execution",
  "ownership_transfer",
  "role_management",
  "authority_update",
  "hook_update",
  "pause_toggle",
  "timelock_operation",
  "contract_deployment",
  "selfdestruct_capability",
]);

const INPUT_EFFECTS = new Set(["asset_pull", "mint"]);
const OUTPUT_EFFECTS = new Set(["asset_send", "burn"]);

const INPUT_HINTS = ["deposit", "mint", "stake", "supply", "repay", "transferin", "bridgein", "join", "wrap"];
const OUTPUT_HINTS = ["withdraw", "redeem", "transfer", "send", "sweep", "claim", "borrow", "unstake", "burn"];
const CONTROL_HINTS = ["upgrade", "owner", "admin", "pause", "role", "authority", "hook", "timelock", "config"];

const LANE_META = {
  top: { label: "Control", tone: "#8b92a8", chip: "CTRL" },
  ops: { label: "Operations", tone: "#6b7590", chip: "OPS" },
  left: { label: "Inflows", tone: "#6a9e94", chip: "IN" },
  right: { label: "Outflows", tone: "#9a8a6e", chip: "OUT" },
};

const TYPE_META = {
  safe: { label: "SAFE", accent: "#6a9e94" },
  timelock: { label: "TL", accent: "#9a8a6e" },
  eoa: { label: "EOA", accent: "#a09870" },
  contract: { label: "CON", accent: "#7a8098" },
  proxy_admin: { label: "ADM", accent: "#8880a0" },
  unknown: { label: "UNK", accent: "#94a3b8" },
  open: { label: "OPEN", accent: "#64748b" },
  many: { label: "MULTI", accent: "#8a80a0" },
};

const MONITOR_FLAGS = [
  { key: "watch_upgrades", label: "Upgrades" },
  { key: "watch_ownership", label: "Ownership" },
  { key: "watch_pause", label: "Pause" },
  { key: "watch_roles", label: "Roles" },
  // Read both `watch_safe_signers` (auto-enrolled rows + new editor
  // saves) and `watch_signers` (legacy alias) so the chip lights up
  // either way. ``aliases`` is consumed by ``monitoringChips``.
  { key: "watch_safe_signers", aliases: ["watch_signers"], label: "Safe activity" },
  { key: "watch_timelock", label: "Timelock" },
  { key: "watch_state", label: "State" },
];

const MONITOR_ALERT_GROUPS = [
  {
    key: "upgrades",
    label: "Upgrades",
    flags: ["watch_upgrades"],
    eventTypes: ["upgraded", "admin_changed", "beacon_upgraded"],
  },
  {
    key: "ownership",
    label: "Ownership",
    flags: ["watch_ownership"],
    eventTypes: ["ownership_transferred"],
  },
  {
    key: "pause",
    label: "Pause",
    flags: ["watch_pause"],
    eventTypes: ["paused", "unpaused"],
  },
  {
    key: "roles",
    label: "Roles",
    flags: ["watch_roles"],
    eventTypes: ["role_granted", "role_revoked"],
  },
  {
    key: "signers",
    label: "Safe activity",
    // Backend's _should_watch maps both signer changes AND Safe-tx
    // executions onto `watch_safe_signers` — the historical UI-only
    // alias `watch_signers` stays for backward compat with old alerts.
    flags: ["watch_safe_signers", "watch_signers"],
    eventTypes: [
      "signer_added",
      "signer_removed",
      "threshold_changed",
      "safe_tx_executed",
      "safe_tx_failed",
      "safe_module_executed",
      "safe_module_failed",
    ],
  },
  {
    key: "timelock",
    label: "Timelock",
    flags: ["watch_timelock"],
    eventTypes: ["timelock_scheduled", "timelock_executed", "delay_changed"],
  },
  {
    key: "state",
    label: "State polling",
    flags: ["watch_state"],
    eventTypes: ["state_changed_poll"],
    needsPolling: true,
  },
];

function shortAddr(address) {
  if (!address || address.length < 12) return address || "";
  return address.slice(0, 6) + ".." + address.slice(-4);
}

function formatDelay(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value <= 0) return "";
  if (value >= 86400) return `${Math.round(value / 86400)}d`;
  if (value >= 3600) return `${Math.round(value / 3600)}h`;
  return `${Math.round(value / 60)}m`;
}

function maskWebhook(url) {
  if (!url) return "";
  const value = String(url);
  if (value.length <= 24) return value;
  return `${value.slice(0, 18)}...${value.slice(-8)}`;
}

function functionName(signature) {
  return String(signature || "?").split("(")[0] || "?";
}

function isRoleConstant(name) {
  return /^[A-Z][A-Z0-9_]+$/.test(name);
}

function hasHint(name, hints) {
  return hints.some((hint) => name.includes(hint));
}

function laneForFunction(fn) {
  const effects = new Set(fn.effect_labels || []);
  const loweredName = functionName(fn.function).toLowerCase();

  if ([...CONTROL_EFFECTS].some((label) => effects.has(label))) return "top";
  if ([...INPUT_EFFECTS].some((label) => effects.has(label)) && ![...OUTPUT_EFFECTS].some((label) => effects.has(label))) return "left";
  if ([...OUTPUT_EFFECTS].some((label) => effects.has(label))) return "right";
  if (hasHint(loweredName, CONTROL_HINTS)) return "top";
  if (hasHint(loweredName, INPUT_HINTS) && !hasHint(loweredName, OUTPUT_HINTS)) return "left";
  if (hasHint(loweredName, OUTPUT_HINTS)) return "right";
  return "ops";
}

function toneForFunction(fn, lane) {
  const effects = new Set(fn.effect_labels || []);
  if (effects.has("implementation_update") || effects.has("delegatecall_execution")) return "#9b8a9e";
  if (effects.has("ownership_transfer")) return "#9e8a8d";
  if (effects.has("role_management") || effects.has("authority_update") || effects.has("hook_update")) return "#7a8098";
  if (effects.has("pause_toggle")) return "#998a6a";
  if (effects.has("timelock_operation")) return "#8a7e6a";
  if (effects.has("asset_pull") || effects.has("mint")) return "#6a9e94";
  if (effects.has("asset_send") || effects.has("burn")) return "#9a8a6e";
  return LANE_META[lane].tone;
}

function compactActionSummary(fn) {
  const effects = new Set(fn.effect_labels || []);
  if (effects.has("implementation_update")) return "changes logic";
  if (effects.has("delegatecall_execution")) return "delegatecall path";
  if (effects.has("ownership_transfer")) return "changes owner";
  if (effects.has("authority_update")) return "changes authority";
  if (effects.has("hook_update")) return "changes hook";
  if (effects.has("pause_toggle")) return "pause control";

  if (effects.has("asset_pull") || effects.has("mint")) return "moves value in";
  if (effects.has("asset_send") || effects.has("burn")) return "moves value out";
  return "";
}

function lanePriority(fn) {
  const effects = new Set(fn.effect_labels || []);
  if (effects.has("implementation_update") || effects.has("delegatecall_execution")) return 0;
  if (effects.has("ownership_transfer")) return 1;
  if (effects.has("role_management") || effects.has("authority_update") || effects.has("hook_update")) return 2;
  if (effects.has("pause_toggle")) return 3;
  if (effects.has("timelock_operation")) return 4;
  if (effects.has("asset_pull") || effects.has("mint")) return 5;
  if (effects.has("asset_send") || effects.has("burn")) return 6;
  if (effects.has("arbitrary_external_call") || effects.has("external_contract_call")) return 7;
  return 9;
}

function isRoleIdAddress(address) {
  const hex = address.slice(2);
  const leadingZeros = hex.match(/^0*/)[0].length;
  return leadingZeros >= 24;
}

// Build a minimal nodeInfo + edge lookup over the per-contract control graphs
// so we can surface *indirect* upstream governance context without flattening
// function-level direct callers into it.
function buildControlGraphIndex(companyData) {
  const controllerOf = new Map(); // from-address → [{to, relation}]
  const nodeInfo = new Map();
  if (!companyData) return { controllerOf, nodeInfo };
  for (const contract of companyData.contracts || []) {
    const cg = contract.control_graph;
    if (!cg) continue;
    for (const node of cg.nodes || []) {
      const addr = (node.address || "").toLowerCase();
      if (addr) nodeInfo.set(addr, node);
    }
    for (const edge of cg.edges || []) {
      // safe_owner edges are UI noise — owners are rendered nested under their Safe.
      if (edge.relation === "safe_owner") continue;
      const from = (edge.from || "").toLowerCase();
      const to = (edge.to || "").toLowerCase();
      if (!from || !to || from === to) continue;
      if (!controllerOf.has(from)) controllerOf.set(from, []);
      const existing = controllerOf.get(from);
      if (!existing.some((e) => e.to === to)) {
        existing.push({ to, relation: edge.relation });
      }
    }
  }
  return { controllerOf, nodeInfo };
}

// Direct callers = exactly what effective_permissions emits for the function:
// direct_owner, authority_roles[].principals, controllers[].principals. Contract
// principals stay as contracts — we do NOT replace them with "first reachable
// Safe/timelock/EOA" via the control graph, because that produces false claims
// like "Safe can pause" when the function is role-gated and the Safe doesn't
// hold that role. See ProtocolSurface note at ~line 236.
function collectDirectCallers(fn) {
  const byAddress = new Map();

  function pushPrincipal(principal, origin) {
    const address = String(principal?.address || "").toLowerCase();
    if (!address.startsWith("0x")) return;
    if (isRoleIdAddress(address)) return;
    const existing = byAddress.get(address);
    if (existing) {
      if (!existing.origins.includes(origin)) existing.origins.push(origin);
      return;
    }
    byAddress.set(address, {
      address,
      resolvedType: String(principal.resolved_type || "unknown"),
      details: principal.details && typeof principal.details === "object" ? { ...principal.details } : {},
      label: principal.label || null,
      sourceContract: principal.source_contract || null,
      sourceControllerId: principal.source_controller_id || null,
      origins: [origin],
    });
  }

  if (fn.direct_owner) {
    pushPrincipal(fn.direct_owner, "direct owner");
  }
  for (const roleGrant of fn.authority_roles || []) {
    for (const principal of roleGrant.principals || []) {
      pushPrincipal(principal, `role ${roleGrant.role}`);
    }
  }
  for (const controller of fn.controllers || []) {
    const label = controller.label || controller.controller_id || "controller";
    for (const principal of controller.principals || []) {
      pushPrincipal(principal, label);
    }
  }

  return [...byAddress.values()].sort((a, b) => a.address.localeCompare(b.address));
}

// Indirect control path = walk outgoing edges from each direct-caller contract
// principal until we hit non-contract principals (safes, timelocks, EOAs).
// Reported separately so the UI can present it as "governance context" rather
// than claiming those principals can directly call the function.
function collectIndirectPath(directCallers, graphIndex) {
  const { controllerOf, nodeInfo } = graphIndex;
  const out = new Map();
  const visited = new Set();

  function walk(addr, depth, trail) {
    if (!addr || visited.has(addr) || depth > 6) return;
    visited.add(addr);
    const edges = controllerOf.get(addr) || [];
    for (const edge of edges) {
      const to = edge.to;
      if (!to) continue;
      const node = nodeInfo.get(to);
      const isContract = node && node.type === "contract";
      if (!isContract && !isRoleIdAddress(to)) {
        // Keep the first path we discover to each principal — shorter paths
        // are more informative and dedupe visual clutter.
        if (!out.has(to)) {
          out.set(to, {
            address: to,
            resolvedType: String(node?.type || "unknown"),
            details: node?.details && typeof node.details === "object" ? { ...node.details } : {},
            label: node?.label || null,
            path: [...trail, { address: to, relation: edge.relation }],
          });
        }
      }
      if (isContract) {
        walk(to, depth + 1, [...trail, { address: to, relation: edge.relation }]);
      }
    }
  }

  for (const caller of directCallers) {
    if (caller.resolvedType !== "contract") continue;
    visited.clear();
    walk(caller.address, 0, [{ address: caller.address, relation: "direct" }]);
  }

  return [...out.values()].sort((a, b) => a.address.localeCompare(b.address));
}

function collectPrincipals(fn, companyData) {
  const direct = collectDirectCallers(fn);
  const graphIndex = buildControlGraphIndex(companyData);
  const indirect = collectIndirectPath(direct, graphIndex);
  return { direct, indirect };
}

function guardSummary(fn, companyData) {
  const { direct, indirect } = collectPrincipals(fn, companyData);
  // `principals` stays as the direct list for backward compatibility — every
  // consumer that reads `fnView.guard.principals` only cares about who can
  // actually call the function *now*, not the governance chain above that.
  const principals = direct;

  if (!direct.length) {
    const meta = TYPE_META[fn.authority_public ? "open" : "unknown"];
    return {
      kind: fn.authority_public ? "open" : "unknown",
      label: meta.label,
      sublabel: fn.authority_public ? "public" : "unresolved",
      accent: meta.accent,
      principals,
      indirect,
    };
  }

  if (direct.length > 1) {
    return {
      kind: "many",
      label: `${direct.length}P`,
      sublabel: "mixed",
      accent: TYPE_META.many.accent,
      principals,
      indirect,
    };
  }

  const principal = direct[0];
  const type = TYPE_META[principal.resolvedType] || TYPE_META.unknown;
  const safeOwners = Array.isArray(principal.details?.owners) ? principal.details.owners.length : 0;
  const threshold = Number(principal.details?.threshold);
  const delay = formatDelay(principal.details?.delay);

  let sublabel = shortAddr(principal.address);
  if (principal.resolvedType === "safe" && safeOwners) {
    sublabel = Number.isFinite(threshold) && threshold > 0 ? `${threshold}/${safeOwners}` : `${safeOwners} sig`;
  } else if (principal.resolvedType === "timelock" && delay) {
    sublabel = delay;
  } else if (principal.resolvedType === "contract") {
    // Prefer the contract's own name from the protocol inventory over the
    // generic word "contract" — fetchNextKeyIndex resolving to "AuctionManager"
    // tells the user something; "contract" doesn't.
    const targetAddr = principal.address?.toLowerCase();
    const named = (companyData?.contracts || []).find(
      (c) => c.address?.toLowerCase() === targetAddr,
    );
    sublabel = principal.label || named?.name || "contract";
  }

  return {
    kind: principal.resolvedType,
    label: type.label,
    sublabel,
    accent: type.accent,
    principals,
    indirect,
  };
}

function buildMachines(companyData, functionData) {
  return companyData.contracts
    .map((contract) => {
      const rawFunctions = (functionData[contract.address] || [])
        .filter((fn) => !isRoleConstant(functionName(fn.function)));
      const lanes = { top: [], left: [], right: [], ops: [] };

      for (const fn of rawFunctions) {
        const lane = laneForFunction(fn);
        const { direct, indirect } = collectPrincipals(fn, companyData);
        lanes[lane].push({
          key: `${contract.address}:${fn.selector || fn.function}`,
          contractName: contract.name,
          contractAddress: contract.address,
          name: functionName(fn.function),
          signature: fn.function || fn.abi_signature || "?",
          lane,
          tone: toneForFunction(fn, lane),
          action: compactActionSummary(fn),
          effectLabels: fn.effect_labels || [],
          guard: guardSummary(fn, companyData),
          // `principals` is the direct-callers list — exactly who can fire
          // msg.sender on this function right now. `indirectPrincipals` is the
          // governance path above any contract principals, shown as secondary
          // context in the inspector (never used to claim call rights).
          principals: direct,
          indirectPrincipals: indirect,
          authorityPublic: Boolean(fn.authority_public),
        });
      }

      for (const lane of Object.keys(lanes)) {
        lanes[lane].sort((left, right) => {
          const score = lanePriority({ effect_labels: left.effectLabels }) - lanePriority({ effect_labels: right.effectLabels });
          if (score !== 0) return score;
          return left.name.localeCompare(right.name);
        });
      }

      const totalFunctions = lanes.top.length + lanes.ops.length + lanes.left.length + lanes.right.length;
      return {
        ...contract,
        totalFunctions,
        lanes,
      };
    })
    .filter((machine) => machine.totalFunctions > 0 || machine.is_proxy)
    .sort((left, right) => {
      if (right.totalFunctions !== left.totalFunctions) return right.totalFunctions - left.totalFunctions;
      return String(left.name || "").localeCompare(String(right.name || ""));
    });
}

function machineFunctions(machine) {
  if (!machine?.lanes) return [];
  return [
    ...(machine.lanes.top || []),
    ...(machine.lanes.ops || []),
    ...(machine.lanes.left || []),
    ...(machine.lanes.right || []),
  ];
}

function tabForLane(lane) {
  if (lane === "left") return "inflows";
  if (lane === "right") return "outflows";
  return "control";
}

function findFunctionView(machine, target = {}) {
  const signature = String(target.functionSignature || target.fn || "").toLowerCase();
  const selector = String(target.selector || "").toLowerCase();
  if (!signature && !selector) return null;
  return machineFunctions(machine).find((fnView) => {
    const fnSig = String(fnView.signature || "").toLowerCase();
    const fnKey = String(fnView.key || "").toLowerCase();
    if (selector && fnKey.endsWith(`:${selector}`)) return true;
    return signature && fnSig === signature;
  }) || null;
}

function FunctionPort({ fnView, onSelect, onNavigate, orientation, highlighted }) {
  return (
    <div className={`ps-port ps-port-${orientation}${highlighted ? " ps-port-score-highlight" : ""}`} style={{ "--port-accent": fnView.tone }}>
      <div className="ps-port-copy" onClick={() => onSelect(fnView)} style={{ cursor: "pointer" }}>
        <div className="ps-port-name">{fnView.name}</div>
        {fnView.action && <div className="ps-port-action">{fnView.action}</div>}
      </div>
      <GuardButton fnView={fnView} onSelect={onSelect} onNavigate={onNavigate} />
    </div>
  );
}

const OPS_CATEGORIES = [
  { key: "setters", label: "Setters", match: (n) => /^(set|unset|reset)/i.test(n) },
  { key: "updates", label: "Updates", match: (n) => /^update/i.test(n) },
  { key: "add-remove", label: "Add / Remove", match: (n) => /^(add|remove)/i.test(n) },
  { key: "proposals", label: "Proposals", match: (n) => /^(propose|confirm|cancel)/i.test(n) },
  { key: "lifecycle", label: "Lifecycle", match: (n) => /^(initialize|create|delete|destroy|finalize|migrate)/i.test(n) },
  { key: "recovery", label: "Recovery", match: (n) => /^recover/i.test(n) },
  { key: "reports", label: "Reports", match: (n) => /^report/i.test(n) },
  { key: "other", label: "Other", match: () => true },
];

function categorizeOps(items) {
  const groups = OPS_CATEGORIES.map((cat) => ({ ...cat, items: [] }));
  const assigned = new Set();
  for (const cat of groups) {
    for (const item of items) {
      if (!assigned.has(item.key) && cat.match(item.name)) {
        cat.items.push(item);
        assigned.add(item.key);
      }
    }
  }
  return groups.filter((g) => g.items.length > 0);
}

function OpsCategory({ category, onSelect, onNavigate, highlightedFunctionKey }) {
  const [expanded, setExpanded] = useState(false);
  const containsHighlight = category.items.some((fnView) => fnView.key === highlightedFunctionKey);
  useEffect(() => {
    if (containsHighlight) setExpanded(true);
  }, [containsHighlight]);
  return (
    <div className="ps-ops-category">
      <button
        type="button"
        className="ps-ops-category-header"
        onClick={() => setExpanded(!expanded)}
      >
        <span className={`ps-ops-chevron${expanded ? " ps-ops-chevron-open" : ""}`}>&#9656;</span>
        <span className="ps-ops-category-label">{category.label}</span>
        <span className="ps-ops-category-count">{category.items.length}</span>
      </button>
      {expanded && (
        <div className="ps-ops-category-body">
          {category.items.map((fnView) => (
            <FunctionPort
              key={fnView.key}
              fnView={fnView}
              orientation="ops"
              onSelect={onSelect}
              onNavigate={onNavigate}
              highlighted={fnView.key === highlightedFunctionKey}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function OpsLane({ items, onSelect, onNavigate, highlightedFunctionKey }) {
  const categories = useMemo(() => categorizeOps(items), [items]);
  return (
    <section className="ps-lane ps-lane-ops">
      <div className="ps-lane-header">
        <span className="ps-lane-title"><span>Operations</span></span>
        <span>{items.length}</span>
      </div>
      <div className="ps-lane-body ps-ops-groups">
        {categories.length ? (
          categories.map((cat) => (
            <OpsCategory
              key={cat.key}
              category={cat}
              onSelect={onSelect}
              onNavigate={onNavigate}
              highlightedFunctionKey={highlightedFunctionKey}
            />
          ))
        ) : (
          <div className="ps-lane-empty">No mapped functions</div>
        )}
      </div>
    </section>
  );
}

function LaneColumn({ title, laneKey, items, onSelect, onNavigate, highlightedFunctionKey }) {
  return (
    <section className={`ps-lane ps-lane-${laneKey}`}>
      <div className="ps-lane-header">
        <span className="ps-lane-title">
          <span>{title}</span>
        </span>
        <span>{items.length}</span>
      </div>
      <div className="ps-lane-body">
        {items.length ? (
          items.map((fnView) => (
            <FunctionPort
              key={fnView.key}
              fnView={fnView}
              orientation={laneKey}
              onSelect={onSelect}
              onNavigate={onNavigate}
              highlighted={fnView.key === highlightedFunctionKey}
            />
          ))
        ) : (
          <div className="ps-lane-empty">No mapped functions</div>
        )}
      </div>
    </section>
  );
}

function BalanceTable({ machine }) {
  const [hideDust, setHideDust] = useState(true);

  if (!machine.balances || machine.balances.length === 0) {
    return <div className="ps-lane-empty">No token balances</div>;
  }

  const filtered = hideDust
    ? machine.balances.filter((b) => b.usd_value == null || b.usd_value >= 10)
    : machine.balances;
  const hiddenCount = machine.balances.length - filtered.length;

  return (
    <section className="ps-balance-section">
      <div className="ps-balance-header">
        <span>Balances</span>
        {machine.total_usd ? <span className="ps-balance-total">{formatUsd(machine.total_usd)}</span> : null}
      </div>
      <button
        className={`ps-balance-filter${hideDust ? " active" : ""}`}
        onClick={() => setHideDust(!hideDust)}
      >
        {hideDust ? `Hide <$10 (${hiddenCount})` : "Show all"}
      </button>
      <div className="ps-balance-list">
        {filtered.map((b, i) => {
          const human = Number(b.raw_balance) / (10 ** b.decimals);
          const amount = human >= 1e6 ? `${(human / 1e6).toFixed(1)}M`
            : human >= 1e3 ? `${(human / 1e3).toFixed(1)}K`
            : human >= 1 ? human.toFixed(2)
            : human.toFixed(6);
          return (
            <div key={i} className="ps-balance-row">
              <div className="ps-balance-token">
                <span className="ps-balance-symbol">{b.token_symbol}</span>
                <span className="ps-balance-name">{b.token_name}</span>
              </div>
              <div className="ps-balance-values">
                <span className="ps-balance-amount">{amount}</span>
                <span className="ps-balance-usd">{b.usd_value ? formatUsd(b.usd_value) : "—"}</span>
              </div>
            </div>
          );
        })}
        {filtered.length === 0 && <div className="ps-lane-empty">No balances above $10</div>}
      </div>
    </section>
  );
}

const MACHINE_TABS = [
  { key: "control", label: "Control" },
  { key: "inflows", label: "Inflows" },
  { key: "outflows", label: "Outflows" },
  { key: "balances", label: "Balances" },
];

function ContractMachine({
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

function isHexAddress(value) {
  return /^0x[a-fA-F0-9]{40}$/.test(String(value || ""));
}

function buildFallbackDependencyGraph(machine) {
  if (!machine?.address) return null;
  const nodes = new Map();
  const edges = [];
  const targetId = `contract:${machine.address.toLowerCase()}`;

  function addNode(address, values = {}) {
    if (!isHexAddress(address)) return null;
    const id = `contract:${address.toLowerCase()}`;
    if (!nodes.has(id)) {
      nodes.set(id, {
        id,
        address,
        label: values.label || shortAddr(address),
        type: values.type || "regular",
        source: values.source || [],
        proxy_type: values.proxy_type || null,
        is_target: Boolean(values.is_target),
        is_proxy_context: Boolean(values.is_proxy_context),
      });
    } else {
      nodes.set(id, { ...nodes.get(id), ...values });
    }
    return id;
  }

  addNode(machine.address, {
    label: machine.name || shortAddr(machine.address),
    type: machine.is_proxy ? "proxy" : "regular",
    proxy_type: machine.proxy_type,
    is_target: true,
    source: ["selected"],
  });

  if (isHexAddress(machine.implementation)) {
    const implId = addNode(machine.implementation, {
      label: `${machine.name || "Implementation"} impl`,
      type: "implementation",
      source: ["proxy"],
    });
    edges.push({
      from: targetId,
      to: implId,
      op: "DELEGATES_TO",
      function_name: machine.proxy_type || "implementation",
    });
  }

  if (isHexAddress(machine.owner)) {
    const ownerId = addNode(machine.owner, {
      label: "owner",
      type: "regular",
      source: ["owner"],
    });
    edges.push({
      from: targetId,
      to: ownerId,
      op: "STATIC_REF",
      function_name: "owner",
    });
  }

  for (const [controllerId, value] of Object.entries(machine.controllers || {})) {
    if (!isHexAddress(value)) continue;
    const controllerNodeId = addNode(value, {
      label: controllerId.replace(/^[^:]+:/, ""),
      type: "regular",
      source: ["controller"],
    });
    edges.push({
      from: targetId,
      to: controllerNodeId,
      op: "STATIC_REF",
      function_name: controllerId.split(":").pop() || "controller",
    });
  }

  const uniqueEdges = [];
  const seenEdges = new Set();
  for (const edge of edges) {
    if (!edge.from || !edge.to || edge.from === edge.to) continue;
    const key = `${edge.from}|${edge.to}|${edge.function_name}`;
    if (seenEdges.has(key)) continue;
    seenEdges.add(key);
    uniqueEdges.push(edge);
  }

  if (nodes.size <= 1 && uniqueEdges.length === 0) return null;
  return { nodes: [...nodes.values()], edges: uniqueEdges };
}

function DependencyGraphModal({ machine, onClose }) {
  const [graphData, setGraphData] = useState(null);
  const [graphNote, setGraphNote] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const onKey = (event) => {
      if (event.key === "Escape") onClose?.();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    if (!machine) return undefined;
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      setGraphData(null);
      setGraphNote(null);
      const ids = [machine.job_id, machine.impl_job_id, machine.address]
        .filter(Boolean)
        .filter((id, index, arr) => arr.indexOf(id) === index);
      let sawArtifact = false;
      let lastError = null;

      for (const id of ids) {
        const encoded = encodeURIComponent(id);
        try {
          const detail = await api(`/api/analyses/${encoded}`);
          if (cancelled) return;
          if (detail?.dependency_graph_viz?.nodes?.length) {
            setGraphData(detail.dependency_graph_viz);
            setGraphNote(null);
            setLoading(false);
            return;
          }
          if ((detail?.available_artifacts || []).includes("dependency_graph_viz")) {
            sawArtifact = true;
          }
        } catch (err) {
          lastError = err;
        }

        if (!sawArtifact) continue;

        try {
          const artifact = await api(`/api/analyses/${encoded}/artifact/dependency_graph_viz.json`);
          if (cancelled) return;
          if (artifact?.nodes?.length) {
            setGraphData(artifact);
            setGraphNote(null);
            setLoading(false);
            return;
          }
        } catch (err) {
          lastError = err;
        }
      }

      if (cancelled) return;
      const fallback = buildFallbackDependencyGraph(machine);
      if (fallback?.nodes?.length) {
        setGraphData(fallback);
        setGraphNote("Fallback graph generated from selected contract metadata because no stored dependency artifact loaded.");
        setLoading(false);
        return;
      }

      if (!cancelled) {
        setError(
          sawArtifact
            ? `Dependency graph artifact is listed, but it could not be loaded${lastError?.message ? `: ${lastError.message}` : "."}`
            : "No dependency graph artifact is available for this contract.",
        );
        setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [machine]);

  if (!machine) return null;

  return (
    <div className="ps-modal-backdrop" onMouseDown={onClose}>
      <div
        className="ps-dependency-modal"
        role="dialog"
        aria-modal="true"
        aria-label="Dependency graph"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="ps-dependency-modal-header">
          <div>
            <div className="ps-dependency-modal-eyebrow">Dependency Graph</div>
            <h2>{machine.name || shortAddr(machine.address)}</h2>
            <a
              className="ps-dependency-modal-sub ps-scanner-link"
              href={blockExplorerAddressUrl(machine.address, machine.chain)}
              target="_blank"
              rel="noreferrer"
            >
              {machine.address}
            </a>
          </div>
          <button type="button" className="ps-modal-close" onClick={onClose} aria-label="Close dependency graph">
            x
          </button>
        </header>
        <div className="ps-dependency-modal-body">
          {loading && <div className="ps-modal-empty">Loading dependency graph...</div>}
          {!loading && error && <div className="ps-modal-empty ps-modal-empty-error">{error}</div>}
          {!loading && graphData && (
            <>
              {graphNote && <div className="ps-dependency-note">{graphNote}</div>}
              <DependencyGraphTab data={graphData} runName={null} chain={machine.chain} />
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function AuditsPanel({ machine, companyName }) {
  const [timeline, setTimeline] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const contractId = machine.contract_id;

  useEffect(() => {
    if (!contractId) {
      setTimeline(null);
      return undefined;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    getTimeline(contractId)
      .then((data) => {
        if (cancelled) return;
        setTimeline(data);
        setLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err?.message || "Failed to load audits");
        setLoading(false);
      });
    return () => { cancelled = true; };
  }, [contractId]);

  if (!contractId) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Contract not yet indexed for audit coverage.</div>
      </section>
    );
  }
  if (loading) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Loading audits…</div>
      </section>
    );
  }
  if (error) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Failed to load audits: {error}</div>
      </section>
    );
  }
  if (!timeline) return null;

  const statusMeta = AUDIT_STATUS_META[timeline.current_status] || AUDIT_STATUS_META.non_proxy_unaudited;
  const coverage = timeline.coverage || [];
  const topAudits = coverage.slice(0, 5);

  const handleAuditClick = (auditId) => {
    const url = `/company/${encodeURIComponent(companyName)}/audits?audit=${encodeURIComponent(auditId)}`;
    window.location.href = url;
  };

  return (
    <section className="ps-principal-section">
      <div className="ps-principal-section-hdr">Audit coverage</div>

      <div style={{ marginBottom: 12 }}>
        <MetaBadge meta={statusMeta} />
        <span style={{ marginLeft: 8, color: "#6b7590", fontSize: 12 }}>
          {coverage.length} audit{coverage.length === 1 ? "" : "s"}
          {coverage.length > topAudits.length ? ` (showing ${topAudits.length} most recent)` : ""}
        </span>
      </div>

      {topAudits.length === 0 ? (
        <div className="ps-inspector-empty">No audits cover this contract yet.</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {topAudits.map((a) => {
            const matchMeta = MATCH_TYPE_META[a.match_type] || MATCH_TYPE_META.direct;
            // drift === true means the bytecode at the impl address changed
            // since this audit was matched. drift === null/undefined means
            // we couldn't determine (missing keccak on either side) — show
            // no badge rather than a misleading one.
            const driftKnown = a.bytecode_drift === true || a.bytecode_drift === false;
            return (
              <button
                key={a.audit_id}
                onClick={() => handleAuditClick(a.audit_id)}
                style={{
                  textAlign: "left",
                  padding: "8px 10px",
                  borderRadius: 6,
                  border: "1px solid #e2e8f0",
                  background: "#fafafa",
                  cursor: "pointer",
                  font: "inherit",
                  color: "inherit",
                }}
              >
                <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 8 }}>
                  <div style={{ fontWeight: 600, fontSize: 13 }}>{a.auditor || "Unknown"}</div>
                  <div style={{ fontSize: 11, color: "#6b7590", whiteSpace: "nowrap" }}>{formatAuditDate(a.date)}</div>
                </div>
                <div style={{ fontSize: 12, color: "#334155", marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {a.title || ""}
                </div>
                <div style={{ marginTop: 4, display: "flex", gap: 4, flexWrap: "wrap" }}>
                  <MetaBadge meta={matchMeta} />
                  {a.match_confidence && (
                    <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
                      {a.match_confidence}
                    </span>
                  )}
                  {a.equivalence_status && a.equivalence_status !== "proven" && EQUIVALENCE_META[a.equivalence_status] && (
                    <MetaBadge
                      meta={EQUIVALENCE_META[a.equivalence_status]}
                      title={a.equivalence_reason || ""}
                    />
                  )}
                  {a.equivalence_status === "proven" && a.proof_kind && PROOF_KIND_META[a.proof_kind] && (
                    <MetaBadge
                      meta={PROOF_KIND_META[a.proof_kind]}
                      title={proofKindTitle(a.proof_kind)}
                    />
                  )}
                  {a.bytecode_drift === true && (
                    <MetaBadge
                      meta={DRIFT_TRUE_META}
                      label="⚠ code changed"
                      title="Runtime bytecode at this impl changed since the audit was matched"
                    />
                  )}
                  {a.bytecode_drift === false && (
                    <MetaBadge
                      meta={DRIFT_FALSE_META}
                      label="✓ bytecode stable"
                      title="Runtime bytecode hash matches the hash captured at audit match time"
                    />
                  )}
                  {!driftKnown && a.bytecode_keccak_now && !a.bytecode_keccak_at_match && (
                    <span
                      className="ps-badge"
                      title="Anchor not set — refresh coverage to stamp runtime bytecode hash"
                      style={{
                        "--badge-accent": "#6b7590",
                        fontSize: 10,
                      }}
                    >
                      drift unverified
                    </span>
                  )}
                </div>
              </button>
            );
          })}
        </div>
      )}

      <LiveFindingsSection coverage={coverage} />
    </section>
  );
}

function LiveFindingsSection({ coverage }) {
  // Collect every live finding across covering audits, tagged with its
  // originating auditor/title so the user can see which audit raised it.
  const entries = [];
  for (const a of coverage) {
    const lf = a.live_findings || [];
    for (const f of lf) {
      entries.push({ finding: f, audit: a });
    }
  }

  if (entries.length === 0) return null;

  // Sort: severity descending (critical first), stable within.
  const severityRank = { critical: 0, high: 1, medium: 2, low: 3, info: 4 };
  entries.sort((x, y) => {
    const rx = severityRank[x.finding.severity] ?? 5;
    const ry = severityRank[y.finding.severity] ?? 5;
    return rx - ry;
  });

  return (
    <div style={{ marginTop: 16 }}>
      <div className="ps-principal-section-hdr">Live findings on current code</div>
      <div style={{ fontSize: 11, color: "#6b7590", marginBottom: 8 }}>
        Issues that were not marked "fixed" in the audit. May still be active.
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {entries.map((e, i) => {
          const sevMeta = SEVERITY_META[e.finding.severity] || SEVERITY_META.info;
          const statusLabel = STATUS_LABELS[e.finding.status] || e.finding.status || "?";
          return (
            <div
              key={i}
              style={{
                padding: "6px 8px",
                borderRadius: 5,
                border: "1px solid #e2e8f0",
                background: "#fff",
              }}
            >
              <div style={{ display: "flex", gap: 6, alignItems: "baseline", flexWrap: "wrap" }}>
                <MetaBadge meta={sevMeta} label={e.finding.severity || "info"} />
                <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
                  {statusLabel}
                </span>
                <span style={{ fontSize: 11, color: "#6b7590" }}>
                  {e.audit.auditor || "Unknown"}
                </span>
              </div>
              <div style={{ fontSize: 12, color: "#334155", marginTop: 4 }}>
                {e.finding.title || "(untitled)"}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

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

function InspectorCard({ selected, onNavigate }) {
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

// Pretty event-type label for the activity section. Falls back to the
// raw underscore-separated form if we get an event_type the watcher
// emits but the UI hasn't taught itself about yet.
const EVENT_LABELS = {
  signer_added: "Signer added",
  signer_removed: "Signer removed",
  threshold_changed: "Threshold changed",
  safe_tx_executed: "Tx executed",
  safe_tx_failed: "Tx failed",
  safe_module_executed: "Module call",
  safe_module_failed: "Module call failed",
  timelock_scheduled: "Queued",
  timelock_executed: "Executed",
  delay_changed: "Delay changed",
  ownership_transferred: "Owner transferred",
  paused: "Paused",
  unpaused: "Unpaused",
  role_granted: "Role granted",
  role_revoked: "Role revoked",
};
const EVENT_ACCENTS = {
  signer_added: "#3b82f6",
  signer_removed: "#3b82f6",
  threshold_changed: "#f59e0b",
  safe_tx_executed: "#22c55e",
  safe_tx_failed: "#ef4444",
  safe_module_executed: "#22c55e",
  safe_module_failed: "#ef4444",
  timelock_scheduled: "#3b82f6",
  timelock_executed: "#f59e0b",
  delay_changed: "#f59e0b",
  ownership_transferred: "#ef4444",
  paused: "#ef4444",
  unpaused: "#ef4444",
  role_granted: "#f59e0b",
  role_revoked: "#f59e0b",
};

function formatEventAgo(detectedAt) {
  if (!detectedAt) return null;
  const d = new Date(detectedAt);
  if (Number.isNaN(d.getTime())) return null;
  const seconds = Math.max(0, (Date.now() - d.getTime()) / 1000);
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  if (seconds < 30 * 86400) return `${Math.floor(seconds / 86400)}d ago`;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function PrincipalDetail({ principal, machines, onNavigate, onFocusContract, addressLabels, refreshAddressLabels }) {
  const [focusIdx, setFocusIdx] = useState(0);
  // Recent on-chain activity for safes/timelocks. Sourced from
  // /api/monitored-events keyed by address — the unified watcher writes
  // a MonitoredEvent row for every CallScheduled/CallExecuted, signer
  // change, and Safe-tx execution it sees. Lazy-loaded only when a
  // safe or timelock is selected so we don't hit the endpoint for
  // every principal click on contracts/EOAs/etc.
  const [activity, setActivity] = useState(null);
  const [activityError, setActivityError] = useState(null);
  const principalAddress = principal?.address?.toLowerCase();
  const principalType = principal?.type;
  const wantsActivity = principalType === "safe" || principalType === "timelock";

  useEffect(() => {
    if (!wantsActivity || !principalAddress) {
      setActivity(null);
      setActivityError(null);
      return;
    }
    let cancelled = false;
    setActivity(null);
    setActivityError(null);
    api(`/api/monitored-events?address=${encodeURIComponent(principalAddress)}&limit=15`)
      .then((rows) => { if (!cancelled) setActivity(Array.isArray(rows) ? rows : []); })
      .catch((e) => { if (!cancelled) setActivityError(e.message || String(e)); });
    return () => { cancelled = true; };
  }, [wantsActivity, principalAddress]);

  if (!principal) return null;
  const type = TYPE_META[principal.type] || TYPE_META.unknown;
  const controlled = (principal.controls || []);
  const controlledMachines = machines.filter((m) =>
    controlled.some((a) => a.toLowerCase() === m.address?.toLowerCase())
  );
  const owners = principal.details?.owners || [];
  const threshold = principal.details?.threshold;
  const delay = principal.details?.delay;

  return (
    <article className="ps-machine" style={{ borderLeft: `2px solid ${type.accent}` }}>
      <header className="ps-machine-header">
        <div className="ps-machine-name">
          {addressLabels?.get(principal.address?.toLowerCase()) || principal.label || shortAddr(principal.address)}
        </div>
        <div className="ps-machine-address">{principal.address}</div>
        {/* Only EOAs and Safes are "just an address" — add the inline label
            affordance for those. Timelocks and contracts already have a
            meaningful contract-level name. */}
        {(principal.type === "eoa" || principal.type === "safe") && (
          <div style={{ marginTop: 6 }}>
            <AddressLabelInline
              address={principal.address}
              labels={addressLabels}
              refreshAll={refreshAddressLabels}
            />
          </div>
        )}
        <div className="ps-machine-badges">
          <span className="ps-badge" style={{ "--badge-accent": type.accent }}>{type.label}</span>
          {principal.type === "safe" && threshold && (
            <span className="ps-badge" style={{ "--badge-accent": "#6a9e94" }}>{threshold}/{owners.length} threshold</span>
          )}
          {principal.type === "timelock" && delay > 0 && (
            <span className="ps-badge" style={{ "--badge-accent": "#9a8a6e" }}>{formatDelay(delay)} delay</span>
          )}
        </div>
      </header>

      {principal.type === "safe" && owners.length > 0 && (
        <section className="ps-principal-section">
          <div className="ps-principal-section-hdr">Signers ({owners.length})</div>
          {owners.map((addr) => {
            const labeled = addressLabels?.get((addr || "").toLowerCase());
            return (
              <div key={addr} className="ps-principal-signer">
                <span className="ps-principal-signer-row">
                  {labeled && <span className="ps-principal-signer-name">{labeled}</span>}
                  <span className="ps-principal-signer-addr">{addr}</span>
                </span>
                <AddressLabelInline
                  address={addr}
                  labels={addressLabels}
                  refreshAll={refreshAddressLabels}
                  size="xs"
                />
              </div>
            );
          })}
        </section>
      )}

      {controlledMachines.length > 0 && (
        <section className="ps-principal-section">
          <div className="ps-principal-section-hdr" style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            {/* This list is derived from the recursive control graph, not from
                function-level permissions. Labeling it "Controls" would claim
                direct call rights — which we can't guarantee without per-
                function role-holder validation (see service/policy TODO). */}
            <span title="Computed from the recursive control graph — does not imply direct call rights on these contracts' privileged functions">
              Appears In Governance Path For ({controlledMachines.length})
            </span>
            {controlledMachines.length > 1 && (
              <div className="ps-search-arrows" style={{ marginLeft: 8 }}>
                <button onClick={() => {
                  const prev = (focusIdx - 1 + controlledMachines.length) % controlledMachines.length;
                  setFocusIdx(prev);
                  onFocusContract && onFocusContract(controlledMachines[prev].address);
                }}>◀</button>
                <span className="ps-search-counter">{focusIdx + 1} / {controlledMachines.length}</span>
                <button onClick={() => {
                  const next = (focusIdx + 1) % controlledMachines.length;
                  setFocusIdx(next);
                  onFocusContract && onFocusContract(controlledMachines[next].address);
                }}>▶</button>
              </div>
            )}
          </div>
          {controlledMachines.map((m, i) => (
            <div
              key={m.address}
              className={`ps-principal-controlled ps-principal-clickable${i === focusIdx ? " ps-principal-focused" : ""}`}
              onClick={() => {
                setFocusIdx(i);
                onFocusContract && onFocusContract(m.address);
              }}
            >
              <span className="ps-principal-controlled-name">{m.name || shortAddr(m.address)}</span>
              <span className="ps-principal-controlled-addr">{shortAddr(m.address)}</span>
              {m.total_usd ? <span className="ps-search-preview-value">{formatUsd(m.total_usd)}</span> : null}
              <span className="ps-principal-goto">→</span>
            </div>
          ))}
        </section>
      )}

      {/* Recent on-chain activity. Pulls the last 15 MonitoredEvent rows
       * for this address — the unified watcher writes one per
       * CallScheduled/CallExecuted, signer change, and Safe tx
       * execution it sees. Empty state explains what the user would
       * see once we get historical backfill working (#4c). */}
      {wantsActivity ? (
        <section className="ps-principal-section">
          <div className="ps-principal-section-hdr">Recent activity</div>
          {activityError ? (
            <div className="ps-inspector-empty">Activity lookup failed: {activityError}</div>
          ) : activity == null ? (
            <div className="ps-inspector-empty">Loading activity…</div>
          ) : activity.length === 0 ? (
            <div className="ps-inspector-empty">
              No on-chain activity recorded yet. The watcher captures events going forward from enrollment;
              historical events before then aren't backfilled yet.
            </div>
          ) : (
            <div className="ps-activity-list">
              {activity.map((evt) => {
                const label = EVENT_LABELS[evt.event_type] || evt.event_type.replace(/_/g, " ");
                const accent = EVENT_ACCENTS[evt.event_type] || "#94a3b8";
                const ago = formatEventAgo(evt.detected_at);
                const txShort = evt.tx_hash ? `${evt.tx_hash.slice(0, 10)}…${evt.tx_hash.slice(-4)}` : null;
                return (
                  <div key={evt.id} className="ps-activity-row">
                    <span className="ps-badge" style={{ "--badge-accent": accent }}>{label}</span>
                    {ago ? <span className="ps-activity-ago">{ago}</span> : null}
                    {evt.block_number ? <span className="ps-activity-block">block {evt.block_number.toLocaleString()}</span> : null}
                    {txShort ? <span className="ps-activity-tx mono">{txShort}</span> : null}
                  </div>
                );
              })}
            </div>
          )}
        </section>
      ) : null}
    </article>
  );
}

function Breadcrumbs({ items, onNavigate }) {
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


const ROLE_META = {
  value_handler: { label: "Value Handlers", color: "#6a9e94", defaultOn: true },
  token:         { label: "Tokens",         color: "#6a8a9e", defaultOn: true },
  governance:    { label: "Governance",     color: "#8a6a9e", defaultOn: true },
  bridge:        { label: "Bridges",        color: "#9e8a6a", defaultOn: true },
  factory:       { label: "Factories",      color: "#6a9e8a", defaultOn: true },
  utility:       { label: "Utilities",      color: "#7a7a7a", defaultOn: false },
};
const ALL_ROLES = Object.keys(ROLE_META);

function RoleFilterBar({ machines, enabledRoles, onToggle }) {
  const counts = useMemo(() => {
    const c = {};
    for (const r of ALL_ROLES) c[r] = 0;
    for (const m of machines) c[m.role] = (c[m.role] || 0) + 1;
    return c;
  }, [machines]);

  return (
    <div className="ps-role-bar">
      {ALL_ROLES.map((role) => {
        const meta = ROLE_META[role];
        const on = enabledRoles.has(role);
        return (
          <button
            key={role}
            type="button"
            className={`ps-role-chip${on ? " ps-role-chip-on" : ""}`}
            style={{ "--role-color": meta.color }}
            onClick={() => onToggle(role)}
          >
            <span className="ps-role-dot" />
            <span>{meta.label}</span>
            <span className="ps-role-count">{counts[role]}</span>
          </button>
        );
      })}
    </div>
  );
}

function ContractNode({ data }) {
  const m = data.machine;
  const roleColor = (ROLE_META[m.role] || ROLE_META.utility).color;
  return (
    <div
      className={`ps-node${data.selected ? " ps-node-selected" : ""}${data.focused ? " ps-node-focused" : ""}`}
      style={{ borderLeftColor: roleColor }}
      onClick={data.onSelect}
    >
      <Handle type="target" position={Position.Top} id="ctrl-in" className="ps-handle" />
      <Handle type="target" position={Position.Left} id="value-in" className="ps-handle" />
      <Handle type="source" position={Position.Right} id="value-out" className="ps-handle" />
      <Handle type="source" position={Position.Bottom} id="ctrl-out" className="ps-handle" />
      <div className="ps-node-header">
        <span className="ps-node-name">{m.name || shortAddr(m.address)}</span>
      </div>
      {m.capabilities && m.capabilities.length > 0 && (
        <div className="ps-node-caps">
          {m.capabilities.map((cap) => (
            <span key={cap} className="ps-node-cap">{cap}</span>
          ))}
        </div>
      )}
      {m.standards && m.standards.length > 0 && (
        <div className="ps-node-standards">{m.standards.join(" · ")}</div>
      )}
      <div className="ps-node-addr">{shortAddr(m.address)}</div>
      <div className="ps-node-role" style={{ color: roleColor }}>{(ROLE_META[m.role] || ROLE_META.utility).label.replace(/s$/, "")}</div>
      {m.total_usd ? <div className="ps-node-balance">{formatUsd(m.total_usd)}</div> : null}
    </div>
  );
}

const PRINCIPAL_COLORS = {
  safe: "#6a9e94",
  eoa: "#a09870",
  timelock: "#9a8a6e",
  proxy_admin: "#8880a0",
};

function PrincipalNode({ data }) {
  const p = data.principal;
  const color = PRINCIPAL_COLORS[p.type] || "#64748b";
  const owners = Array.isArray(p.details?.owners) ? p.details.owners : [];
  const threshold = p.details?.threshold;
  const delay = p.details?.delay;

  return (
    <div
      className={`ps-principal-node${data.focused ? " ps-node-focused" : ""}`}
      style={{ "--principal-color": color, cursor: data.onSelect ? "pointer" : "default" }}
      onClick={data.onSelect}
    >
      <Handle type="target" position={Position.Top} id="ctrl-in" className="ps-handle" />
      <Handle type="source" position={Position.Bottom} id="ctrl-out" className="ps-handle" />
      <div className="ps-principal-badge" style={{ background: color + "22", color }}>
        {p.type === "safe" && threshold ? `${threshold}/${owners.length} SAFE` : p.type.toUpperCase()}
      </div>
      <div className="ps-principal-addr">{shortAddr(p.address)}</div>
      {p.type === "timelock" && delay && (
        <div className="ps-principal-detail">
          {Number(delay) >= 86400 ? `${Math.round(Number(delay) / 86400)}d` : Number(delay) >= 3600 ? `${Math.round(Number(delay) / 3600)}h` : `${Math.round(Number(delay) / 60)}m`} delay
        </div>
      )}
      {p.type === "safe" && owners.length > 0 && (
        <div className="ps-principal-owners">
          {owners.map((o) => (
            <div key={o} className="ps-principal-owner">
              <span className="ps-owner-dot" />
              {shortAddr(o)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

const nodeTypes = { contract: ContractNode, principal: PrincipalNode };

const elk = new ELK();

function hierarchicalLayout(machines, edgePairs) {
  const n = machines.length;
  if (n === 0) return [];
  if (n === 1) return [{ x: 0, y: 0 }];

  // Build directed adjacency: from → Set<to> (controller → target)
  const addrToIdx = new Map();
  machines.forEach((m, i) => addrToIdx.set(m.address?.toLowerCase(), i));

  const children = new Map(); // idx → Set<idx>  (who this node controls)
  const parents = new Map();  // idx → Set<idx>  (who controls this node)
  for (let i = 0; i < n; i++) { children.set(i, new Set()); parents.set(i, new Set()); }

  for (const [from, to] of edgePairs) {
    const fi = addrToIdx.get(from);
    const ti = addrToIdx.get(to);
    if (fi !== undefined && ti !== undefined && fi !== ti) {
      children.get(fi).add(ti);
      parents.get(ti).add(fi);
    }
  }

  // Assign tiers via BFS from roots (nodes with no parents)
  const tier = new Array(n).fill(-1);
  const roots = [];
  for (let i = 0; i < n; i++) {
    if (parents.get(i).size === 0) roots.push(i);
  }
  // If no roots (cycles), pick the node with most children
  if (roots.length === 0) {
    let best = 0;
    for (let i = 1; i < n; i++) {
      if (children.get(i).size > children.get(best).size) best = i;
    }
    roots.push(best);
  }

  const queue = [...roots];
  for (const r of roots) tier[r] = 0;

  const MAX_TIER = 20;
  while (queue.length > 0) {
    const curr = queue.shift();
    const nextTier = tier[curr] + 1;
    if (nextTier > MAX_TIER) continue;
    for (const child of children.get(curr)) {
      if (tier[child] < nextTier) {
        tier[child] = nextTier;
        queue.push(child);
      }
    }
  }

  // Unconnected nodes get their own tier at the bottom
  const maxTier = Math.max(0, ...tier.filter((t) => t >= 0));
  for (let i = 0; i < n; i++) {
    if (tier[i] < 0) tier[i] = maxTier + 1;
  }

  // Group nodes by tier
  const tiers = new Map();
  for (let i = 0; i < n; i++) {
    if (!tiers.has(tier[i])) tiers.set(tier[i], []);
    tiers.get(tier[i]).push(i);
  }

  // Score each node by influence
  const outCount = new Array(n).fill(0);
  const inCount = new Array(n).fill(0);
  const hasEdge = new Set();
  for (const [from, to] of edgePairs) {
    const fi = addrToIdx.get(from);
    const ti = addrToIdx.get(to);
    if (fi !== undefined) { outCount[fi]++; hasEdge.add(fi); }
    if (ti !== undefined) { inCount[ti]++; hasEdge.add(ti); }
  }

  // Split connected vs isolated
  const connected = [];
  const isolated = [];
  for (let i = 0; i < n; i++) {
    if (hasEdge.has(i)) connected.push(i);
    else isolated.push(i);
  }

  // Rank connected by influence (more outgoing = higher)
  connected.sort((a, b) => {
    const sa = outCount[a] - inCount[a];
    const sb = outCount[b] - inCount[b];
    if (sb !== sa) return sb - sa;
    return outCount[b] - outCount[a];
  });

  const NODE_W = 250;
  const NODE_H = 160;
  // Scale columns based on node count — more nodes = wider layout
  const colCount = n <= 9 ? 3 : n <= 20 ? 4 : 5;
  const spread = NODE_W * 1.15;
  const positions = new Array(n);

  // Connected nodes: multi-column stagger, spreading wider as we go down
  for (let rank = 0; rank < connected.length; rank++) {
    const idx = connected[rank];
    const col = rank % colCount;
    const row = Math.floor(rank / colCount);
    const rowSpread = spread * (1 + row * 0.08);
    let x, y;
    y = row * NODE_H;
    // Spread columns evenly around center
    const colOffset = (col - (colCount - 1) / 2) * rowSpread;
    // Deterministic jitter (subtle)
    const jx = ((rank * 7 + 13) % 30 - 15);
    const jy = ((rank * 11 + 7) % 16 - 8);
    x = colOffset + jx;
    y += jy;
    positions[idx] = { x: Math.round(x), y: Math.round(y) };
  }

  // Isolated nodes: ellipse ring around the connected core
  if (isolated.length > 0) {
    const cxs = connected.map((i) => positions[i].x);
    const cys = connected.map((i) => positions[i].y);
    const cx = connected.length > 0 ? (Math.min(...cxs) + Math.max(...cxs)) / 2 : 0;
    const cy = connected.length > 0 ? (Math.min(...cys) + Math.max(...cys)) / 2 : 0;
    const rx = connected.length > 0 ? (Math.max(...cxs) - Math.min(...cxs)) / 2 + NODE_W * 1.5 : NODE_W * 2;
    const ry = connected.length > 0 ? (Math.max(...cys) - Math.min(...cys)) / 2 + NODE_H * 1.3 : NODE_H * 2;

    for (let i = 0; i < isolated.length; i++) {
      const angle = (2 * Math.PI * i) / isolated.length - Math.PI / 2;
      positions[isolated[i]] = {
        x: Math.round(cx + Math.cos(angle) * rx),
        y: Math.round(cy + Math.sin(angle) * ry),
      };
    }
  }

  return positions;
}

function buildGraphLayout(machines, fundFlows, principals) {
  const sorted = [...machines].sort((a, b) => b.totalFunctions - a.totalFunctions);
  const principalList = principals || [];

  // Layout contracts only — principals get positioned relative to what they control
  const contractEntities = sorted.map((m) => ({ address: m.address?.toLowerCase(), kind: "contract" }));

  // Collect contract-to-contract edge pairs
  const edgePairs = [];
  const byName = new Map();
  for (const m of sorted) {
    if (!m.name) continue;
    if (!byName.has(m.name)) byName.set(m.name, []);
    byName.get(m.name).push(m);
  }
  for (const [, group] of byName) {
    if (group.length < 2) continue;
    const proxy = group.find((g) => g.is_proxy);
    const impl = group.find((g) => !g.is_proxy);
    if (proxy && impl) edgePairs.push([proxy.address?.toLowerCase(), impl.address?.toLowerCase()]);
  }
  const contractAddrs = new Set(contractEntities.map((e) => e.address));
  const allAddrs = new Set([...contractAddrs, ...principalList.map((p) => p.address?.toLowerCase())]);
  for (const flow of fundFlows || []) {
    const from = flow.from?.toLowerCase();
    const to = flow.to?.toLowerCase();
    if (from && to && contractAddrs.has(from) && contractAddrs.has(to)) {
      edgePairs.push([from, to]);
    }
  }

  // Layout contracts
  const positions = hierarchicalLayout(contractEntities, edgePairs);
  const contractPositions = new Map();

  // Build contract nodes
  const nodes = sorted.map((m, i) => {
    const pos = positions[i] || { x: 0, y: 0 };
    contractPositions.set(m.address?.toLowerCase(), pos);
    return {
      id: m.address,
      type: "contract",
      position: pos,
      data: { machine: m },
    };
  });

  // Position principals near the contracts they control
  const PRINCIPAL_OFFSET_Y = -200; // above their contracts, with breathing room
  const usedPrincipalPositions = [];
  for (const p of principalList) {
    const controls = (p.controls || []).map((a) => a.toLowerCase());
    const controlledPositions = controls
      .map((a) => contractPositions.get(a))
      .filter(Boolean);

    let px, py;
    if (controlledPositions.length > 0) {
      // Center above controlled contracts
      px = controlledPositions.reduce((s, c) => s + c.x, 0) / controlledPositions.length;
      py = Math.min(...controlledPositions.map((c) => c.y)) + PRINCIPAL_OFFSET_Y;
    } else {
      px = 0;
      py = PRINCIPAL_OFFSET_Y;
    }

    // Avoid overlapping other principals and contracts
    for (const used of usedPrincipalPositions) {
      if (Math.abs(px - used.x) < 180 && Math.abs(py - used.y) < 100) {
        px += 200;
      }
    }
    const pos = { x: Math.round(px), y: Math.round(py) };
    usedPrincipalPositions.push(pos);

    nodes.push({
      id: p.address,
      type: "principal",
      position: pos,
      data: { principal: p },
    });
  }

  // Build edges from the same edge pairs used for layout
  const edges = [];
  for (const [, group] of byName) {
    if (group.length < 2) continue;
    const proxy = group.find((g) => g.is_proxy);
    const impl = group.find((g) => !g.is_proxy);
    if (proxy && impl) {
      edges.push({
        id: `${proxy.address}-${impl.address}`,
        source: proxy.address,
        target: impl.address,
        sourceHandle: "ctrl-out",
        targetHandle: "ctrl-in",
        type: "smoothstep",
        style: { stroke: "#334155", strokeWidth: 1 },
        animated: false,
      });
    }
  }

  // Fund flow / control edges with semantic handle routing
  const LANE_HANDLES = {
    control: { sourceHandle: "ctrl-out", targetHandle: "ctrl-in" },
    inflow:  { sourceHandle: "value-out", targetHandle: "value-in" },
    outflow: { sourceHandle: "value-out", targetHandle: "value-in" },
  };
  for (const flow of fundFlows || []) {
    const from = flow.from?.toLowerCase();
    const to = flow.to?.toLowerCase();
    if (!from || !to || !allAddrs.has(from) || !allAddrs.has(to)) continue;
    const edgeId = `flow-${from}-${to}`;
    if (edges.some((e) => e.id === edgeId)) continue;
    const isValue = flow.type === "controls_value";
    const handles = LANE_HANDLES[flow.lane || "control"] || LANE_HANDLES.control;
    edges.push({
      id: edgeId,
      source: from,
      target: to,
      sourceHandle: handles.sourceHandle,
      targetHandle: handles.targetHandle,
      type: "smoothstep",
      style: { stroke: isValue ? "#6a9e94" : "#475569", strokeWidth: isValue ? 1.5 : 1 },
      animated: true,
      data: { capabilities: (flow.capabilities || []).slice(0, 3), flowType: flow.type },
    });
  }

  return { nodes, edges };
}

async function elkLayout(machines, fundFlows, principals) {
  const { nodes: rawNodes, edges: rawEdges } = buildGraphLayout(machines, fundFlows, principals);

  // Build elk graph
  const elkGraph = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "DOWN",
      "elk.spacing.nodeNode": "60",
      "elk.layered.spacing.nodeNodeBetweenLayers": "80",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
      "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
      "elk.layered.edgeRouting": "ORTHOGONAL",
      "elk.spacing.edgeNode": "30",
      "elk.spacing.edgeEdge": "15",
      "elk.layered.spacing.edgeEdgeBetweenLayers": "15",
      "elk.layered.spacing.edgeNodeBetweenLayers": "30",
    },
    children: rawNodes.map((n) => ({
      id: n.id,
      width: n.type === "principal" ? 140 : 220,
      height: n.type === "principal" ? 60 : 120,
    })),
    edges: rawEdges.map((e) => ({
      id: e.id,
      sources: [e.source],
      targets: [e.target],
    })),
  };

  try {
    const layout = await elk.layout(elkGraph);
    const posMap = new Map();
    for (const child of layout.children || []) {
      posMap.set(child.id, { x: child.x || 0, y: child.y || 0 });
    }
    const laidOutNodes = rawNodes.map((n) => ({
      ...n,
      position: posMap.get(n.id) || n.position,
    }));
    return { nodes: laidOutNodes, edges: rawEdges };
  } catch {
    // Fallback to manual positions if elk fails
    return { nodes: rawNodes, edges: rawEdges };
  }
}

function PrincipalTourNav({ tour, onGo, onBack }) {
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

function FocusOnNode({ address, focusKey }) {
  const { setCenter, getNodes } = useReactFlow();
  const lastKey = useRef(null);
  useEffect(() => {
    if (!address || focusKey === lastKey.current) return;
    lastKey.current = focusKey;
    // Small delay to let ReactFlow finish rendering positions
    const timer = setTimeout(() => {
      const allNodes = getNodes();
      let node = allNodes.find((n) => n.id === address);
      if (!node) node = allNodes.find((n) => n.id?.toLowerCase() === address.toLowerCase());
      if (node) {
        const w = node.measured?.width || node.width || 220;
        const h = node.measured?.height || node.height || 120;
        const x = node.positionAbsolute?.x ?? node.position?.x ?? 0;
        const y = node.positionAbsolute?.y ?? node.position?.y ?? 0;
        setCenter(x + w / 2, y + h / 2, { zoom: 1.2, duration: 400 });
      }
    }, 100);
    return () => clearTimeout(timer);
  }, [address, focusKey, getNodes, setCenter]);
  return null;
}

function SurfaceCanvas({ machines, fundFlows, principals, selectedAddress, focusAddress, focusedAddress, highlightedAddresses, onSelectMachine, onSelectPrincipal, principalTour, onTourGo, onTourBack }) {
  const [initNodes, setInitNodes] = useState([]);
  const [initEdges, setInitEdges] = useState([]);

  // Run elk layout (async)
  useEffect(() => {
    let cancelled = false;
    elkLayout(machines, fundFlows, principals).then(({ nodes: n, edges: e }) => {
      if (!cancelled) {
        setInitNodes(n);
        setInitEdges(e);
      }
    });
    return () => { cancelled = true; };
  }, [machines, fundFlows, principals]);

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (!initNodes.length) return;
    const sel = selectedAddress?.toLowerCase();
    // Find all nodes connected to the selected node
    const connectedNodes = new Set();
    if (sel) {
      connectedNodes.add(sel);
      for (const e of initEdges) {
        const src = e.source?.toLowerCase();
        const tgt = e.target?.toLowerCase();
        if (src === sel) connectedNodes.add(tgt);
        if (tgt === sel) connectedNodes.add(src);
      }
    }

    // Audit-coverage highlight takes precedence when active: non-covered
    // nodes dim, covered ones get a green ring so the user sees exactly
    // which contracts an audit touched. Falls back to the connected-node
    // dimming when no audit is selected.
    const hiActive = highlightedAddresses && highlightedAddresses.size > 0;

    const foc = focusedAddress?.toLowerCase();
    setNodes(
      initNodes.map((n) => {
        const nid = n.id?.toLowerCase();
        const inAudit = hiActive && highlightedAddresses.has(nid);
        const dimmed = hiActive ? !inAudit : (sel && !connectedNodes.has(nid));
        const focused = foc && nid === foc;
        const style = dimmed
          ? { opacity: 0.2 }
          : inAudit
          ? { boxShadow: "0 0 0 2px #22c55e, 0 0 12px rgba(34,197,94,0.55)", borderRadius: 6 }
          : {};
        return {
          ...n,
          style,
          data: {
            ...n.data,
            selected: n.id === selectedAddress,
            focused,
            // Dispatch by node kind: contract nodes carry .machine, principal
            // nodes carry .principal. This lets a click on a Safe/Timelock/EOA
            // node open its detail panel instead of doing nothing.
            onSelect: n.data.principal
              ? () => onSelectPrincipal && onSelectPrincipal(n.data.principal)
              : () => onSelectMachine(n.data.machine),
          },
        };
      })
    );

    setEdges(
      initEdges.map((e) => {
        const src = e.source?.toLowerCase();
        const tgt = e.target?.toLowerCase();
        // When audit highlight is active, fade edges that touch a
        // non-covered endpoint so the covered subgraph reads clearly.
        const edgeInAudit = hiActive && highlightedAddresses.has(src) && highlightedAddresses.has(tgt);
        const related = hiActive ? edgeInAudit : (!sel || src === sel || tgt === sel);
        const showLabel = sel && related;
        const caps = e.data?.capabilities || [];
        const labelText = showLabel ? (caps.join(", ") || e.data?.flowType || "") : "";
        return {
          ...e,
          label: labelText,
          labelStyle: { fill: "#94a3b8", fontSize: 9 },
          labelBgStyle: labelText ? { fill: "#0f1218", fillOpacity: 0.85 } : undefined,
          labelBgPadding: labelText ? [4, 6] : undefined,
          style: {
            ...e.style,
            opacity: related ? 1 : 0.08,
            strokeWidth: related && sel ? 2 : (e.style?.strokeWidth || 1),
          },
          animated: related && e.animated,
        };
      })
    );
  }, [initNodes, initEdges, selectedAddress, focusedAddress, highlightedAddresses, onSelectMachine, onSelectPrincipal]);

  return (
    <div className="ps-canvas-wrap">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        onPaneClick={() => onSelectMachine(null)}
        fitView
        minZoom={0.2}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1e293b" gap={24} size={1} />
        <Controls showInteractive={false} />
        <FocusOnNode address={focusAddress?.address} focusKey={focusAddress?.key} />
        {principalTour && principalTour.principals.length > 1 && (
          <Panel position="top-right">
            <PrincipalTourNav tour={principalTour} onGo={onTourGo} onBack={onTourBack} />
          </Panel>
        )}
      </ReactFlow>
    </div>
  );
}

// Detail tab's empty state — when nothing is selected, show the same
// composite-score block + radar that the company hero uses, so the
// sidebar isn't a blank "click something" prompt. Falls back to a quiet
// stub if companyData hasn't loaded yet.
function DetailEmptyState({ companyName, companyData, coverageData, onExampleClick }) {
  if (!companyData) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Loading protocol overview…</div>
      </section>
    );
  }
  const { axes, composite, grade } = computeProtocolScore(
    companyData,
    coverageData,
  );
  return (
    <section className="ps-detail-empty">
      <div className="ps-detail-empty-hdr">{companyName}</div>
      <div className={`company-hero-score grade-${grade}`}>
        <span className="company-hero-score-value">{composite}</span>
        <span className="company-hero-score-unit">/ 100</span>
      </div>
      <div className="company-hero-score-label">Grade {grade.toUpperCase()}</div>
      <div className={`company-hero-grade-bar grade-${grade}`}>
        <div
          className="company-hero-grade-bar-fill"
          style={{ width: `${Math.max(4, composite)}%` }}
        />
      </div>
      <div className="ps-detail-empty-radar">
        <ProtocolRadar
          axes={axes}
          size={240}
          labelRadius={0.40}
          labelInsetX={64}
          labelInsetY={12}
          onExampleClick={onExampleClick}
        />
      </div>
      <div className="ps-detail-empty-hint">
        Click a contract or principal on the canvas for its detail.
      </div>
    </section>
  );
}

function SidebarTabs({ mode, onSetMode, auditCount, showDetail = true }) {
  return (
    <div className="ps-sidebar-tabs">
      {/* showDetail is on by default in both embedded and fullscreen
          modes — clicking a contract anywhere is expected to surface the
          function-lane view. Kept as an opt-out prop so a future caller
          that needs a chrome-only sidebar can still suppress the tab. */}
      {showDetail && (
        <button
          className={`ps-sidebar-tab ${mode === "detail" ? "active" : ""}`}
          onClick={() => onSetMode("detail")}
        >
          Detail
        </button>
      )}
      <button
        className={`ps-sidebar-tab ${mode === "agent" ? "active" : ""}`}
        onClick={() => onSetMode("agent")}
      >
        Agent
      </button>
      <button
        className={`ps-sidebar-tab ${mode === "audits" ? "active" : ""}`}
        onClick={() => onSetMode("audits")}
      >
        Audits{auditCount != null ? `(${auditCount})` : ""}
      </button>
      <button
        className={`ps-sidebar-tab ${mode === "monitoring" ? "active" : ""}`}
        onClick={() => onSetMode("monitoring")}
      >
        Monitor
      </button>
      <button
        className={`ps-sidebar-tab ${mode === "upgrades" ? "active" : ""}`}
        onClick={() => onSetMode("upgrades")}
      >
        Upgrades
      </button>
    </div>
  );
}

// Sidebar Upgrades tab. Two states:
//   - No machine selected: list proxies in this protocol with upgrade counts.
//     Click a row → focus that proxy on canvas (parent handles selection).
//   - Machine selected (proxy): lazy-fetch the analysis blob for that contract
//     (the per-contract upgrade_history isn't included in /api/company/{name},
//     so we go via /api/analyses/{job_id}) and render the existing
//     UpgradesPanel — same layout as the standalone /address/<addr>/upgrades
//     page so the per-impl audit cards (UpgradeAuditCard) appear identically.
function UpgradesSidebarPanel({ machine, companyName, machines, onSelect, cache, onCache }) {
  const [history, setHistory] = useState(null);
  const [deps, setDeps] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!machine || !machine.is_proxy || !machine.job_id) {
      setHistory(null);
      setDeps({});
      setLoading(false);
      setError(null);
      return;
    }
    const cached = cache && cache[machine.job_id];
    if (cached) {
      setHistory(cached.history || null);
      setDeps(cached.deps || {});
      setLoading(false);
      setError(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    setHistory(null);
    setDeps({});
    // Two lean artifact fetches in parallel instead of the multi-MB
    // /api/analyses/{job_id} blob — that endpoint merges every artifact
    // (contract_analysis, control_snapshot, effective_permissions, …) and
    // the Upgrades tab only needs upgrade_history + dependencies. Each
    // promise's failure is isolated: if `dependencies` errors out, the
    // timeline still renders with raw addresses instead of resolved
    // contract names.
    const jid = encodeURIComponent(machine.job_id);
    Promise.all([
      api(`/api/analyses/${jid}/artifact/upgrade_history`).catch(() => null),
      api(`/api/analyses/${jid}/artifact/dependencies`).catch(() => null),
    ])
      .then(([uhBody, depsBody]) => {
        if (cancelled) return;
        const h = (uhBody && typeof uhBody === "object") ? uhBody : null;
        const d = (depsBody && typeof depsBody === "object")
          // dependencies artifact body is shaped { dependencies: {...} }
          // when stored via the analysis pipeline; some legacy paths
          // store the inner map directly. Accept either.
          ? (depsBody.dependencies || depsBody)
          : {};
        setHistory(h);
        setDeps(d);
        if (onCache) onCache(machine.job_id, h, d);
      })
      .catch((e) => { if (!cancelled) setError(e.message || String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
    // cache/onCache deliberately omitted: we read on mount/selection change
    // only so a cache update from this very fetch doesn't retrigger the
    // effect.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [machine?.job_id, machine?.is_proxy]);

  if (!machine) {
    const proxies = (machines || []).filter((m) => m.is_proxy);
    if (proxies.length === 0) {
      return (
        <section className="ps-principal-section">
          <div className="ps-inspector-empty">No proxies in this protocol.</div>
        </section>
      );
    }
    // Resolve a count for the badge: prefer the server's value, else the
    // total_upgrades on a previously-fetched analysis blob (lazy-cached as
    // the user clicks into proxies). When neither is known, omit the chip
    // rather than rendering a misleading "0".
    function countFor(m) {
      if (typeof m.upgrade_count === "number" && m.upgrade_count > 0) return m.upgrade_count;
      const cached = cache && cache[m.job_id];
      const totalUpgrades = cached?.history?.total_upgrades;
      return typeof totalUpgrades === "number" ? totalUpgrades : null;
    }
    // Last-upgrade resolution: the server returns ISO timestamp; fall back
    // to the cached artifact's last_upgrade_block when timestamp is missing
    // (older proxies may have block numbers but no chain timestamp).
    function lastUpgradeFor(m) {
      if (m.last_upgrade_timestamp) {
        const d = new Date(m.last_upgrade_timestamp);
        if (!Number.isNaN(d.getTime())) {
          return { sortKey: d.getTime(), label: d.toLocaleDateString("en-US", { year: "numeric", month: "short" }) };
        }
      }
      if (typeof m.last_upgrade_block === "number" && m.last_upgrade_block > 0) {
        return { sortKey: m.last_upgrade_block, label: `block ${m.last_upgrade_block.toLocaleString()}` };
      }
      // Fall back to anything we cached during a per-proxy load.
      const cached = cache && cache[m.job_id];
      const targetAddr = (cached?.history?.target_address || m.address || "").toLowerCase();
      const proxy = cached?.history?.proxies?.[targetAddr];
      const block = proxy?.last_upgrade_block;
      if (typeof block === "number" && block > 0) {
        return { sortKey: block, label: `block ${block.toLocaleString()}` };
      }
      return null;
    }
    return (
      <section className="ps-principal-section">
        <div className="ps-upgrades-global-hint">Click a proxy to see its timeline + audit matches.</div>
        <div className="ps-upgrades-global">
          {proxies
            .slice()
            .sort((a, b) => {
              // Most-recently-upgraded first; proxies with no recency data
              // sink to the bottom and tiebreak alphabetically.
              const la = lastUpgradeFor(a);
              const lb = lastUpgradeFor(b);
              if (la && lb) return lb.sortKey - la.sortKey;
              if (la) return -1;
              if (lb) return 1;
              return String(a.name || "").localeCompare(String(b.name || ""));
            })
            .map((m) => {
              const count = countFor(m);
              const last = lastUpgradeFor(m);
              return (
                <button
                  key={m.address}
                  type="button"
                  className="ps-upgrades-row"
                  onClick={() => onSelect && onSelect(m)}
                >
                  <div className="ps-upgrades-row-name">
                    <span>{m.name || shortAddr(m.address)}</span>
                    <span className="ps-upgrades-row-arrow">→</span>
                  </div>
                  <div className="ps-upgrades-row-meta">
                    {count != null ? (
                      <span className="ps-badge" style={{ "--badge-accent": "#8b92a8" }}>
                        {count} upgrade{count === 1 ? "" : "s"}
                      </span>
                    ) : null}
                    {m.proxy_type ? (
                      <span className="ps-badge" style={{ "--badge-accent": "#9a8a6e" }}>{m.proxy_type}</span>
                    ) : null}
                    {last ? (
                      <span className="ps-badge" style={{ "--badge-accent": "var(--tone-ownership)" }}>
                        last {last.label}
                      </span>
                    ) : null}
                  </div>
                </button>
              );
            })}
        </div>
      </section>
    );
  }

  if (!machine.is_proxy) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">{machine.name || "This contract"} is not a proxy. No upgrade history.</div>
      </section>
    );
  }

  if (error) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">Failed to load upgrade history: {error}</div>
      </section>
    );
  }

  return (
    <div className="ps-upgrades-sidebar-body">
      <UpgradesPanel
        upgradeHistory={history}
        contractId={machine.contract_id}
        companyName={companyName}
        contractAddress={machine.address}
        contractName={machine.name}
        dependencies={deps}
        loading={loading}
      />
    </div>
  );
}

function monitoringChips(config) {
  const active = MONITOR_FLAGS.filter((flag) => {
    if (config?.[flag.key]) return true;
    return (flag.aliases || []).some((alias) => config?.[alias]);
  });
  if (!active.length) return <span className="ps-monitor-muted">none</span>;
  return active.map((flag) => (
    <span key={flag.key} className="ps-monitor-chip">{flag.label}</span>
  ));
}

function groupKeysFromConfig(config = {}) {
  return MONITOR_ALERT_GROUPS
    .filter((group) => group.flags.some((flag) => config?.[flag]))
    .map((group) => group.key);
}

function configFromGroupKeys(groupKeys) {
  const selected = new Set(groupKeys);
  const config = {};
  for (const group of MONITOR_ALERT_GROUPS) {
    for (const flag of group.flags) {
      config[flag] = selected.has(group.key);
    }
  }
  return config;
}

function eventTypesFromGroupKeys(groupKeys) {
  const selected = new Set(groupKeys);
  const out = [];
  for (const group of MONITOR_ALERT_GROUPS) {
    if (!selected.has(group.key)) continue;
    for (const eventType of group.eventTypes) {
      if (!out.includes(eventType)) out.push(eventType);
    }
  }
  return out;
}

function needsPollingFromGroupKeys(groupKeys) {
  const selected = new Set(groupKeys);
  return MONITOR_ALERT_GROUPS.some((group) => group.needsPolling && selected.has(group.key));
}

function subscriptionEventTypeSet(subscription) {
  const raw = subscription?.event_filter?.event_types;
  if (!Array.isArray(raw) || raw.length === 0) return null;
  return new Set(raw.map((eventType) => String(eventType).toLowerCase()));
}

function matchingWebhookCountForConfig(config, subscriptions = []) {
  if (!subscriptions.length) return 0;
  const eventTypes = eventTypesFromGroupKeys(groupKeysFromConfig(config))
    .map((eventType) => eventType.toLowerCase());
  return subscriptions.filter((subscription) => {
    const allowed = subscriptionEventTypeSet(subscription);
    if (!allowed) return true;
    return eventTypes.some((eventType) => allowed.has(eventType));
  }).length;
}

function contractTypeForMachine(machine) {
  if (machine?.is_proxy) return "proxy";
  if (machine?.is_pausable || machine?.capabilities?.includes("pause")) return "pausable";
  if (machine?.role === "governance") return "governance";
  return "regular";
}

function SurfaceMonitoringPanel({ companyData, machines, selectedMachine }) {
  const protocolId = companyData?.protocol_id;
  const [contracts, setContracts] = useState([]);
  const [subscriptions, setSubscriptions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [busyId, setBusyId] = useState(null);
  const [savingAlert, setSavingAlert] = useState(false);
  const [editorSessions, setEditorSessions] = useState([]);
  const [activeEditorKey, setActiveEditorKey] = useState(null);
  const [monitorQuery, setMonitorQuery] = useState("");
  const [monitorAlertFilters, setMonitorAlertFilters] = useState([]);
  const [monitorStatusFilter, setMonitorStatusFilter] = useState("active");
  const [monitorWebhookFilter, setMonitorWebhookFilter] = useState("any");

  const machineByAddress = useMemo(() => {
    const map = new Map();
    for (const machine of machines || []) {
      const address = machine.address?.toLowerCase();
      if (address) map.set(address, machine);
      const implementation = machine.implementation?.toLowerCase();
      if (implementation && !map.has(implementation)) {
        map.set(implementation, { ...machine, name: `${machine.name || shortAddr(machine.address)} impl`, address: machine.implementation });
      }
    }
    return map;
  }, [machines]);

  const contractByAddress = useMemo(() => {
    const map = new Map();
    for (const contract of contracts) {
      const address = contract.address?.toLowerCase();
      if (address) map.set(address, contract);
    }
    return map;
  }, [contracts]);

  const refresh = useCallback(async ({ quiet = false } = {}) => {
    if (!protocolId) return;
    if (!quiet) setLoading(true);
    setError(null);
    try {
      const [monitoring, subs] = await Promise.all([
        api(`/api/protocols/${protocolId}/monitoring`),
        api(`/api/protocols/${protocolId}/subscriptions`),
      ]);
      setContracts(Array.isArray(monitoring) ? monitoring : []);
      setSubscriptions(Array.isArray(subs) ? subs : []);
    } catch (err) {
      setError(err?.message || String(err));
    } finally {
      if (!quiet) setLoading(false);
    }
  }, [protocolId]);

  useEffect(() => {
    refresh();
    const timer = setInterval(() => refresh({ quiet: true }), 15000);
    return () => clearInterval(timer);
  }, [refresh]);

  if (!protocolId) {
    return (
      <section className="ps-principal-section">
        <div className="ps-inspector-empty">No protocol monitoring id is available.</div>
      </section>
    );
  }

  const monitoredAlerts = [...contracts]
    .sort((a, b) => {
      const aMachine = machineByAddress.get(a.address?.toLowerCase());
      const bMachine = machineByAddress.get(b.address?.toLowerCase());
      return String(aMachine?.name || a.address).localeCompare(String(bMachine?.name || b.address));
    });
  const activeAlerts = monitoredAlerts.filter((contract) => contract.is_active);
  const inactiveAlerts = monitoredAlerts.filter((contract) => !contract.is_active);
  const filteredMonitorAlerts = monitoredAlerts.filter((contract) => {
    const machine = machineByAddress.get(contract.address?.toLowerCase());
    const query = monitorQuery.trim().toLowerCase();
    const haystack = [
      machine?.name,
      contract.address,
      contract.chain,
      contract.contract_type,
    ].filter(Boolean).join(" ").toLowerCase();
    const statusMatches = (
      monitorStatusFilter === "all" ||
      (monitorStatusFilter === "active" && contract.is_active) ||
      (monitorStatusFilter === "inactive" && !contract.is_active)
    );
    const groups = groupKeysFromConfig(contract.monitoring_config);
    const webhookCount = matchingWebhookCountForConfig(contract.monitoring_config || {}, subscriptions);
    const alertsMatch = (
      monitorAlertFilters.length === 0 ||
      monitorAlertFilters.every((key) => groups.includes(key))
    );
    const webhookMatches = (
      monitorWebhookFilter === "any" ||
      (monitorWebhookFilter === "with" && webhookCount > 0) ||
      (monitorWebhookFilter === "without" && webhookCount === 0)
    );
    return statusMatches && alertsMatch && webhookMatches && (!query || haystack.includes(query));
  });
  const focusedAlert = selectedMachine?.address
    ? activeAlerts.find((contract) => contract.address?.toLowerCase() === selectedMachine.address.toLowerCase()) || null
    : null;
  const activeEditor = (
    editorSessions.find((session) => session.key === activeEditorKey && !session.minimized) ||
    editorSessions.find((session) => !session.minimized) ||
    null
  );
  const minimizedEditors = editorSessions.filter((session) => session.minimized);

  function openAlertEditor(machine = null, existingContract = null) {
    const target = machine || (existingContract?.address ? machineByAddress.get(existingContract.address.toLowerCase()) : null) || null;
    const matchedContract = target?.address ? contractByAddress.get(target.address.toLowerCase()) : null;
    const address = target?.address || existingContract?.address;
    const key = address?.toLowerCase();
    if (!key) return;
    const nextSession = {
      key,
      machine: target,
      contract: existingContract || (matchedContract?.is_active ? matchedContract : null),
      minimized: false,
    };
    setEditorSessions((prev) => [
      ...prev
        .filter((session) => session.key !== key)
        .map((session) => ({ ...session, minimized: true })),
      nextSession,
    ]);
    setActiveEditorKey(key);
  }

  function minimizeAlertEditor(key) {
    setEditorSessions((prev) => prev.map((session) => (
      session.key === key ? { ...session, minimized: true } : session
    )));
    setActiveEditorKey((current) => (current === key ? null : current));
  }

  function restoreAlertEditor(key) {
    setEditorSessions((prev) => prev.map((session) => (
      { ...session, minimized: session.key !== key }
    )));
    setActiveEditorKey(key);
  }

  function closeAlertEditor(key) {
    setEditorSessions((prev) => prev.filter((session) => session.key !== key));
    setActiveEditorKey((current) => (current === key ? null : current));
  }

  function toggleMonitorAlertFilter(key) {
    setMonitorAlertFilters((prev) => (
      prev.includes(key)
        ? prev.filter((value) => value !== key)
        : [...prev, key]
    ));
  }

  function cycleMonitorWebhookFilter() {
    setMonitorWebhookFilter((prev) => {
      if (prev === "any") return "with";
      if (prev === "with") return "without";
      return "any";
    });
  }

  async function patchContract(contract, patch) {
    setBusyId(contract.id);
    setError(null);
    try {
      await api(`/api/monitored-contracts/${contract.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });
      await refresh({ quiet: true });
    } catch (err) {
      setError(err?.message || String(err));
    } finally {
      setBusyId(null);
    }
  }

  async function saveAlert(draft) {
    if (!draft.address) return;
    const machine = machineByAddress.get(draft.address.toLowerCase());
    const existingContract = contractByAddress.get(draft.address.toLowerCase());
    const groupKeys = draft.groupKeys?.length ? draft.groupKeys : ["upgrades"];
    setSavingAlert(true);
    setError(null);
    try {
      await api(`/api/protocols/${protocolId}/monitoring`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          address: draft.address,
          chain: machine?.chain || existingContract?.chain || draft.chain || "ethereum",
          contract_type: existingContract?.contract_type || contractTypeForMachine(machine),
          monitoring_config: configFromGroupKeys(groupKeys),
          needs_polling: needsPollingFromGroupKeys(groupKeys),
          is_active: true,
        }),
      });

      if (draft.webhookMode === "new" && draft.webhookUrl?.trim()) {
        const eventTypes = eventTypesFromGroupKeys(groupKeys);
        await api(`/api/protocols/${protocolId}/subscribe`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            discord_webhook_url: draft.webhookUrl.trim(),
            label: draft.webhookLabel?.trim() || null,
            event_filter: eventTypes.length ? { event_types: eventTypes } : null,
          }),
        });
      }

      closeAlertEditor(draft.key || draft.address.toLowerCase());
      await refresh({ quiet: true });
    } catch (err) {
      setError(err?.message || String(err));
    } finally {
      setSavingAlert(false);
    }
  }

  return (
    <section className="ps-monitor-panel">
      <div className="ps-monitor-header">
        <div>
          <div className="ps-monitor-title">{selectedMachine ? "Contract alerts" : "Monitor alerts"}</div>
          <div className="ps-monitor-subtitle">
            {selectedMachine
              ? `${selectedMachine.name || shortAddr(selectedMachine.address)} · ${focusedAlert ? "alert active" : "no alert"}`
              : `${filteredMonitorAlerts.length}/${monitoredAlerts.length} shown · ${activeAlerts.length} active · ${inactiveAlerts.length} inactive`}
          </div>
        </div>
      </div>

      {error && <div className="ps-monitor-error">{error}</div>}
      {loading && <div className="ps-inspector-empty">Loading alerts...</div>}

      {selectedMachine ? (
        <FocusedContractAlerts
          machine={selectedMachine}
          alert={focusedAlert}
          subscriptions={subscriptions}
          busyId={busyId}
          onAdd={() => openAlertEditor(selectedMachine)}
          onEdit={(contract) => openAlertEditor(selectedMachine, contract)}
          onTurnOff={(contract) => patchContract(contract, { is_active: false })}
        />
      ) : (
        <>
          <MonitorAlertFilters
            query={monitorQuery}
            status={monitorStatusFilter}
            webhookStatus={monitorWebhookFilter}
            selectedGroups={monitorAlertFilters}
            onQueryChange={setMonitorQuery}
            onStatusChange={setMonitorStatusFilter}
            onCycleWebhookStatus={cycleMonitorWebhookFilter}
            onToggleGroup={toggleMonitorAlertFilter}
            onClearGroups={() => setMonitorAlertFilters([])}
          />
          <AlertsTable
            alerts={filteredMonitorAlerts}
            machineByAddress={machineByAddress}
            subscriptions={subscriptions}
            busyId={busyId}
            emptyLabel="No monitored alerts match these filters."
            onEdit={(contract) => openAlertEditor(null, contract)}
            onSetActive={(contract, isActive) => patchContract(contract, { is_active: isActive })}
          />
        </>
      )}

      {editorSessions.length ? createPortal(
        <aside className={`ps-monitor-side-menu${activeEditor ? " ps-monitor-side-menu-edit" : " ps-monitor-side-menu-minimized"}`}>
          {activeEditor ? (
            <MonitorAlertEditor
              key={activeEditor.key}
              sessionKey={activeEditor.key}
              subscriptions={subscriptions}
              initialMachine={activeEditor.machine}
              initialContract={activeEditor.contract}
              saving={savingAlert}
              onMinimize={() => minimizeAlertEditor(activeEditor.key)}
              onClose={() => closeAlertEditor(activeEditor.key)}
              onSave={saveAlert}
            />
          ) : null}
          {minimizedEditors.length ? (
            <MinimizedAlertEditors
              sessions={minimizedEditors}
              onRestore={restoreAlertEditor}
              onClose={closeAlertEditor}
              inline={Boolean(activeEditor)}
            />
          ) : null}
        </aside>,
        document.body,
      ) : null}
    </section>
  );
}

function FocusedContractAlerts({
  machine,
  alert,
  subscriptions,
  busyId,
  onAdd,
  onEdit,
  onTurnOff,
}) {
  const webhookCount = alert ? matchingWebhookCountForConfig(alert.monitoring_config || {}, subscriptions) : 0;
  return (
    <div className="ps-monitor-focus-card">
      <div className="ps-monitor-focus-top">
        <div className="ps-monitor-alert-main">
          <div className="ps-monitor-alert-name">{machine?.name || shortAddr(machine?.address)}</div>
          <div className="ps-monitor-alert-meta">
            <span>{shortAddr(machine?.address)}</span>
            <span>{alert ? (alert.needs_polling ? "polling" : "events") : "not monitored"}</span>
            <span>{webhookCount ? `${webhookCount} webhook${webhookCount === 1 ? "" : "s"}` : "no webhook"}</span>
          </div>
        </div>
        <div className="ps-monitor-alert-actions">
          {alert ? (
            <button type="button" className="ps-monitor-btn ps-monitor-btn-primary" onClick={() => onEdit(alert)}>
              Edit alert
            </button>
          ) : (
            <button type="button" className="ps-monitor-btn ps-monitor-btn-primary" onClick={onAdd}>
              Add alert
            </button>
          )}
          {alert ? (
            <button
              type="button"
              className="ps-monitor-btn"
              disabled={busyId === alert.id}
              onClick={() => onTurnOff(alert)}
            >
              Turn off
            </button>
          ) : null}
        </div>
      </div>

      {alert ? (
        <div className="ps-monitor-contract-watch">
          {monitoringChips(alert.monitoring_config || {})}
          <MonitorWebhookIndicator count={webhookCount} />
        </div>
      ) : (
        <div className="ps-inspector-empty">No active alert for this contract.</div>
      )}
    </div>
  );
}

function MonitorEventIcon({ kind }) {
  const common = {
    width: 13,
    height: 13,
    viewBox: "0 0 16 16",
    fill: "none",
    "aria-hidden": "true",
  };

  if (kind === "upgrades") {
    return (
      <svg {...common}>
        <path d="M8 12.5V3.5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
        <path d="M4.8 6.7L8 3.5L11.2 6.7" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M4 12.5H12" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "ownership") {
    return (
      <svg {...common}>
        <circle cx="8" cy="5" r="2.1" stroke="currentColor" strokeWidth="1.5" />
        <path d="M3.8 12.5C4.5 10.5 5.9 9.5 8 9.5C10.1 9.5 11.5 10.5 12.2 12.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "pause") {
    return (
      <svg {...common}>
        <rect x="4.4" y="3.5" width="2.2" height="9" rx="0.8" fill="currentColor" />
        <rect x="9.4" y="3.5" width="2.2" height="9" rx="0.8" fill="currentColor" />
      </svg>
    );
  }

  if (kind === "roles") {
    return (
      <svg {...common}>
        <circle cx="6" cy="5.8" r="1.8" stroke="currentColor" strokeWidth="1.4" />
        <circle cx="10.8" cy="5.2" r="1.5" stroke="currentColor" strokeWidth="1.4" />
        <path d="M3.2 12.2C3.7 10.6 4.8 9.7 6.3 9.7C7.8 9.7 8.9 10.6 9.4 12.2" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
        <path d="M9.7 10.8C10.2 10.1 10.9 9.8 11.7 9.8C12.5 9.8 13.1 10.2 13.5 11" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "signers") {
    return (
      <svg {...common}>
        <path d="M4 11.7L5.2 8.8L10.9 3.1C11.4 2.6 12.2 2.6 12.7 3.1C13.2 3.6 13.2 4.4 12.7 4.9L7 10.6L4 11.7Z" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round" />
        <path d="M9.9 4.1L11.7 5.9" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
        <path d="M3.5 13H12.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "timelock") {
    return <GuardGlyph kind="timelock" accent="currentColor" title="Timelock" />;
  }

  return (
    <svg {...common}>
      <circle cx="8" cy="8" r="5" stroke="currentColor" strokeWidth="1.4" />
      <path d="M8 5.2V8L10.1 10" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function MonitorEventIcons({ config }) {
  const groups = MONITOR_ALERT_GROUPS.filter((group) => groupKeysFromConfig(config).includes(group.key));
  if (!groups.length) return <span className="ps-monitor-muted">none</span>;
  return (
    <div className="ps-monitor-event-icons" aria-label="Alert types">
      {groups.map((group) => (
        <span
          key={group.key}
          className={`ps-monitor-event-icon ps-monitor-event-icon-${group.key}`}
          data-label={group.label}
          aria-label={group.label}
        >
          <MonitorEventIcon kind={group.key} />
        </span>
      ))}
    </div>
  );
}

function WebhookGlyph() {
  return (
    <svg width="13" height="13" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M5.2 10.8C3.8 10.8 2.7 9.7 2.7 8.3C2.7 6.9 3.8 5.8 5.2 5.8H6.4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <path d="M9.6 5.2H10.8C12.2 5.2 13.3 6.3 13.3 7.7C13.3 9.1 12.2 10.2 10.8 10.2H9.6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <path d="M6.1 8H9.9" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function MonitorWebhookIndicator({ count }) {
  const active = count > 0;
  return (
    <span
      className={`ps-monitor-webhook-indicator${active ? " active" : ""}`}
      data-label={active ? `${count} matching webhook${count === 1 ? "" : "s"}` : "No matching webhook"}
      aria-label={active ? `${count} matching webhook${count === 1 ? "" : "s"}` : "No matching webhook"}
    >
      <WebhookGlyph />
    </span>
  );
}

function MonitorAlertFilters({
  query,
  status,
  webhookStatus,
  selectedGroups,
  onQueryChange,
  onStatusChange,
  onCycleWebhookStatus,
  onToggleGroup,
  onClearGroups,
}) {
  const webhookLabel = webhookStatus === "with"
    ? "Only alerts with matching webhooks"
    : webhookStatus === "without"
      ? "Only alerts without matching webhooks"
      : "Webhook filter off";
  return (
    <div className="ps-monitor-filterbar">
      <input
        className="ps-monitor-filter-input"
        value={query}
        onChange={(event) => onQueryChange(event.target.value)}
        placeholder="Filter name or address"
        aria-label="Filter monitor alerts by name or address"
      />
      <div className="ps-monitor-status-filter" aria-label="Filter monitor alerts by status">
        {["all", "active", "inactive"].map((value) => (
          <button
            key={value}
            type="button"
            className={`ps-monitor-status-chip${status === value ? " active" : ""}`}
            onClick={() => onStatusChange(value)}
          >
            {value}
          </button>
        ))}
        <button
          type="button"
          className={`ps-monitor-webhook-cycle ps-monitor-webhook-cycle-${webhookStatus}`}
          data-label={webhookLabel}
          aria-label={webhookLabel}
          onClick={onCycleWebhookStatus}
        >
          <WebhookGlyph />
          {webhookStatus === "without" ? <span className="ps-monitor-webhook-x">×</span> : null}
        </button>
      </div>
      <div className="ps-monitor-type-filter-row" aria-label="Filter monitor alerts by alert type">
        {MONITOR_ALERT_GROUPS.map((group) => {
          const selected = selectedGroups.includes(group.key);
          return (
            <button
              key={group.key}
              type="button"
              className={`ps-monitor-type-filter${selected ? " active" : ""}`}
              onClick={() => onToggleGroup(group.key)}
              data-label={group.label}
              aria-label={`Filter by ${group.label}`}
            >
              <MonitorEventIcon kind={group.key} />
            </button>
          );
        })}
        {selectedGroups.length ? (
          <button type="button" className="ps-monitor-clear-filter" onClick={onClearGroups}>
            Clear
          </button>
        ) : null}
      </div>
    </div>
  );
}

function AlertsTable({
  alerts,
  machineByAddress,
  subscriptions,
  busyId,
  emptyLabel = "No active alerts.",
  onEdit,
  onSetActive,
}) {
  if (!alerts.length) {
    return <div className="ps-inspector-empty">{emptyLabel}</div>;
  }

  return (
    <div className="ps-monitor-alert-table">
      {alerts.map((contract) => {
        const machine = machineByAddress.get(contract.address?.toLowerCase());
        const webhookCount = matchingWebhookCountForConfig(contract.monitoring_config || {}, subscriptions);
        return (
          <div
            key={contract.id}
            className={`ps-monitor-table-row${contract.is_active ? "" : " inactive"}`}
            role="button"
            tabIndex={0}
            title={contract.address}
            onClick={() => onEdit(contract)}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onEdit(contract);
              }
            }}
          >
            <div className="ps-monitor-table-main">
              <div className="ps-monitor-table-name">{machine?.name || shortAddr(contract.address)}</div>
              <div className="ps-monitor-table-addr">{contract.address}</div>
            </div>
            <MonitorEventIcons config={contract.monitoring_config || {}} />
            <MonitorWebhookIndicator count={webhookCount} />
            <button
              type="button"
              className={`ps-monitor-link ps-monitor-status-toggle${contract.is_active ? " active" : ""}`}
              disabled={busyId === contract.id}
              title={contract.is_active ? "Monitoring is on. Click to turn off." : "Monitoring is off. Click to turn on."}
              onClick={(event) => {
                event.stopPropagation();
                onSetActive(contract, !contract.is_active);
              }}
            >
              {contract.is_active ? "On" : "Off"}
            </button>
          </div>
        );
      })}
    </div>
  );
}

function MinimizedAlertEditors({ sessions, onRestore, onClose, inline = false }) {
  return (
    <div className={`ps-monitor-minimized-stack${inline ? " ps-monitor-minimized-stack-inline" : ""}`}>
      {sessions.map((session) => {
        const address = session.machine?.address || session.contract?.address;
        const label = session.machine?.name || shortAddr(address);
        return (
          <div key={session.key} className="ps-monitor-minimized-item">
            <button
              type="button"
              className="ps-monitor-minimized-restore"
              onClick={() => onRestore(session.key)}
              title={`Restore ${label}`}
            >
              <span>Alert</span>
              <strong>{label}</strong>
            </button>
            <button
              type="button"
              className="ps-monitor-minimized-close"
              onClick={() => onClose(session.key)}
              aria-label={`Close minimized alert for ${label}`}
            >
              ×
            </button>
          </div>
        );
      })}
    </div>
  );
}

function MonitorAlertEditor({
  sessionKey,
  subscriptions,
  initialMachine,
  initialContract,
  saving,
  onMinimize,
  onClose,
  onSave,
}) {
  const address = initialMachine?.address || initialContract?.address || "";
  const initialGroups = initialContract
    ? groupKeysFromConfig(initialContract.monitoring_config)
    : ["upgrades", "ownership", "pause"];
  const [groupKeys, setGroupKeys] = useState(initialGroups.length ? initialGroups : ["upgrades"]);
  const [webhookMode, setWebhookMode] = useState(subscriptions.length ? "existing" : "new");
  const [webhookUrl, setWebhookUrl] = useState("");
  const [webhookLabel, setWebhookLabel] = useState("");

  function toggleGroup(key) {
    setGroupKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next.size ? [...next] : [key];
    });
  }

  const selectedContract = initialContract || null;
  const selectedMachine = initialMachine || null;
  const contractLabel = selectedMachine?.name || shortAddr(address);

  return (
    <div className="ps-monitor-editor" role="dialog" aria-modal="false">
      <form
        className="ps-monitor-editor-form"
        onSubmit={(event) => {
          event.preventDefault();
          onSave({
            key: sessionKey,
            address,
            chain: selectedMachine?.chain || selectedContract?.chain || "ethereum",
            groupKeys,
            webhookMode,
            webhookUrl,
            webhookLabel,
          });
        }}
      >
        <div className="ps-monitor-modal-header">
          <div>
            <div className="ps-monitor-title">{initialContract ? "Edit alert" : "Add alert"}</div>
            <div className="ps-monitor-subtitle">{contractLabel}</div>
          </div>
          <div className="ps-monitor-modal-header-actions">
            <button type="button" className="ps-monitor-icon-btn" onClick={onMinimize} aria-label="Minimize alert editor">-</button>
            <button type="button" className="ps-modal-close" onClick={onClose}>×</button>
          </div>
        </div>

        <div className="ps-monitor-target-card">
          <span>{contractLabel}</span>
          <strong title={address}>{shortAddr(address)}</strong>
        </div>

        <div className="ps-monitor-field">
          <span>Watch</span>
          <div className="ps-monitor-alert-grid">
            {MONITOR_ALERT_GROUPS.map((group) => {
              const selected = groupKeys.includes(group.key);
              return (
                <button
                  key={group.key}
                  type="button"
                  className={`ps-monitor-alert-choice${selected ? " active" : ""}`}
                  onClick={() => toggleGroup(group.key)}
                >
                  {group.label}
                </button>
              );
            })}
          </div>
        </div>

        <div className="ps-monitor-field">
          <span>Webhook</span>
          <div className="ps-monitor-webhook-choice">
            {subscriptions.length ? (
              <button
                type="button"
                className={`ps-monitor-alert-choice${webhookMode === "existing" ? " active" : ""}`}
                onClick={() => setWebhookMode("existing")}
              >
                Existing ({subscriptions.length})
              </button>
            ) : null}
            <button
              type="button"
              className={`ps-monitor-alert-choice${webhookMode === "new" ? " active" : ""}`}
              onClick={() => setWebhookMode("new")}
            >
              New webhook
            </button>
          </div>
          {webhookMode === "new" ? (
            <>
              <input
                className="ps-monitor-input"
                value={webhookUrl}
                onChange={(event) => setWebhookUrl(event.target.value)}
                placeholder="Discord webhook URL"
              />
              <input
                className="ps-monitor-input"
                value={webhookLabel}
                onChange={(event) => setWebhookLabel(event.target.value)}
                placeholder="Label"
              />
            </>
          ) : (
            <div className="ps-monitor-selected-webhooks">
              {subscriptions.map((sub) => (
                <span key={sub.id} className="ps-monitor-chip">
                  {sub.label || maskWebhook(sub.discord_webhook_url)}
                </span>
              ))}
            </div>
          )}
        </div>

        <div className="ps-monitor-modal-actions">
          <button type="button" className="ps-monitor-btn" onClick={onClose}>Cancel</button>
          <button
            type="submit"
            className="ps-monitor-btn ps-monitor-btn-primary"
            disabled={saving || !address}
          >
            {saving ? "Saving" : "Save alert"}
          </button>
        </div>
      </form>
    </div>
  );
}

function SelectedContractAuditCoverage({ machine, coverageData, onPickAudit }) {
  if (!machine || !coverageData) return null;
  const addresses = [machine.address, machine.implementation]
    .filter(Boolean)
    .map((address) => address.toLowerCase());
  const row = (coverageData.coverage || []).find((entry) =>
    addresses.includes(String(entry.address || "").toLowerCase())
  );
  const verifiedAudits = bytecodeVerifiedAudits(row?.audits);

  if (!row || verifiedAudits.length === 0) {
    return (
      <section className="ps-audits-contract-card">
        <div className="ps-audits-contract-top">
          <span>{machine.name || row?.contract_name || shortAddr(machine.address)}</span>
        </div>
        <div className="ps-audits-contract-addr">{row?.address || machine.address}</div>
        <div className="ps-audits-contract-note">No bytecode match</div>
      </section>
    );
  }

  return (
    <section className="ps-audits-contract-card">
      <div className="ps-audits-contract-top">
        <span>{machine.name || row.contract_name || shortAddr(machine.address)}</span>
        <span className="ps-monitor-muted">
          {verifiedAudits.length} bytecode match{verifiedAudits.length === 1 ? "" : "es"}
        </span>
      </div>
      <div className="ps-audits-contract-addr">{row.address}</div>
      <div className="ps-audits-contract-list">
        {verifiedAudits.map((audit) => {
          const matchMeta = MATCH_TYPE_META[audit.match_type] || MATCH_TYPE_META.direct;
          return (
            <button
              key={audit.audit_id}
              type="button"
              className="ps-audits-contract-row"
              onClick={() => onPickAudit?.(audit.audit_id)}
            >
              <div className="ps-audits-contract-row-main">
                <span>{audit.auditor || "Unknown"}</span>
                <span>{formatAuditDate(audit.date)}</span>
              </div>
              {audit.title && <div className="ps-audits-contract-row-title">{audit.title}</div>}
              <div className="ps-audits-contract-badges">
                <MetaBadge meta={matchMeta} />
                {audit.match_confidence && (
                  <span className="ps-badge" style={{ "--badge-accent": "#6b7590", fontSize: 10 }}>
                    {audit.match_confidence}
                  </span>
                )}
                {audit.equivalence_status && EQUIVALENCE_META[audit.equivalence_status] && (
                  <MetaBadge
                    meta={EQUIVALENCE_META[audit.equivalence_status]}
                    title={audit.equivalence_reason || ""}
                  />
                )}
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}

function AuditsListPanel({ coverageData, activeAuditId, onPickAudit, loading, error, machines, selectedMachine }) {
  const [readingAudit, setReadingAudit] = useState(null);
  if (loading) return <section className="ps-principal-section"><div className="ps-inspector-empty">Loading audits…</div></section>;
  if (error) return <section className="ps-principal-section"><div className="ps-inspector-empty">Failed: {error}</div></section>;
  if (!coverageData) return null;

  // Invert: audit_id → { audit, addresses: Set<lowercase>, shaByAddr: Map<addr, sha> }
  // Each coverage row has per-(contract, audit) match metadata — notably
  // matched_commit_sha (Tier 2). Capture it here so the modal can render
  // the SHA next to the contract without fetching per-row detail again.
  const byAudit = new Map();
  for (const entry of coverageData.coverage || []) {
    const addr = (entry.address || "").toLowerCase();
    if (!addr) continue;
    for (const a of entry.audits || []) {
      if (!isBytecodeVerifiedAudit(a)) continue;
      const id = a.audit_id;
      if (!byAudit.has(id)) {
        byAudit.set(id, { audit: a, addresses: new Set(), shaByAddr: new Map() });
      }
      const bucket = byAudit.get(id);
      bucket.addresses.add(addr);
      if (a.matched_commit_sha) {
        bucket.shaByAddr.set(addr, a.matched_commit_sha);
      }
    }
  }

  // Sort audits by date desc (nulls last), then id desc. Active audit is
  // displayed first via CSS ordering (to surface the coverage list).
  const entries = [...byAudit.values()].sort((x, y) => {
    const dx = x.audit.date || "";
    const dy = y.audit.date || "";
    if (dx !== dy) return dx < dy ? 1 : -1;
    return (y.audit.audit_id || 0) - (x.audit.audit_id || 0);
  });

  const activeEntry = activeAuditId != null
    ? entries.find((e) => e.audit.audit_id === activeAuditId)
    : null;

  // Resolve lowercase addresses → { name, address } using the machines map
  // so covered contracts are legible instead of just raw hex.
  const contractByAddr = new Map();
  if (Array.isArray(machines)) {
    for (const m of machines) {
      const a = (m.address || "").toLowerCase();
      if (a) contractByAddr.set(a, m);
    }
  }

  return (
    <>
      <section className="ps-audits-panel">
        <SelectedContractAuditCoverage
          machine={selectedMachine}
          coverageData={coverageData}
          onPickAudit={onPickAudit}
        />
        <div className="ps-audits-panel-hdr">Verified audits ({entries.length})</div>

        {activeEntry && (
          <div className="ps-audits-active-card">
            <div className="ps-audits-active-hdr">
              <span>Covered contracts</span>
              <button
                className="ps-audits-clear"
                onClick={() => onPickAudit(null)}
                title="Clear highlight"
              >
                ✕ clear
              </button>
            </div>
            <div className="ps-audits-covered-list">
              {[...activeEntry.addresses].sort().map((addr) => {
                const m = contractByAddr.get(addr);
                return (
                  <div key={addr} className="ps-audits-covered-row">
                    <span className="ps-audits-covered-name">{m?.name || "unknown"}</span>
                    <span className="ps-audits-covered-addr">{shortAddr(addr)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        <div className="ps-audits-list">
          {entries.length === 0 ? (
            <div className="ps-inspector-empty">None</div>
          ) : null}
          {entries.map(({ audit, addresses }) => {
            const isActive = activeAuditId === audit.audit_id;
            return (
              <div
                key={audit.audit_id}
                className={`ps-audits-row ${isActive ? "active" : ""}`}
              >
                <button
                  className="ps-audits-row-main"
                  onClick={() => onPickAudit(isActive ? null : audit.audit_id)}
                >
                  <div className="ps-audits-row-top">
                    <span className="ps-audits-row-auditor">{audit.auditor || "Unknown"}</span>
                    <span className="ps-audits-row-date">{formatAuditDate(audit.date)}</span>
                  </div>
                  {audit.title && <div className="ps-audits-row-title">{audit.title}</div>}
                  <div className="ps-audits-row-meta">
                    matches {addresses.size} contract{addresses.size === 1 ? "" : "s"}
                  </div>
                </button>
                <button
                  className="ps-audits-row-read"
                  onClick={() =>
                    setReadingAudit({
                      audit,
                      addresses,
                      shaByAddr: byAudit.get(audit.audit_id)?.shaByAddr || new Map(),
                    })
                  }
                  title="Read audit"
                >
                  Read ↗
                </button>
              </div>
            );
          })}
        </div>
      </section>
      {readingAudit && (
        <AuditReadModal
          audit={readingAudit.audit}
          addresses={readingAudit.addresses}
          shaByAddr={readingAudit.shaByAddr}
          machines={contractByAddr}
          onClose={() => setReadingAudit(null)}
        />
      )}
    </>
  );
}
function AuditReadModal({ audit, addresses, machines, shaByAddr, onClose }) {
  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(true);
  const [detailError, setDetailError] = useState(null);
  const [text, setText] = useState(null);
  const [textLoading, setTextLoading] = useState(false);
  const [textError, setTextError] = useState(null);
  // PDF embed failure → flip to text fallback.
  const [pdfFailed, setPdfFailed] = useState(false);
  // Which page to jump to in the iframe (null = default / first page).
  const [targetPage, setTargetPage] = useState(null);

  useEffect(() => {
    let cancelled = false;
    setDetail(null);
    setDetailLoading(true);
    setDetailError(null);
    setText(null);
    setPdfFailed(false);
    setTargetPage(null);
    fetch(`/api/audits/${encodeURIComponent(audit.audit_id)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then((d) => {
        if (!cancelled) { setDetail(d); setDetailLoading(false); }
      })
      .catch((e) => {
        if (!cancelled) { setDetailError(String(e.message || e)); setDetailLoading(false); }
      });
    return () => { cancelled = true; };
  }, [audit.audit_id]);

  // Close on Escape
  useEffect(() => {
    const onKey = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const sourceUrl = detail?.url || audit.url || audit.source_url || null;
  const rawPdfUrl = detail?.pdf_url || audit.pdf_url || null;
  const urlLooksLikePdf = !rawPdfUrl && typeof sourceUrl === "string" && sourceUrl.toLowerCase().endsWith(".pdf");
  // Point the iframe at our proxy — external hosts (e.g. GitHub raw
  // content) send X-Frame-Options: deny which blocks inline rendering.
  const pdfUrl = (rawPdfUrl || urlLooksLikePdf)
    ? `/api/audits/${encodeURIComponent(audit.audit_id)}/pdf`
    : null;
  const downloadUrl = rawPdfUrl || (urlLooksLikePdf ? sourceUrl : null) || sourceUrl;
  const showPdf = !!pdfUrl && !pdfFailed;
  const showText = !showPdf;

  // Always fetch text — needed both for the fallback view AND to build the
  // page index so clicking a covered contract can jump to its mention.
  // Depend only on audit.audit_id so re-renders from setting textLoading
  // don't cancel the fetch mid-flight.
  useEffect(() => {
    let cancelled = false;
    setTextLoading(true);
    setTextError(null);
    setText(null);
    fetch(`/api/audits/${encodeURIComponent(audit.audit_id)}/text`)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.text();
      })
      .then((t) => {
        if (!cancelled) { setText(t); setTextLoading(false); }
      })
      .catch((e) => {
        if (!cancelled) { setTextError(String(e.message || e)); setTextLoading(false); }
      });
    return () => { cancelled = true; };
  }, [audit.audit_id]);

  // Parse "--- page N ---" markers from the extracted text into a page index.
  // Shared by both the contract-mention lookup and the commit-SHA lookup.
  const lowerPages = useMemo(() => {
    if (!text) return [];
    const pages = [];
    const re = /--- page (\d+) ---/g;
    let m;
    let last = 0;
    let lastPage = null;
    while ((m = re.exec(text)) !== null) {
      if (lastPage != null) {
        pages.push({ page: lastPage, body: text.slice(last, m.index) });
      }
      lastPage = parseInt(m[1], 10);
      last = m.index + m[0].length;
    }
    if (lastPage != null) pages.push({ page: lastPage, body: text.slice(last) });
    return pages.map((p) => ({ page: p.page, body: p.body.toLowerCase() }));
  }, [text]);

  const mentionByAddress = useMemo(() => {
    const out = new Map();
    if (!lowerPages.length) return out;
    for (const addr of addresses) {
      const lower = addr.toLowerCase();
      const short6 = lower.slice(0, 10); // 0x + first 8 hex chars — covers most PDF abbreviations
      const m2 = machines.get ? machines.get(lower) : null;
      const name = m2?.name || null;
      const nameLower = name && name.length >= 5 ? name.toLowerCase() : null;
      let found = null;
      for (const p of lowerPages) {
        if (p.body.includes(lower)
            || p.body.includes(short6)
            || (nameLower && p.body.includes(nameLower))) {
          found = p.page;
          break;
        }
      }
      out.set(addr, found);
    }
    return out;
  }, [lowerPages, addresses, machines]);

  // For each SHA we care about (the per-contract matched_commit_sha), find
  // the first page it appears on. Audits embed the SHA in either full
  // (40-char) or abbreviated (7-char) form, so try both.
  const pageBySha = useMemo(() => {
    const out = new Map();
    if (!lowerPages.length || !shaByAddr || !shaByAddr.values) return out;
    const uniq = new Set();
    for (const s of shaByAddr.values()) if (s) uniq.add(String(s).toLowerCase());
    for (const sha of uniq) {
      const short = sha.slice(0, 7);
      let found = null;
      for (const p of lowerPages) {
        if (p.body.includes(sha) || p.body.includes(short)) {
          found = p.page;
          break;
        }
      }
      out.set(sha, found);
    }
    return out;
  }, [lowerPages, shaByAddr]);

  return (
    <div className="ps-audit-modal-backdrop" onClick={onClose}>
      <div className="ps-audit-modal" onClick={(e) => e.stopPropagation()}>
        <header className="ps-audit-modal-header">
          <div className="ps-audit-modal-header-left">
            <div className="ps-audit-modal-auditor">{audit.auditor || "Unknown auditor"}</div>
            <div className="ps-audit-modal-title">{audit.title || "Untitled audit"}</div>
            <div className="ps-audit-modal-meta">
              {formatAuditDate(audit.date)} · covers {addresses.size} contract{addresses.size === 1 ? "" : "s"}
            </div>
            <AuditCommitChips detail={detail} />
          </div>
          <div className="ps-audit-modal-actions">
            {sourceUrl && (
              <a
                className="ps-audit-modal-btn"
                href={sourceUrl}
                target="_blank"
                rel="noreferrer noopener"
              >
                Source ↗
              </a>
            )}
            {downloadUrl && (
              <a
                className="ps-audit-modal-btn primary"
                href={downloadUrl}
                target="_blank"
                rel="noreferrer noopener"
                download
              >
                Download
              </a>
            )}
            <button className="ps-audit-modal-btn" onClick={onClose} aria-label="Close">✕</button>
          </div>
        </header>
        <div className="ps-audit-modal-body">
          <aside className="ps-audit-modal-aside">
            <div className="ps-audit-modal-aside-hdr">Covered contracts</div>
            <div className="ps-audit-modal-aside-hint">
              {text && mentionByAddress.size
                ? "Click a contract to jump to where it's referenced."
                : textLoading
                  ? "Indexing references…"
                  : null}
            </div>
            <div className="ps-audit-modal-aside-list">
              {[...addresses].sort().map((addr) => {
                const m = machines.get ? machines.get(addr) : null;
                const page = mentionByAddress.get(addr);
                const hasJump = !!page && showPdf;
                const isActive = targetPage === page && hasJump;
                const Tag = hasJump ? "button" : "div";
                const sha = shaByAddr && shaByAddr.get ? shaByAddr.get(addr) : null;
                const shortSha = sha ? String(sha).slice(0, 7) : null;
                const repo = Array.isArray(detail?.referenced_repos) && detail.referenced_repos.length
                  ? detail.referenced_repos[0]
                  : null;
                return (
                  <Tag
                    key={addr}
                    className={`ps-audit-modal-aside-row ${hasJump ? "clickable" : ""} ${isActive ? "active" : ""}`}
                    onClick={hasJump ? () => setTargetPage(page) : undefined}
                    type={hasJump ? "button" : undefined}
                  >
                    <div className="ps-audit-modal-aside-row-main">
                      <div className="ps-audit-modal-aside-name">{m?.name || "unknown"}</div>
                      <div className="ps-audit-modal-aside-addr">{addr}</div>
                    </div>
                    <div className="ps-audit-modal-aside-badges">
                      {page ? (
                        <span className="ps-audit-modal-aside-page" title={`Mentioned on page ${page}`}>
                          p{page}
                        </span>
                      ) : text ? (
                        <span className="ps-audit-modal-aside-page dim" title="Not found in extracted text">—</span>
                      ) : null}
                      {shortSha && (() => {
                        const shaLower = String(sha).toLowerCase();
                        const shaPage = pageBySha.get(shaLower) ?? null;
                        const shaJumpable = !!shaPage && showPdf;
                        if (shaJumpable) {
                          // Clicking jumps the PDF to the page where the SHA
                          // is referenced (like the page badge, but for the
                          // commit mention instead of the contract mention).
                          return (
                            <button
                              type="button"
                              className="ps-audit-modal-aside-sha"
                              title={`Commit ${sha} — mentioned on page ${shaPage}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                setTargetPage(shaPage);
                              }}
                            >
                              {shortSha}
                            </button>
                          );
                        }
                        // Fallback: link to GitHub when the SHA isn't in the
                        // extracted text (or PDF isn't rendered).
                        if (repo) {
                          return (
                            <a
                              href={`https://github.com/${repo}/tree/${sha}`}
                              target="_blank"
                              rel="noreferrer noopener"
                              className="ps-audit-modal-aside-sha"
                              title={`Verified against commit ${sha} — open on GitHub`}
                              onClick={(e) => e.stopPropagation()}
                            >
                              {shortSha}
                            </a>
                          );
                        }
                        return (
                          <span
                            className="ps-audit-modal-aside-sha"
                            title={`Verified against commit ${sha}`}
                          >
                            {shortSha}
                          </span>
                        );
                      })()}
                    </div>
                  </Tag>
                );
              })}
            </div>
          </aside>
          <div className="ps-audit-modal-doc">
            {detailLoading && (
              <div className="ps-audit-modal-empty">Loading audit…</div>
            )}
            {!detailLoading && showPdf && (
              <iframe
                key={targetPage ?? "default"}
                className="ps-audit-modal-iframe"
                title="Audit PDF"
                src={`${pdfUrl}${targetPage ? `#page=${targetPage}` : ""}`}
                onError={() => setPdfFailed(true)}
              />
            )}
            {!detailLoading && !showPdf && (
              <>
                {textLoading && <div className="ps-audit-modal-empty">Loading audit text…</div>}
                {textError && <div className="ps-audit-modal-empty">Failed to load text: {textError}</div>}
                {text && <pre className="ps-audit-modal-pre">{text}</pre>}
                {!textLoading && !textError && !text && detail && !detail.has_text && (
                  <div className="ps-audit-modal-empty">
                    No extracted text available for this audit.
                    {sourceUrl && <> Open the <a href={sourceUrl} target="_blank" rel="noreferrer noopener">source</a> to read it.</>}
                  </div>
                )}
              </>
            )}
            {detailError && (
              <div className="ps-audit-modal-empty">Failed to load audit metadata: {detailError}</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// Dedupe a mix of 7-char + 40-char SHAs by keeping the full form when we
// have it and promoting any short form that doesn't have a longer twin.
function dedupeShas(list) {
  const fulls = new Map(); // prefix(7) → full sha
  const shorts = new Set();
  for (const raw of list || []) {
    const sha = String(raw || "").trim().toLowerCase();
    if (!/^[0-9a-f]+$/.test(sha)) continue;
    if (sha.length >= 20) fulls.set(sha.slice(0, 7), sha);
    else if (sha.length >= 4) shorts.add(sha);
  }
  const out = [...fulls.values()];
  for (const s of shorts) {
    if (!fulls.has(s.slice(0, 7))) out.push(s);
  }
  return out;
}

function AuditCommitChips({ detail, maxShown = 4 }) {
  if (!detail) return null;
  const classified = Array.isArray(detail.classified_commits) ? detail.classified_commits : [];
  const reviewedList = classified.filter((c) => c && c.label === "reviewed");
  let shas;
  if (reviewedList.length) {
    shas = dedupeShas(reviewedList.map((c) => c.sha));
  } else {
    shas = dedupeShas(detail.reviewed_commits || []);
  }
  if (!shas.length) return null;

  const repo = Array.isArray(detail.referenced_repos) && detail.referenced_repos.length
    ? detail.referenced_repos[0]
    : null;

  const shown = shas.slice(0, maxShown);
  const extra = shas.length - shown.length;

  return (
    <div className="ps-audit-modal-commits">
      <span className="ps-audit-modal-commits-label">reviewed</span>
      {shown.map((sha) => {
        const short = sha.slice(0, 7);
        const href = repo ? `https://github.com/${repo}/tree/${sha}` : null;
        if (href) {
          return (
            <a
              key={sha}
              className="ps-audit-modal-commit-chip"
              href={href}
              target="_blank"
              rel="noreferrer noopener"
              title={sha}
            >
              {short}
            </a>
          );
        }
        return (
          <span key={sha} className="ps-audit-modal-commit-chip" title={sha}>{short}</span>
        );
      })}
      {extra > 0 && (
        <span className="ps-audit-modal-commit-more">+{extra} more</span>
      )}
    </div>
  );
}

function DraggableSidebar({ children, flyout = null }) {
  const [width, setWidth] = useState(380);
  const [collapsed, setCollapsed] = useState(false);
  const [flyoutCollapsed, setFlyoutCollapsed] = useState(false);
  const dragging = useRef(false);
  const sidebarWidth = collapsed ? 44 : width;
  const showFlyout = !collapsed && flyout && !flyoutCollapsed;
  const showFlyoutRail = !collapsed && flyout && flyoutCollapsed;

  const onMouseDown = useCallback((e) => {
    if (collapsed) return;
    e.preventDefault();
    dragging.current = true;
    const startX = e.clientX;
    const startW = width;
    const onMove = (ev) => {
      if (!dragging.current) return;
      const newW = Math.max(280, Math.min(800, startW - (ev.clientX - startX)));
      setWidth(newW);
    };
    const onUp = () => {
      dragging.current = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [collapsed, width]);

  return (
    <>
      {showFlyout ? (
        <div className="ps-sidebar-flyout" style={{ right: sidebarWidth }}>
          <button
            type="button"
            className="ps-sidebar-flyout-toggle"
            onClick={() => setFlyoutCollapsed(true)}
            title="Minimize panel"
            aria-label="Minimize panel"
          >
            &lt;
          </button>
          {flyout}
        </div>
      ) : null}
      {showFlyoutRail ? (
        <button
          type="button"
          className="ps-sidebar-flyout-rail"
          style={{ right: sidebarWidth }}
          onClick={() => setFlyoutCollapsed(false)}
          title="Expand panel"
          aria-label="Expand panel"
        >
          &gt;
        </button>
      ) : null}
      <div
        className={`ps-sidebar${collapsed ? " ps-sidebar-collapsed" : ""}`}
        style={{
          width: sidebarWidth,
          minWidth: sidebarWidth,
          maxWidth: sidebarWidth,
          "--ps-sidebar-width": `${sidebarWidth}px`,
        }}
      >
        <div className="ps-sidebar-handle" onMouseDown={onMouseDown}>
          <button
            type="button"
            className="ps-sidebar-toggle ps-sidebar-toggle-collapse"
            onMouseDown={(e) => e.stopPropagation()}
            onClick={() => setCollapsed(true)}
            title="Minimize side panel"
            aria-label="Minimize side panel"
          >
            &gt;
          </button>
        </div>
        <div className="ps-sidebar-content">{children}</div>
        <div className="ps-sidebar-rail">
          <button
            type="button"
            className="ps-sidebar-rail-button"
            onClick={() => setCollapsed(false)}
            title="Expand side panel"
            aria-label="Expand side panel"
          >
            &lt;
          </button>
        </div>
      </div>
    </>
  );
}

function formatUsd(value) {
  if (!value || value < 0.01) return null;
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`;
  if (value >= 1e6) return `$${(value / 1e6).toFixed(1)}M`;
  if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`;
  return `$${value.toFixed(2)}`;
}

// ── Search Navigator ────────────────────────────────────────────────────────

const SEARCH_MODES = [
  { key: "all", icon: "⊕", label: "All", accent: "#94a3b8" },
  { key: "safe", icon: "🔒", label: "Safes", accent: "#6a9e94" },
  { key: "eoa", icon: "👤", label: "EOAs", accent: "#a09870" },
  { key: "timelock", icon: "⏳", label: "Timelocks", accent: "#9a8a6e" },
  { key: "funds", icon: "💰", label: "Has Funds", accent: "#f59e0b" },
];

const SORT_OPTIONS = [
  { key: "value", label: "Value ↓" },
  { key: "signers", label: "Signers ↓" },
  { key: "functions", label: "Functions ↓" },
  { key: "name", label: "Name A-Z" },
];

function buildSearchResults(machines, principals, mode, sortKey, query) {
  let items = [];

  if (mode === "safe" || mode === "eoa" || mode === "timelock") {
    // Show principals of this type
    const targetType = mode;
    for (const p of principals) {
      if (p.type !== targetType) continue;
      const controlled = (p.controls || []);
      const controlledMachines = machines.filter((m) =>
        controlled.some((a) => a.toLowerCase() === m.address?.toLowerCase())
      );
      const totalValue = controlledMachines.reduce((sum, m) => sum + (m.total_usd || 0), 0);
      const signers = p.details?.threshold || (p.details?.owners?.length) || 0;
      const delay = p.details?.delay || 0;
      items.push({
        kind: "principal",
        address: p.address,
        name: p.label || "",
        type: p.type,
        value: totalValue,
        signers,
        delay,
        functions: controlled.length,
        controlledMachines,
        // Select the first controlled contract when navigating to this principal
        machine: controlledMachines[0] || null,
        principal: p,
      });
    }
  } else {
    // Show contracts
    for (const m of machines) {
      const ownerPrincipal = principals.find((p) =>
        (p.controls || []).some((a) => a.toLowerCase() === m.address?.toLowerCase())
      );
      items.push({
        kind: "contract",
        address: m.address,
        name: m.name || "",
        type: ownerPrincipal?.type || "unknown",
        value: m.total_usd || 0,
        signers: ownerPrincipal?.details?.threshold || 0,
        delay: 0,
        functions: m.totalFunctions || 0,
        machine: m,
        principal: ownerPrincipal,
      });
    }
    if (mode === "funds") items = items.filter((i) => i.value > 0);
  }

  // Text query
  if (query) {
    const q = query.toLowerCase().trim();
    const minMatch = q.match(/(?:min(?:imum)?\s*)?value\s*(?:of\s*|>\s*|>=\s*)?\$?(\d+(?:\.\d+)?)\s*(m|k)?/i);
    if (minMatch) {
      let threshold = parseFloat(minMatch[1]);
      const unit = (minMatch[2] || "").toLowerCase();
      if (unit === "m") threshold *= 1e6;
      else if (unit === "k") threshold *= 1e3;
      items = items.filter((i) => i.value >= threshold);
    } else {
      items = items.filter((i) => {
        const haystack = [i.name, i.address, i.type].join(" ").toLowerCase();
        return haystack.includes(q);
      });
    }
  }

  // Sort
  if (sortKey === "value") items.sort((a, b) => b.value - a.value);
  else if (sortKey === "signers") items.sort((a, b) => b.signers - a.signers);
  else if (sortKey === "functions") items.sort((a, b) => b.functions - a.functions);
  else if (sortKey === "name") items.sort((a, b) => a.name.localeCompare(b.name));

  return items;
}

function SearchModesBar({ mode, setMode }) {
  return (
    <div className="ps-search-modes">
      {SEARCH_MODES.map((m) => (
        <button
          key={m.key}
          className={`ps-search-mode${mode === m.key ? " active" : ""}`}
          style={{ "--mode-accent": m.accent }}
          onClick={() => setMode(m.key)}
          title={m.label}
        >
          <span className="ps-search-mode-icon">{m.icon}</span>
          <span className="ps-search-mode-label">{m.label}</span>
        </button>
      ))}
    </div>
  );
}

function SearchNavigator({ machines, principals, onFocus, mode, setMode }) {
  const [sortKey, setSortKey] = useState("value");
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const [hasInteracted, setHasInteracted] = useState(false);

  const results = useMemo(
    () => buildSearchResults(machines, principals, mode, sortKey, query),
    [machines, principals, mode, sortKey, query]
  );

  // Reset index when results change
  useEffect(() => { setIndex(0); }, [results.length, mode, sortKey, query]);

  // Notify parent when the user drives the navigator. The initial preview
  // should not become a selected/focused contract by itself.
  useEffect(() => {
    if (!hasInteracted) return;
    if (results.length > 0 && results[index]) {
      onFocus(results[index]);
    } else {
      onFocus(null);
    }
  }, [hasInteracted, index, results]);

  const prev = () => {
    setHasInteracted(true);
    setIndex((i) => (i > 0 ? i - 1 : results.length - 1));
  };
  const next = () => {
    setHasInteracted(true);
    setIndex((i) => (i < results.length - 1 ? i + 1 : 0));
  };

  const current = results[index];

  return (
    <div className="ps-search-nav">
      {/* Mode pills (All / Safes / EOAs / Timelocks / Has Funds) now render
          at top-left via <SearchModesBar />. The rest of the search nav
          (input, sort, arrows, preview) stays in the centre overlay. */}
      <div className="ps-search-controls">
        <input
          type="text"
          value={query}
          onChange={(e) => {
            setHasInteracted(true);
            setQuery(e.target.value);
          }}
          placeholder="Search... (e.g. 'min value 3M')"
          className="ps-search-input"
        />
        <select
          value={sortKey}
          onChange={(e) => {
            setHasInteracted(true);
            setSortKey(e.target.value);
          }}
          className="ps-search-sort"
        >
          {SORT_OPTIONS.map((o) => <option key={o.key} value={o.key}>{o.label}</option>)}
        </select>
        <div className="ps-search-arrows">
          <button onClick={prev} disabled={results.length === 0} title="Previous">▲</button>
          <span className="ps-search-counter">
            {results.length > 0 ? `${index + 1} / ${results.length}` : "0"}
          </span>
          <button onClick={next} disabled={results.length === 0} title="Next">▼</button>
        </div>
      </div>
      {current && (
        <div className="ps-search-preview">
          <span className="ps-search-preview-name">{current.name || shortAddr(current.address)}</span>
          <span className="ps-search-preview-type">{current.type}</span>
          <span className="ps-search-preview-addr">{shortAddr(current.address)}</span>
          {current.value > 0 && <span className="ps-search-preview-value">{formatUsd(current.value)}</span>}
          {current.kind === "principal" && current.type === "safe" && current.signers > 0 && (
            <span className="ps-search-preview-meta">{current.signers}/{current.principal?.details?.owners?.length || "?"} signers</span>
          )}
          {current.kind === "principal" && current.type === "timelock" && current.delay > 0 && (
            <span className="ps-search-preview-meta">{formatDelay(current.delay)} delay</span>
          )}
          {current.kind === "principal" && (
            <span className="ps-search-preview-meta">controls {current.functions} contracts</span>
          )}
          {current.kind === "contract" && (
            <span className="ps-search-preview-meta">{current.functions} fns</span>
          )}
        </div>
      )}
    </div>
  );
}

export default function ProtocolSurface({ companyName, initialData = null, embedded = false }) {
  // initialData lets a parent (CompanyOverview) hand us the
  // /api/company/{name} payload it already fetched, so we don't fire a
  // second 1-3 MB request on mount. We still pull functions out of it
  // (they're embedded on each contract entry).
  const [companyData, setCompanyData] = useState(initialData);
  const initialFunctionData = useMemo(() => {
    if (!initialData?.contracts) return {};
    return Object.fromEntries(
      initialData.contracts.filter((c) => c.address).map((c) => [c.address, c.functions || []])
    );
  }, [initialData]);
  const [functionData, setFunctionData] = useState(initialFunctionData);
  const [selectedGuard, setSelectedGuard] = useState(null);
  const [selectedMachine, setSelectedMachine] = useState(null);
  const [selectedPrincipal, setSelectedPrincipal] = useState(null);
  const [radarExampleSelection, setRadarExampleSelection] = useState(null);
  const [suppressSearchFocus, setSuppressSearchFocus] = useState(() => (
    !embedded && Boolean(
      new URLSearchParams(window.location.search).get("score")
        || new URLSearchParams(window.location.search).get("scoreAxis")
        || sessionStorage.getItem("psat:surfaceRadarExample"),
    )
  ));
  // Search mode lives on the parent so the mode-pill bar can render at
  // top-left while the rest of SearchNavigator stays in the centre overlay.
  const [searchMode, setSearchMode] = useState("all");
  const [breadcrumbs, setBreadcrumbs] = useState([]);
  const [focusAddress, setFocusAddress] = useState(null);
  const [focusedAddress, setFocusedAddress] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    return params.get("focus") || null;
  });
  const focusKeyRef = useRef(0);
  const triggerFocus = useCallback((addr) => {
    focusKeyRef.current += 1;
    setFocusAddress({ address: addr, key: focusKeyRef.current });
    setFocusedAddress(addr || null);
    if (embedded) return;
    // Sync focus address to URL
    const url = new URL(window.location.href);
    if (addr) {
      url.searchParams.set("focus", addr);
      url.searchParams.delete("fn");
      url.searchParams.delete("score");
    } else {
      url.searchParams.delete("focus");
      url.searchParams.delete("fn");
      url.searchParams.delete("score");
    }
    window.history.replaceState({}, "", url.toString());
  }, [embedded]);
  // Multi-principal tour state: { principals: [...], index: 0, sourceContract: "0x...", sourceFunction: "fn" }
  const [principalTour, setPrincipalTour] = useState(null);
  const [error, setError] = useState(null);
  const [headerCollapsed, setHeaderCollapsed] = useState(true);
  const [dependencyGraphMachine, setDependencyGraphMachine] = useState(null);

  // Right sidebar mode: "detail" (default), "agent", "audits",
  // "monitoring", or "upgrades".
  // Default to Agent in both embedded and fullscreen views — the chat
  // interface is the most useful entry point on first load. A canvas
  // click switches to Detail in both modes (handlers below).
  const [sidebarMode, setSidebarMode] = useState("agent");
  // Per-proxy upgrade history cache, keyed by job_id. Server's
  // /api/company/{name} returns upgrade_count=null for protocols whose
  // chain monitor hasn't ingested events yet (the static-analysis blob in
  // /api/analyses/{job_id} has the real numbers). We populate this lazily
  // each time the user opens a proxy in the Upgrades tab so subsequent
  // visits skip the round-trip and the global proxy list can show real
  // counts for already-opened proxies.
  const [upgradeHistoryCache, setUpgradeHistoryCache] = useState({});
  const cacheUpgradeHistory = useCallback((jobId, history, deps) => {
    if (!jobId) return;
    setUpgradeHistoryCache((prev) => ({ ...prev, [jobId]: { history, deps } }));
  }, []);

  // Coverage payload — one call, cached locally. Used to build the audits
  // list + the audit_id → address-set map for highlight propagation.
  const [coverageData, setCoverageData] = useState(null);
  const [coverageError, setCoverageError] = useState(null);
  const [coverageLoading, setCoverageLoading] = useState(false);

  // Active audit: when non-null, its covered contracts get a green ring
  // and everything else dims on the canvas.
  const [activeAuditId, setActiveAuditId] = useState(null);

  // Admin-curated address → name map. Fetched once; edits are optimistic
  // against the local copy and persisted via the admin-gated PUT/DELETE.
  const [addressLabels, setAddressLabels] = useState(new Map());
  const refreshAddressLabels = useCallback(() => {
    listAddressLabels()
      .then((d) => {
        const m = new Map();
        for (const [addr, info] of Object.entries(d?.labels || {})) {
          m.set(String(addr).toLowerCase(), info.name);
        }
        setAddressLabels(m);
      })
      .catch(() => { /* labels are best-effort — keep whatever we had */ });
  }, []);
  useEffect(() => { refreshAddressLabels(); }, [refreshAddressLabels]);
  useEffect(() => {
    if (!companyName) return undefined;
    let cancelled = false;
    setCoverageLoading(true);
    setCoverageError(null);
    getCoverage(companyName)
      .then((d) => { if (!cancelled) { setCoverageData(d); setCoverageLoading(false); } })
      .catch((e) => { if (!cancelled) { setCoverageError(e?.message || "Failed"); setCoverageLoading(false); } });
    return () => { cancelled = true; };
  }, [companyName]);

  // Agent-emitted highlights: addresses the LLM mentioned in its last
  // answer, intersected server-side with the protocol's in-scope contracts.
  // Plain state so AgentPanel can replace it via setHighlightedAddresses.
  const [agentHighlights, setAgentHighlights] = useState(null);

  // Highlighted addresses on the canvas: union of agent highlights (Agent
  // tab) with the audit-coverage set (Audits tab). Either source can drive
  // the green ring. Lowercased Set so the canvas comparison is O(1); null
  // when neither source is active so the canvas falls back to selection-
  // dimming.
  const highlightedAddresses = useMemo(() => {
    const fromAudit = (() => {
      if (activeAuditId == null || !coverageData) return null;
      const out = new Set();
      for (const entry of coverageData.coverage || []) {
        const addr = (entry.address || "").toLowerCase();
        if (!addr) continue;
        if ((entry.audits || []).some((a) => a.audit_id === activeAuditId && isBytecodeVerifiedAudit(a))) {
          out.add(addr);
        }
      }
      return out;
    })();
    if (!fromAudit && !agentHighlights) return null;
    const merged = new Set();
    if (fromAudit) for (const a of fromAudit) merged.add(a);
    if (agentHighlights) for (const a of agentHighlights) merged.add(a);
    return merged.size ? merged : null;
  }, [activeAuditId, coverageData, agentHighlights]);

  const setHighlightedAddresses = setAgentHighlights;
  const [enabledRoles, setEnabledRoles] = useState(() => {
    const initial = new Set();
    for (const [role, meta] of Object.entries(ROLE_META)) {
      if (meta.defaultOn) initial.add(role);
    }
    return initial;
  });

  useEffect(() => {
    if (!companyName) return undefined;
    // Skip the fetch when the parent already handed us the payload —
    // the embedded surface in CompanyOverview reuses its parent's data,
    // which previously caused a duplicate 1-3 MB request.
    if (initialData) {
      setCompanyData(initialData);
      setError(null);
      setSelectedGuard(null);
      setRadarExampleSelection(null);
      return undefined;
    }
    let cancelled = false;

    async function load() {
      try {
        setError(null);
        setSelectedGuard(null);
        setRadarExampleSelection(null);
        const companyResponse = await fetch(`/api/company/${encodeURIComponent(companyName)}`);
        if (!companyResponse.ok) throw new Error("Failed to load company overview");
        const companyPayload = await companyResponse.json();
        if (cancelled) return;
        setCompanyData(companyPayload);

        // Functions are now included in the company response — no separate artifact fetches needed
        const permissionEntries = companyPayload.contracts
          .filter((c) => c.address)
          .map((c) => [c.address, c.functions || []]);

        if (cancelled) return;
        setFunctionData(Object.fromEntries(permissionEntries));
      } catch (err) {
        if (!cancelled) setError(err.message || "Failed to load surface");
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [companyName, initialData]);

  const allMachines = useMemo(
    () => (companyData ? buildMachines(companyData, functionData) : []),
    [companyData, functionData]
  );

  const machines = useMemo(
    () => allMachines.filter((m) => enabledRoles.has(m.role || "utility")),
    [allMachines, enabledRoles]
  );

  // Restore focus from URL on initial data load
  const restoredFocus = useRef(false);
  useEffect(() => {
    if (embedded || restoredFocus.current || !machines.length) return;
    const params = new URLSearchParams(window.location.search);
    const urlFocus = params.get("focus");
    if (params.get("score")) return;
    if (urlFocus) {
      restoredFocus.current = true;
      const machine = machines.find((m) => m.address?.toLowerCase() === urlFocus.toLowerCase());
      if (machine) {
        setSelectedMachine(machine);
        setSelectedPrincipal(null);
        setSelectedGuard(null);
        setRadarExampleSelection(null);
      }
      triggerFocus(urlFocus);
    }
  }, [embedded, machines, triggerFocus]);

  const handleToggleRole = useCallback((role) => {
    setEnabledRoles((prev) => {
      const next = new Set(prev);
      if (next.has(role)) next.delete(role);
      else next.add(role);
      return next;
    });
  }, []);

  const handleSelectMachine = useCallback((machine) => {
    setSelectedMachine(machine);
    setSelectedPrincipal(null);
    setSelectedGuard(null);
    setRadarExampleSelection(null);
    triggerFocus(machine?.address || null);
    // Clear any agent-emitted green-ring overlay when selection moves —
    // otherwise pane clicks (which call this with null) leave the
    // previous agent-highlighted address visually "focused".
    if (!machine) setAgentHighlights(null);
  }, [triggerFocus]);

  const handleSelectGuard = useCallback((fnView) => {
    setSelectedGuard(fnView);
    setRadarExampleSelection(null);
  }, []);

  const handleRadarExampleClick = useCallback((example) => {
    const targetAddress = example?.contractAddress?.toLowerCase();
    if (!targetAddress) return;
    const machine = allMachines.find((m) => m.address?.toLowerCase() === targetAddress);
    if (!machine) return;
    const fnView = findFunctionView(machine, example);
    setEnabledRoles((prev) => {
      const role = machine.role || "utility";
      if (prev.has(role)) return prev;
      const next = new Set(prev);
      next.add(role);
      return next;
    });
    setSidebarMode("detail");
    setSelectedMachine(machine);
    setSelectedPrincipal(null);
    setSelectedGuard(fnView || null);
    setRadarExampleSelection({
      contractAddress: machine.address,
      functionKey: fnView?.key || null,
    });
    setSuppressSearchFocus(false);
    triggerFocus(machine.address);
    const url = new URL(window.location.href);
    url.searchParams.set("focus", machine.address);
    url.searchParams.set("score", "1");
    if (fnView?.signature) url.searchParams.set("fn", fnView.signature);
    else url.searchParams.delete("fn");
    window.history.replaceState({}, "", url.toString());
  }, [allMachines, triggerFocus]);

  const restoredExampleSelection = useRef(false);
  useEffect(() => {
    if (embedded || restoredExampleSelection.current || !allMachines.length) return;
    const params = new URLSearchParams(window.location.search);
    const focus = params.get("focus");
    const fn = params.get("fn");
    let target = null;
    if (focus && params.get("score")) {
      target = { contractAddress: focus, functionSignature: fn || "", selector: fn || "" };
    } else if (window.location.pathname.endsWith("/surface")) {
      try {
        const pending = JSON.parse(sessionStorage.getItem("psat:surfaceRadarExample") || "null");
        if (pending?.companyName === companyName && pending?.contractAddress) {
          target = pending;
          sessionStorage.removeItem("psat:surfaceRadarExample");
        }
      } catch {
        sessionStorage.removeItem("psat:surfaceRadarExample");
      }
    }
    if (!target) return;
    const machine = allMachines.find((m) => m.address?.toLowerCase() === target.contractAddress.toLowerCase());
    if (!machine) return;
    restoredExampleSelection.current = true;
    handleRadarExampleClick({
      contractAddress: machine.address,
      functionSignature: target.functionSignature || "",
      selector: target.selector || "",
    });
  }, [allMachines, companyName, embedded, handleRadarExampleClick]);

  // Clicking a Safe/Timelock/EOA node on the canvas selects the principal
  // (opens the detail panel with signers / delay / controlled contracts)
  // and focuses it — same behaviour as clicking a single-principal guard
  // badge, just driven from the node itself.
  const handleSelectPrincipal = useCallback((principal) => {
    if (!principal) return;
    setSelectedPrincipal(principal);
    setSelectedMachine(null);
    setSelectedGuard(null);
    setRadarExampleSelection(null);
    setPrincipalTour(null);
    if (principal.address) triggerFocus(principal.address);
  }, [triggerFocus]);

  const visiblePrincipals = useMemo(() => {
    const visibleAddrs = new Set(machines.map((m) => m.address?.toLowerCase()));
    return (companyData?.principals || []).filter((p) =>
      !isRoleIdAddress(p.address || "") &&
      (p.controls || []).some((a) => visibleAddrs.has(a.toLowerCase()))
    );
  }, [machines, companyData]);

  const navigateToPrincipal = useCallback((target) => {
    let principal = visiblePrincipals.find((p) => p.address?.toLowerCase() === target.address?.toLowerCase());
    if (!principal) {
      principal = {
        address: target.address,
        type: target.type,
        label: target.label || target.type,
        details: target.details || {},
        controls: machines
          .filter((m) => m.owner?.toLowerCase() === target.address?.toLowerCase())
          .map((m) => m.address),
      };
    }
    setSelectedPrincipal(principal);
    setSelectedMachine(null);
    setSelectedGuard(null);
    setRadarExampleSelection(null);
    triggerFocus(target.address);
  }, [machines, visiblePrincipals, triggerFocus]);

  const handleNavigate = useCallback((target) => {
    // Push current view to breadcrumbs before navigating
    setBreadcrumbs((prev) => {
      const current = selectedPrincipal
        ? { type: selectedPrincipal.type, address: selectedPrincipal.address, label: selectedPrincipal.label }
        : selectedMachine
        ? { type: "contract", address: selectedMachine.address, label: selectedMachine.name }
        : null;
      return current ? [...prev, current] : prev;
    });

    const hasPrincipalTour = target._allPrincipals && target._allPrincipals.length > 1;
    if (hasPrincipalTour) {
      setPrincipalTour({
        principals: target._allPrincipals,
        index: 0,
        sourceContract: target._sourceContract,
        sourceFunction: target._sourceFunction,
      });
    } else {
      setPrincipalTour(null);
    }

    // Surface the navigation result in the Detail panel. Without this,
    // clicking a guard chip from the Agent tab silently mutates state
    // the user can't see — looks like "nothing happened" until they
    // manually click Detail. The chip click is an explicit drill-in
    // request, so swapping to Detail is the right behavior.
    setSidebarMode("detail");

    if (target.type === "contract") {
      const machine = machines.find((m) => m.address?.toLowerCase() === target.address?.toLowerCase());
      if (machine) {
        setSelectedMachine(machine);
        setSelectedPrincipal(null);
        setSelectedGuard(null);
        setRadarExampleSelection(null);
        triggerFocus(machine.address);
      }
    } else {
      navigateToPrincipal(target);
    }
  }, [machines, visiblePrincipals, selectedMachine, selectedPrincipal, triggerFocus, navigateToPrincipal]);

  const handleBreadcrumbNav = useCallback((item, index) => {
    // Truncate breadcrumbs to this point
    setBreadcrumbs((prev) => prev.slice(0, index));
    if (item.type === "contract") {
      const machine = machines.find((m) => m.address?.toLowerCase() === item.address?.toLowerCase());
      if (machine) { setSelectedMachine(machine); setSelectedPrincipal(null); setSelectedGuard(null); setRadarExampleSelection(null); }
    } else {
      const principal = visiblePrincipals.find((p) => p.address?.toLowerCase() === item.address?.toLowerCase());
      if (principal) { setSelectedPrincipal(principal); setSelectedMachine(null); setSelectedGuard(null); setRadarExampleSelection(null); }
    }
  }, [machines, visiblePrincipals]);

  const totals = useMemo(() => {
    return machines.reduce(
      (acc, machine) => {
        acc.contracts += 1;
        acc.functions += machine.totalFunctions;
        if (machine.total_usd) { acc.withBalance += 1; acc.totalUsd += machine.total_usd; }
        return acc;
      },
      { contracts: 0, functions: 0, withBalance: 0, totalUsd: 0 }
    );
  }, [machines]);

  if (error) return <p className="empty">Failed: {error}</p>;
  if (!companyData) return <p className="empty">Loading surface...</p>;

  const radarExampleFlyout = sidebarMode === "detail" && radarExampleSelection && selectedMachine && !selectedPrincipal ? (
    <div className="ps-sidebar-flyout-content">
      <ContractMachine
        key={`${selectedMachine.address}:radar`}
        machine={selectedMachine}
        onSelectGuard={handleSelectGuard}
        onNavigate={handleNavigate}
        companyName={companyName}
        highlightedFunctionKey={radarExampleSelection.functionKey}
        highlightedContract={!radarExampleSelection.functionKey}
        onOpenDependencyGraph={setDependencyGraphMachine}
      />
      <InspectorCard selected={selectedGuard} onNavigate={handleNavigate} />
    </div>
  ) : null;

  return (
    <div className="ps-surface ps-surface-fullscreen">
      {/* Overview strip (contracts / functions / with-funds) removed by
          request. The role filter toolbar below occupies this slot now. */}
      {false && (
      <div className={`ps-surface-overlay ${headerCollapsed ? "ps-surface-overlay-collapsed" : ""}`}>
        <button
          className="ps-surface-overlay-toggle"
          onClick={() => setHeaderCollapsed(!headerCollapsed)}
          title={headerCollapsed ? "Expand" : "Minimize"}
        >
          {headerCollapsed ? "\u25BC" : "\u25B2"}
        </button>
        {!headerCollapsed && (
          <div className="ps-surface-header">
            <div>
              <div className="ps-surface-eyebrow">Protocol Surface</div>
              <h2 className="ps-surface-title">{companyName}</h2>
              <p className="ps-surface-copy">
                Each contract shows control paths, operations, inflows, and outflows. Click any guard badge to inspect access control.
              </p>
            </div>
            <div className="ps-surface-stats">
              <div className="ps-surface-stat">
                <span>{totals.contracts}</span>
                <label>contracts</label>
              </div>
              <div className="ps-surface-stat">
                <span>{totals.functions}</span>
                <label>functions</label>
              </div>
              {totals.withBalance > 0 && (
                <div className="ps-surface-stat">
                  <span style={{ color: "#f59e0b" }}>{totals.withBalance}</span>
                  <label>with funds</label>
                </div>
              )}
              {totals.totalUsd > 0 && (
                <div className="ps-surface-stat">
                  <span style={{ color: "#f59e0b" }}>{formatUsd(totals.totalUsd)}</span>
                  <label>tracked value</label>
                </div>
              )}
              {companyData?.tvl?.defillama_tvl && (
                <div className="ps-surface-stat">
                  <span style={{ color: "#8b5cf6" }}>{formatUsd(companyData.tvl.defillama_tvl)}</span>
                  <label>protocol TVL</label>
                </div>
              )}
            </div>
          </div>
        )}
        {headerCollapsed && (
          <div className="ps-surface-header-mini">
            <span className="ps-surface-eyebrow" style={{ margin: 0 }}>{companyName}</span>
            <div className="ps-surface-stats">
              <div className="ps-surface-stat">
                <span>{totals.contracts}</span>
                <label>contracts</label>
              </div>
              <div className="ps-surface-stat">
                <span>{totals.functions}</span>
                <label>functions</label>
              </div>
              {totals.withBalance > 0 && (
                <div className="ps-surface-stat">
                  <span style={{ color: "#f59e0b" }}>{totals.withBalance}</span>
                  <label>with funds</label>
                </div>
              )}
              {totals.totalUsd > 0 && (
                <div className="ps-surface-stat">
                  <span style={{ color: "#f59e0b" }}>{formatUsd(totals.totalUsd)}</span>
                  <label>tracked value</label>
                </div>
              )}
              {companyData?.tvl?.defillama_tvl && (
                <div className="ps-surface-stat">
                  <span style={{ color: "#8b5cf6" }}>{formatUsd(companyData.tvl.defillama_tvl)}</span>
                  <label>protocol TVL</label>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
      )}

      {/* Role filter bar — now in the top-left slot where the overview strip used to live */}
      <div className="ps-surface-toolbar-overlay">
        <RoleFilterBar machines={allMachines} enabledRoles={enabledRoles} onToggle={handleToggleRole} />
      </div>

      {/* Search mode pills — top-left slot (where the overview used to be) */}
      <div className="ps-search-modes-overlay">
        <SearchModesBar mode={searchMode} setMode={setSearchMode} />
      </div>

      <div className="ps-surface-search-overlay">
        <SearchNavigator
        machines={machines}
        principals={visiblePrincipals}
        mode={searchMode}
        setMode={setSearchMode}
        onFocus={(item) => {
          if (suppressSearchFocus || radarExampleSelection) return;
          if (!item) {
            setSelectedMachine(null); setSelectedPrincipal(null);
            setRadarExampleSelection(null);
            setFocusedAddress(null);
            const url = new URL(window.location.href);
            url.searchParams.delete("focus");
            window.history.replaceState({}, "", url.toString());
            return;
          }
          setBreadcrumbs([]);
          if (item.kind === "principal" && item.principal) {
            setSelectedPrincipal(item.principal);
            setSelectedMachine(item.machine);
            setSelectedGuard(null);
            setRadarExampleSelection(null);
            // Focus on the principal node or its first controlled contract
            triggerFocus(item.address || item.machine?.address);
          } else if (item.machine) {
            setSelectedMachine(item.machine);
            setSelectedPrincipal(null);
            setSelectedGuard(null);
            setRadarExampleSelection(null);
            triggerFocus(item.machine.address);
          }
        }}
      />
      </div>

      <div className="ps-layout">
        <ReactFlowProvider>
          <SurfaceCanvas
            machines={machines}
            fundFlows={companyData?.fund_flows}
            principals={visiblePrincipals}
            selectedAddress={selectedMachine?.address || selectedPrincipal?.address}
            focusAddress={focusAddress}
            focusedAddress={focusedAddress}
            highlightedAddresses={highlightedAddresses}
            onSelectMachine={(m) => {
              // Auto-switch to Detail when the user clicks a contract
              // ON THE CANVAS so the function lanes are immediately
              // visible. Agent-link clicks go through
              // handleSelectMachine directly (not this wrapper), so
              // they don't trigger this and the user stays in the chat.
              if (m && sidebarMode !== "detail") setSidebarMode("detail");
              handleSelectMachine(m);
            }}
            onSelectPrincipal={(p) => {
              if (p && sidebarMode !== "detail") setSidebarMode("detail");
              handleSelectPrincipal(p);
            }}
            principalTour={principalTour}
            onTourGo={(nextIndex) => {
              const p = principalTour.principals[nextIndex];
              setPrincipalTour((prev) => ({ ...prev, index: nextIndex }));
              navigateToPrincipal({
                type: p.resolvedType || "unknown",
                address: p.address,
                label: p.label,
                details: p.details,
              });
            }}
            onTourBack={() => {
              setPrincipalTour(null);
              if (principalTour?.sourceContract) {
                const machine = machines.find((m) => m.address?.toLowerCase() === principalTour.sourceContract?.toLowerCase());
                if (machine) {
                  setSelectedMachine(machine);
                  setSelectedPrincipal(null);
                  setSelectedGuard(null);
                  setRadarExampleSelection(null);
                  triggerFocus(machine.address);
                }
              }
            }}
          />
        </ReactFlowProvider>
        <DraggableSidebar flyout={radarExampleFlyout}>
          <SidebarTabs
            mode={sidebarMode}
            onSetMode={setSidebarMode}
            auditCount={coverageData?.audit_count}
            showDetail
          />
          {sidebarMode === "audits" && (
            <AuditsListPanel
              coverageData={coverageData}
              activeAuditId={activeAuditId}
              onPickAudit={setActiveAuditId}
              loading={coverageLoading}
              error={coverageError}
              machines={machines}
              selectedMachine={selectedMachine}
            />
          )}
          {sidebarMode === "monitoring" && (
            <SurfaceMonitoringPanel
              companyData={companyData}
              machines={allMachines}
              selectedMachine={
                selectedMachine ||
                allMachines.find((m) => m.address?.toLowerCase() === focusedAddress?.toLowerCase()) ||
                null
              }
            />
          )}
          {sidebarMode === "upgrades" && (
            <UpgradesSidebarPanel
              machine={selectedMachine}
              companyName={companyName}
              machines={machines}
              onSelect={handleSelectMachine}
              cache={upgradeHistoryCache}
              onCache={cacheUpgradeHistory}
            />
          )}
          {sidebarMode === "detail" && (
            <Breadcrumbs items={breadcrumbs} onNavigate={handleBreadcrumbNav} />
          )}
          {sidebarMode === "detail" && !selectedPrincipal && (!selectedMachine || radarExampleSelection) && (
            <DetailEmptyState
              companyName={companyName}
              companyData={companyData}
              coverageData={coverageData}
              onExampleClick={handleRadarExampleClick}
            />
          )}
          {sidebarMode === "detail" && selectedPrincipal && (
            <PrincipalDetail
              key={selectedPrincipal.address}
              principal={selectedPrincipal}
              machines={machines}
              onNavigate={handleNavigate}
              onFocusContract={(addr) => triggerFocus(addr)}
              addressLabels={addressLabels}
              refreshAddressLabels={refreshAddressLabels}
            />
          )}
          {sidebarMode === "detail" && selectedMachine && !selectedPrincipal && !radarExampleSelection && (
            <ContractMachine
              key={selectedMachine.address}
              machine={selectedMachine}
              onSelectGuard={handleSelectGuard}
              onNavigate={handleNavigate}
              companyName={companyName}
              highlightedFunctionKey={radarExampleSelection?.functionKey}
              onOpenDependencyGraph={setDependencyGraphMachine}
            />
          )}
          {sidebarMode === "detail" && !selectedPrincipal && !radarExampleSelection && (
            <InspectorCard selected={selectedGuard} onNavigate={handleNavigate} />
          )}
          {sidebarMode === "agent" && (
            <AgentPanel
              companyName={companyName}
              selectedMachine={selectedMachine}
              onHighlight={setHighlightedAddresses}
              onFocusAddress={(addr) => {
                // Route through the same selection handlers a canvas
                // click uses so we get the connected-edges-stay-bright
                // dim behavior for free.
                const lc = addr.toLowerCase();
                const machine = machines.find(
                  (m) => (m.address || "").toLowerCase() === lc,
                );
                if (machine) {
                  handleSelectMachine(machine);
                  return;
                }
                const principal = visiblePrincipals.find(
                  (p) => (p.address || "").toLowerCase() === lc,
                );
                if (principal) {
                  handleSelectPrincipal(principal);
                  return;
                }
                // Out-of-scope address (typical: an EOA that's a Safe
                // owner / role holder but not itself a canvas node).
                // Fetch its "touch radius" — every contract it has
                // function-level authority over — and write that set
                // into highlightedAddresses. The canvas's existing
                // audit-overlay dim path then dims everything else.
                triggerFocus(addr);
                api(
                  `/api/agent/address-touches?company=${encodeURIComponent(companyName)}&address=${encodeURIComponent(addr)}`,
                )
                  .then((data) => {
                    const set = new Set([lc]);
                    for (const t of data?.touches || []) {
                      if (t.address) set.add(t.address.toLowerCase());
                    }
                    setHighlightedAddresses(set);
                  })
                  .catch(() => {
                    // Network/auth error — at least light up the focus
                    // target so the click isn't a no-op.
                    setHighlightedAddresses(new Set([lc]));
                  });
              }}
            />
          )}
        </DraggableSidebar>
      </div>
      <DependencyGraphModal
        machine={dependencyGraphMachine}
        onClose={() => setDependencyGraphMachine(null)}
      />
    </div>
  );
}
