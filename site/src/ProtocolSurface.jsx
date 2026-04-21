import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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

import hourglassIcon from "./assets/hourglass-empty.svg";
import questionMarkIcon from "./assets/question-mark.svg";
import vaultIcon from "./assets/vault.svg";

import { getCoverage, getTimeline } from "./api/audits.js";

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

function GuardGlyph({ kind, accent, title }) {
  const common = {
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    fill: "none",
    xmlns: "http://www.w3.org/2000/svg",
    "aria-hidden": "true",
  };

  if (kind === "unknown") {
    return (
      <span
        className="ps-guard-svg-mask"
        style={{ "--guard-icon-accent": accent, maskImage: `url(${questionMarkIcon})` }}
        title={title}
      />
    );
  }

  if (kind === "safe") {
    return (
      <span
        className="ps-guard-svg-mask"
        style={{ "--guard-icon-accent": accent, maskImage: `url(${vaultIcon})` }}
        title={title}
      />
    );
  }

  if (kind === "timelock") {
    return (
      <span
        className="ps-guard-svg-mask"
        style={{ "--guard-icon-accent": accent, maskImage: `url(${hourglassIcon})` }}
        title={title}
      />
    );
  }

  if (kind === "eoa") {
    return (
      <svg {...common}>
        <circle cx="8" cy="5.3" r="2.2" stroke={accent} strokeWidth="1.4" fill={`${accent}18`} />
        <path d="M4.2 12.4C4.8 10.5 6.2 9.5 8 9.5C9.8 9.5 11.2 10.5 11.8 12.4" stroke={accent} strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "contract" || kind === "proxy_admin") {
    return (
      <svg {...common}>
        <rect x="2.6" y="3" width="10.8" height="10" rx="1.8" stroke={accent} strokeWidth="1.4" fill={`${accent}16`} />
        <path d="M5.3 5.4H10.7M5.3 8H10.7M5.3 10.6H8.8" stroke={accent} strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "open") {
    return (
      <svg {...common}>
        <rect x="3.2" y="7.2" width="9.6" height="5.8" rx="1.6" stroke={accent} strokeWidth="1.4" fill={`${accent}16`} />
        <path d="M5.4 7.2V5.8C5.4 4.2 6.7 3 8.2 3C9.2 3 10 3.5 10.5 4.2" stroke={accent} strokeWidth="1.4" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "many") {
    return (
      <svg {...common}>
        <circle cx="5.7" cy="6.2" r="2" stroke={accent} strokeWidth="1.3" fill={`${accent}18`} />
        <circle cx="10.5" cy="5.6" r="1.8" stroke={accent} strokeWidth="1.3" fill={`${accent}10`} />
        <path d="M3.7 12.2C4.2 10.8 5.2 10.1 6.5 10.1C7.8 10.1 8.8 10.8 9.3 12.2" stroke={accent} strokeWidth="1.3" strokeLinecap="round" />
        <path d="M9.4 11.4C9.8 10.5 10.5 10 11.4 10" stroke={accent} strokeWidth="1.3" strokeLinecap="round" />
      </svg>
    );
  }

  return (
    <span
      className="ps-guard-svg-mask"
      style={{ "--guard-icon-accent": accent, maskImage: `url(${questionMarkIcon})` }}
      title={title}
    />
  );
}

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

function collectPrincipals(fn, companyData) {
  const byAddress = new Map();

  // Build a graph of who controls whom from per-contract control graphs
  // controllerOf: address → [{address, type, label, details}]
  const controllerOf = new Map();
  const nodeInfo = new Map();

  if (companyData) {
    for (const contract of companyData.contracts || []) {
      const cg = contract.control_graph;
      if (!cg) continue;
      for (const node of cg.nodes || []) {
        const addr = (node.address || "").toLowerCase();
        if (addr) nodeInfo.set(addr, node);
      }
      for (const edge of cg.edges || []) {
        if (edge.relation === "safe_owner") continue; // owners are nested inside their Safe
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
  }

  // Walk the control graph to find the first non-contract controller
  function resolveController(address, visited) {
    if (!visited) visited = new Set();
    if (visited.has(address)) return null;
    visited.add(address);
    const node = nodeInfo.get(address);
    if (!node) return null;
    // If this node is not a contract, it's the real controller
    if (node.type !== "contract") return node;
    // Walk outgoing edges to find who this contract delegates to
    const edges = controllerOf.get(address);
    if (!edges || edges.length === 0) return null;
    for (const edge of edges) {
      const resolved = resolveController(edge.to, visited);
      if (resolved) return resolved;
    }
    return null;
  }

  function pushPrincipal(principal, origin) {
    const address = String(principal?.address || "").toLowerCase();
    if (!address.startsWith("0x")) return;
    if (isRoleIdAddress(address)) return;

    // Resolve contract principals through the control graph
    if (principal.resolved_type === "contract") {
      const resolved = resolveController(address);
      if (resolved) {
        const rAddr = (resolved.address || "").toLowerCase();
        if (rAddr && !isRoleIdAddress(rAddr)) {
          if (byAddress.has(rAddr)) {
            byAddress.get(rAddr).origins.push(origin);
          } else {
            byAddress.set(rAddr, {
              address: rAddr,
              resolvedType: String(resolved.type || "unknown"),
              details: resolved.details && typeof resolved.details === "object" ? { ...resolved.details } : {},
              sourceContract: address,
              sourceControllerId: principal.source_controller_id || null,
              origins: [origin],
            });
          }
          return;
        }
      }
    }

    const existing = byAddress.get(address);
    if (existing) {
      existing.origins.push(origin);
      return;
    }
    byAddress.set(address, {
      address,
      resolvedType: String(principal.resolved_type || "unknown"),
      details: principal.details && typeof principal.details === "object" ? { ...principal.details } : {},
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
      pushPrincipal(principal, `authority role ${roleGrant.role}`);
    }
  }

  for (const controller of fn.controllers || []) {
    const label = controller.label || controller.controller_id || "controller";
    for (const principal of controller.principals || []) {
      pushPrincipal(principal, label);
    }
  }

  return [...byAddress.values()].sort((left, right) => left.address.localeCompare(right.address));
}

function guardSummary(fn, companyData) {
  const principals = collectPrincipals(fn, companyData);

  if (!principals.length) {
    const meta = TYPE_META[fn.authority_public ? "open" : "unknown"];
    return {
      kind: fn.authority_public ? "open" : "unknown",
      label: meta.label,
      sublabel: fn.authority_public ? "public" : "unresolved",
      accent: meta.accent,
      principals,
    };
  }

  if (principals.length > 1) {
    return {
      kind: "many",
      label: `${principals.length}P`,
      sublabel: "mixed",
      accent: TYPE_META.many.accent,
      principals,
    };
  }

  const principal = principals[0];
  const type = TYPE_META[principal.resolvedType] || TYPE_META.unknown;
  const safeOwners = Array.isArray(principal.details?.owners) ? principal.details.owners.length : 0;
  const threshold = Number(principal.details?.threshold);
  const delay = formatDelay(principal.details?.delay);

  let sublabel = shortAddr(principal.address);
  if (principal.resolvedType === "safe" && safeOwners) {
    sublabel = Number.isFinite(threshold) && threshold > 0 ? `${threshold}/${safeOwners}` : `${safeOwners} sig`;
  } else if (principal.resolvedType === "timelock" && delay) {
    sublabel = delay;
  }

  return {
      kind: principal.resolvedType,
      label: type.label,
      sublabel,
      accent: type.accent,
    principals,
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
          principals: collectPrincipals(fn, companyData),
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

function GuardButton({ fnView, onSelect, onNavigate }) {
  const kind = fnView.guard.kind;
  const principals = fnView.guard.principals || [];
  const isNavigable = onNavigate && principals.length > 0
    && kind !== "unknown" && kind !== "open";

  const handleClick = (e) => {
    if (isNavigable) {
      e.stopPropagation();
      // Sort by address for consistent ordering
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

function FunctionPort({ fnView, onSelect, onNavigate, orientation }) {
  return (
    <div className={`ps-port ps-port-${orientation}`} style={{ "--port-accent": fnView.tone }}>
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

function OpsCategory({ category, onSelect, onNavigate }) {
  const [expanded, setExpanded] = useState(false);
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
            <FunctionPort key={fnView.key} fnView={fnView} orientation="ops" onSelect={onSelect} onNavigate={onNavigate} />
          ))}
        </div>
      )}
    </div>
  );
}

function OpsLane({ items, onSelect, onNavigate }) {
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
            <OpsCategory key={cat.key} category={cat} onSelect={onSelect} onNavigate={onNavigate} />
          ))
        ) : (
          <div className="ps-lane-empty">No mapped functions</div>
        )}
      </div>
    </section>
  );
}

function LaneColumn({ title, laneKey, items, onSelect, onNavigate }) {
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
            <FunctionPort key={fnView.key} fnView={fnView} orientation={laneKey} onSelect={onSelect} onNavigate={onNavigate} />
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
  { key: "audits", label: "Audits" },
];

function ContractMachine({ machine, onSelectGuard, onNavigate, companyName }) {
  const [activeTab, setActiveTab] = useState("control");
  const usdLabel = formatUsd(machine.total_usd);

  const tabCounts = {
    control: machine.lanes.top.length + machine.lanes.ops.length,
    inflows: machine.lanes.left.length,
    outflows: machine.lanes.right.length,
    balances: machine.balances?.length || 0,
  };

  return (
    <article className="ps-machine" style={machine.total_usd ? { borderLeft: "2px solid #f59e0b33" } : undefined}>
      <header className="ps-machine-header">
        <div className="ps-machine-name">{machine.name || shortAddr(machine.address)}</div>
        <div className="ps-machine-address">{shortAddr(machine.address)}</div>
        <div className="ps-machine-badges">
          <span className="ps-badge" style={{ "--badge-accent": (ROLE_META[machine.role] || ROLE_META.utility).color }}>{(ROLE_META[machine.role] || ROLE_META.utility).label.replace(/s$/, "")}</span>
          {machine.is_proxy ? <span className="ps-badge" style={{ "--badge-accent": "#9a8a6e" }}>{machine.proxy_type || "proxy"}</span> : null}
          {machine.upgrade_count != null ? <span className="ps-badge" style={{ "--badge-accent": "#8b92a8" }}>{machine.upgrade_count} upgrades</span> : null}
          <span className="ps-badge" style={{ "--badge-accent": "#6b7590" }}>{machine.totalFunctions} functions</span>
          {usdLabel && <span className="ps-badge" style={{ "--badge-accent": "#f59e0b" }}>{usdLabel}</span>}
        </div>
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
          <LaneColumn title={LANE_META.top.label} laneKey="top" items={machine.lanes.top} onSelect={onSelectGuard} onNavigate={onNavigate} />
          {machine.lanes.ops.length > 0 && <OpsLane items={machine.lanes.ops} onSelect={onSelectGuard} onNavigate={onNavigate} />}
        </>
      )}
      {activeTab === "inflows" && (
        <LaneColumn title={LANE_META.left.label} laneKey="left" items={machine.lanes.left} onSelect={onSelectGuard} onNavigate={onNavigate} />
      )}
      {activeTab === "outflows" && (
        <LaneColumn title={LANE_META.right.label} laneKey="right" items={machine.lanes.right} onSelect={onSelectGuard} onNavigate={onNavigate} />
      )}
      {activeTab === "balances" && (
        <BalanceTable machine={machine} />
      )}
      {activeTab === "audits" && (
        <AuditsPanel machine={machine} companyName={companyName} />
      )}
    </article>
  );
}

// Audit status → color. 'audited' is only granted by the backend when the
// current impl has a reviewed_commit proof or an open-ended high-confidence
// temporal match — see api.py `_current_status`. Medium/grace matches show
// up in the coverage list but don't earn the green pill.
const AUDIT_STATUS_META = {
  audited: { label: "Audited", color: "#166534", bg: "#dcfce7", border: "#bbf7d0" },
  non_proxy_audited: { label: "Audited", color: "#166534", bg: "#dcfce7", border: "#bbf7d0" },
  unaudited_since_upgrade: { label: "Unaudited since upgrade", color: "#92400e", bg: "#fef3c7", border: "#fde68a" },
  non_proxy_unaudited: { label: "No audits", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  never_audited: { label: "Never audited", color: "#991b1b", bg: "#fee2e2", border: "#fecaca" },
};

// reviewed_commit is the source-equivalence proof (Etherscan verified source
// SHA matches the GitHub commit the audit named). Rank it above temporal
// matches so the UI can show a 'verified' shield.
// reviewed_address (Phase F) is the audit's scope table explicitly naming
// this address — authoritative over name matches but weaker than a
// byte-level source proof.
const MATCH_TYPE_META = {
  reviewed_commit: { label: "SHA verified", color: "#166534", bg: "#dcfce7", border: "#bbf7d0" },
  reviewed_address: { label: "📍 address pinned", color: "#1e40af", bg: "#dbeafe", border: "#bfdbfe" },
  direct: { label: "name match", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  impl_era: { label: "impl-era", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
};

// equivalence_status → badge. Mirrors services/audits/source_equivalence.py
// EQUIVALENCE_STATUSES. The "proven" case is covered by MATCH_TYPE_META's
// reviewed_commit pill (stronger signal). These describe the *reasons*
// verification couldn't be granted.
const EQUIVALENCE_META = {
  proven: { label: "✓ proof", color: "#166534", bg: "#dcfce7", border: "#bbf7d0" },
  hash_mismatch: { label: "⚠ source differs", color: "#991b1b", bg: "#fee2e2", border: "#fecaca" },
  commit_not_found_in_repo: { label: "⚠ commit missing", color: "#991b1b", bg: "#fee2e2", border: "#fecaca" },
  candidate_path_missing: { label: "path unknown", color: "#92400e", bg: "#fef3c7", border: "#fde68a" },
  etherscan_unverified: { label: "no verified source", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  no_reviewed_commit: { label: "no commit in PDF", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  no_source_repo: { label: "no repo recorded", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  etherscan_fetch_failed: { label: "etherscan error", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  github_fetch_failed: { label: "github error", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  not_attempted: { label: "not verified yet", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
};

// proof_kind → badge (Phase C). Only rendered on rows whose
// equivalence_status is 'proven'. Mirrors db.models.proof_kind vocabulary.
// 'clean' is the normal green; 'pre_fix_unpatched' is the red-flag case
// where deployed code matches the reviewed commit but a known fix was
// never shipped.
const PROOF_KIND_META = {
  clean: { label: "✓ reviewed", color: "#166534", bg: "#dcfce7", border: "#bbf7d0" },
  post_fix: { label: "✓ fix deployed", color: "#065f46", bg: "#ccfbf1", border: "#99f6e4" },
  pre_fix_unpatched: { label: "🚨 FIX NOT SHIPPED", color: "#7f1d1d", bg: "#fecaca", border: "#f87171" },
  cited_only: { label: "? coincidental", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  unclassified: { label: "unclassified", color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
};

// severity label → color. Matches AuditsTab's CHIP conventions.
const SEVERITY_META = {
  critical: { color: "#7f1d1d", bg: "#fee2e2", border: "#fca5a5" },
  high: { color: "#991b1b", bg: "#fee2e2", border: "#fecaca" },
  medium: { color: "#92400e", bg: "#fef3c7", border: "#fde68a" },
  low: { color: "#475569", bg: "#f1f5f9", border: "#e2e8f0" },
  info: { color: "#1e40af", bg: "#dbeafe", border: "#bfdbfe" },
};

// resolution status → human label. "fixed" never shows in live_findings
// because the backend filters it out, but keep the label in case API
// semantics change.
const STATUS_LABELS = {
  fixed: "fixed",
  partially_fixed: "partially fixed",
  acknowledged: "acknowledged",
  mitigated: "mitigated",
  wont_fix: "won't fix",
};

function formatAuditDate(date) {
  if (!date) return "—";
  const parsed = new Date(date);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
  }
  return String(date);
}

// Small chip rendered from a ``{color, bg, border}`` meta object. Used by
// every audit-related pill in the surface panel — match_type, confidence,
// equivalence status, severity. Kept inline + presentational so the parent
// components don't accumulate repeated <span className="ps-badge"> blocks.
function MetaBadge({ meta, label, title }) {
  return (
    <span
      className="ps-badge"
      title={title || undefined}
      style={{
        "--badge-accent": meta.color,
        background: meta.bg,
        color: meta.color,
        border: `1px solid ${meta.border}`,
        fontSize: 10,
      }}
    >
      {label ?? meta.label}
    </span>
  );
}

// Shared meta for the ±drift badges on an audit chip. Inlined as constants
// so the chip renderer doesn't carry an if/else ladder of hex codes.
const DRIFT_TRUE_META = { color: "#991b1b", bg: "#fee2e2", border: "#fecaca" };
const DRIFT_FALSE_META = { color: "#166534", bg: "#dcfce7", border: "#bbf7d0" };

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
                      title={
                        a.proof_kind === "pre_fix_unpatched"
                          ? "Audit reviewed THIS code; fix commits exist in the audit text but deployed code doesn't match any — the fix was never shipped"
                          : a.proof_kind === "post_fix"
                          ? "Deployed code matches a fix commit the audit referenced; the audit's findings were addressed"
                          : a.proof_kind === "cited_only"
                          ? "Matched a commit that the audit cited for context only (not reviewed, not a fix)"
                          : undefined
                      }
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
        <div className="ps-inspector-label">Resolved Principals</div>
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
    </aside>
  );
}

function PrincipalDetail({ principal, machines, onNavigate, onFocusContract }) {
  const [focusIdx, setFocusIdx] = useState(0);
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
        <div className="ps-machine-name">{principal.label || shortAddr(principal.address)}</div>
        <div className="ps-machine-address">{principal.address}</div>
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
          {owners.map((addr) => (
            <div key={addr} className="ps-principal-signer">{addr}</div>
          ))}
        </section>
      )}

      {controlledMachines.length > 0 && (
        <section className="ps-principal-section">
          <div className="ps-principal-section-hdr" style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span>Controls ({controlledMachines.length} contracts)</span>
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
    <div className={`ps-principal-node${data.focused ? " ps-node-focused" : ""}`} style={{ "--principal-color": color }}>
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

function SurfaceCanvas({ machines, fundFlows, principals, selectedAddress, focusAddress, focusedAddress, highlightedAddresses, onSelectMachine, principalTour, onTourGo, onTourBack }) {
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
            onSelect: () => onSelectMachine(n.data.machine),
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
  }, [initNodes, initEdges, selectedAddress, focusedAddress, highlightedAddresses, onSelectMachine]);

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

function SidebarTabs({ mode, onSetMode, auditCount }) {
  return (
    <div style={{ display: "flex", gap: 4, padding: "8px 8px 0 8px", borderBottom: "1px solid #e2e8f0" }}>
      <button
        onClick={() => onSetMode("detail")}
        style={{
          flex: 1, padding: "8px 10px", borderRadius: "6px 6px 0 0",
          border: "1px solid #e2e8f0", borderBottom: "none",
          background: mode === "detail" ? "#fafafa" : "transparent",
          fontWeight: mode === "detail" ? 600 : 400,
          cursor: "pointer", font: "inherit", color: "inherit",
        }}
      >
        Detail
      </button>
      <button
        onClick={() => onSetMode("audits")}
        style={{
          flex: 1, padding: "8px 10px", borderRadius: "6px 6px 0 0",
          border: "1px solid #e2e8f0", borderBottom: "none",
          background: mode === "audits" ? "#fafafa" : "transparent",
          fontWeight: mode === "audits" ? 600 : 400,
          cursor: "pointer", font: "inherit", color: "inherit",
        }}
      >
        Audits{auditCount != null ? ` (${auditCount})` : ""}
      </button>
    </div>
  );
}

function AuditsListPanel({ coverageData, activeAuditId, onPickAudit, loading, error }) {
  if (loading) return <section className="ps-principal-section"><div className="ps-inspector-empty">Loading audits…</div></section>;
  if (error) return <section className="ps-principal-section"><div className="ps-inspector-empty">Failed: {error}</div></section>;
  if (!coverageData) return null;

  // Invert: audit_id → { audit, addresses: Set<lowercase string> }
  const byAudit = new Map();
  for (const entry of coverageData.coverage || []) {
    const addr = (entry.address || "").toLowerCase();
    if (!addr) continue;
    for (const a of entry.audits || []) {
      const id = a.audit_id;
      if (!byAudit.has(id)) {
        byAudit.set(id, { audit: a, addresses: new Set() });
      }
      byAudit.get(id).addresses.add(addr);
    }
  }

  // Sort audits: active first, then by date desc (nulls last), then id desc
  const entries = [...byAudit.values()].sort((x, y) => {
    const dx = x.audit.date || "";
    const dy = y.audit.date || "";
    if (dx !== dy) return dx < dy ? 1 : -1;
    return (y.audit.audit_id || 0) - (x.audit.audit_id || 0);
  });

  return (
    <section className="ps-principal-section">
      <div className="ps-principal-section-hdr">All audits ({entries.length})</div>
      <div style={{ fontSize: 11, color: "#6b7590", marginBottom: 8 }}>
        Click an audit to highlight its covered contracts on the canvas.
      </div>
      {activeAuditId != null && (
        <button
          onClick={() => onPickAudit(null)}
          style={{
            marginBottom: 8, padding: "4px 10px", borderRadius: 4,
            border: "1px solid #cbd5e1", background: "#fff",
            cursor: "pointer", fontSize: 11, color: "inherit", font: "inherit",
          }}
        >
          ✕ Clear highlight
        </button>
      )}
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {entries.map(({ audit, addresses }) => {
          const isActive = activeAuditId === audit.audit_id;
          return (
            <button
              key={audit.audit_id}
              onClick={() => onPickAudit(isActive ? null : audit.audit_id)}
              style={{
                textAlign: "left",
                padding: "8px 10px",
                borderRadius: 6,
                border: isActive ? "2px solid #22c55e" : "1px solid #e2e8f0",
                background: isActive ? "#f0fdf4" : "#fff",
                cursor: "pointer",
                font: "inherit", color: "inherit",
              }}
            >
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 8 }}>
                <div style={{ fontWeight: 600, fontSize: 13 }}>{audit.auditor || "Unknown"}</div>
                <div style={{ fontSize: 11, color: "#6b7590", whiteSpace: "nowrap" }}>{formatAuditDate(audit.date)}</div>
              </div>
              <div style={{ fontSize: 12, color: "#334155", marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {audit.title || ""}
              </div>
              <div style={{ fontSize: 11, color: "#6b7590", marginTop: 4 }}>
                covers {addresses.size} contract{addresses.size === 1 ? "" : "s"}
                {audit.match_type ? ` · ${audit.match_type}` : ""}
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}

function DraggableSidebar({ children }) {
  const [width, setWidth] = useState(380);
  const dragging = useRef(false);

  const onMouseDown = useCallback((e) => {
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
  }, [width]);

  return (
    <div className="ps-sidebar" style={{ width, minWidth: width, maxWidth: width }}>
      <div className="ps-sidebar-handle" onMouseDown={onMouseDown} />
      <div className="ps-sidebar-content">{children}</div>
    </div>
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

function SearchNavigator({ machines, principals, onFocus }) {
  const [mode, setMode] = useState("all");
  const [sortKey, setSortKey] = useState("value");
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);

  const results = useMemo(
    () => buildSearchResults(machines, principals, mode, sortKey, query),
    [machines, principals, mode, sortKey, query]
  );

  // Reset index when results change
  useEffect(() => { setIndex(0); }, [results.length, mode, sortKey, query]);

  // Notify parent when focused result changes
  useEffect(() => {
    if (results.length > 0 && results[index]) {
      onFocus(results[index]);
    } else {
      onFocus(null);
    }
  }, [index, results]);

  const prev = () => setIndex((i) => (i > 0 ? i - 1 : results.length - 1));
  const next = () => setIndex((i) => (i < results.length - 1 ? i + 1 : 0));

  const current = results[index];

  return (
    <div className="ps-search-nav">
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
      <div className="ps-search-controls">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search... (e.g. 'min value 3M')"
          className="ps-search-input"
        />
        <select
          value={sortKey}
          onChange={(e) => setSortKey(e.target.value)}
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

export default function ProtocolSurface({ companyName }) {
  const [companyData, setCompanyData] = useState(null);
  const [functionData, setFunctionData] = useState({});
  const [selectedGuard, setSelectedGuard] = useState(null);
  const [selectedMachine, setSelectedMachine] = useState(null);
  const [selectedPrincipal, setSelectedPrincipal] = useState(null);
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
    // Sync focus address to URL
    const url = new URL(window.location.href);
    if (addr) {
      url.searchParams.set("focus", addr);
    } else {
      url.searchParams.delete("focus");
    }
    window.history.replaceState({}, "", url.toString());
  }, []);
  // Multi-principal tour state: { principals: [...], index: 0, sourceContract: "0x...", sourceFunction: "fn" }
  const [principalTour, setPrincipalTour] = useState(null);
  const [error, setError] = useState(null);
  const [headerCollapsed, setHeaderCollapsed] = useState(false);

  // Right sidebar mode: "detail" (machine/principal inspector) vs "audits"
  // (flat audit list). "audits" mode keeps the list visible while the user
  // clicks different audits and watches the canvas highlight update.
  const [sidebarMode, setSidebarMode] = useState("detail");

  // Coverage payload — one call, cached locally. Used to build the audits
  // list + the audit_id → address-set map for highlight propagation.
  const [coverageData, setCoverageData] = useState(null);
  const [coverageError, setCoverageError] = useState(null);
  const [coverageLoading, setCoverageLoading] = useState(false);

  // Active audit: when non-null, its covered contracts get a green ring
  // and everything else dims on the canvas.
  const [activeAuditId, setActiveAuditId] = useState(null);

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

  // Highlighted addresses for the active audit — lowercased Set so the
  // canvas comparison is O(1). null if no audit selected.
  const highlightedAddresses = useMemo(() => {
    if (activeAuditId == null || !coverageData) return null;
    const out = new Set();
    for (const entry of coverageData.coverage || []) {
      const addr = (entry.address || "").toLowerCase();
      if (!addr) continue;
      if ((entry.audits || []).some((a) => a.audit_id === activeAuditId)) {
        out.add(addr);
      }
    }
    return out;
  }, [activeAuditId, coverageData]);
  const [enabledRoles, setEnabledRoles] = useState(() => {
    const initial = new Set();
    for (const [role, meta] of Object.entries(ROLE_META)) {
      if (meta.defaultOn) initial.add(role);
    }
    return initial;
  });

  useEffect(() => {
    if (!companyName) return undefined;
    let cancelled = false;

    async function load() {
      try {
        setError(null);
        setSelectedGuard(null);
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
  }, [companyName]);

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
    if (restoredFocus.current || !machines.length) return;
    const params = new URLSearchParams(window.location.search);
    const urlFocus = params.get("focus");
    if (urlFocus) {
      restoredFocus.current = true;
      triggerFocus(urlFocus);
    }
  }, [machines, triggerFocus]);

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
  }, []);

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

    if (target.type === "contract") {
      const machine = machines.find((m) => m.address?.toLowerCase() === target.address?.toLowerCase());
      if (machine) {
        setSelectedMachine(machine);
        setSelectedPrincipal(null);
        setSelectedGuard(null);
        setPrincipalTour(null);
        triggerFocus(machine.address);
      }
    } else {
      // Set up multi-principal tour if multiple principals
      if (target._allPrincipals && target._allPrincipals.length > 1) {
        setPrincipalTour({
          principals: target._allPrincipals,
          index: 0,
          sourceContract: target._sourceContract,
          sourceFunction: target._sourceFunction,
        });
      } else {
        setPrincipalTour(null);
      }
      navigateToPrincipal(target);
    }
  }, [machines, visiblePrincipals, selectedMachine, selectedPrincipal, triggerFocus, navigateToPrincipal]);

  const handleBreadcrumbNav = useCallback((item, index) => {
    // Truncate breadcrumbs to this point
    setBreadcrumbs((prev) => prev.slice(0, index));
    if (item.type === "contract") {
      const machine = machines.find((m) => m.address?.toLowerCase() === item.address?.toLowerCase());
      if (machine) { setSelectedMachine(machine); setSelectedPrincipal(null); setSelectedGuard(null); }
    } else {
      const principal = visiblePrincipals.find((p) => p.address?.toLowerCase() === item.address?.toLowerCase());
      if (principal) { setSelectedPrincipal(principal); setSelectedMachine(null); setSelectedGuard(null); }
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

  return (
    <div className="ps-surface ps-surface-fullscreen">
      {/* Floating header overlay */}
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

      {/* Floating toolbar overlays */}
      <div className="ps-surface-toolbar-overlay">
        <RoleFilterBar machines={allMachines} enabledRoles={enabledRoles} onToggle={handleToggleRole} />
      </div>

      <div className="ps-surface-search-overlay">
        <SearchNavigator
        machines={machines}
        principals={visiblePrincipals}
        onFocus={(item) => {
          if (!item) {
            setSelectedMachine(null); setSelectedPrincipal(null);
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
            // Focus on the principal node or its first controlled contract
            triggerFocus(item.address || item.machine?.address);
          } else if (item.machine) {
            setSelectedMachine(item.machine);
            setSelectedPrincipal(null);
            setSelectedGuard(null);
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
            selectedAddress={selectedMachine?.address}
            focusAddress={focusAddress}
            focusedAddress={focusedAddress}
            highlightedAddresses={highlightedAddresses}
            onSelectMachine={handleSelectMachine}
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
                  triggerFocus(machine.address);
                }
              }
            }}
          />
        </ReactFlowProvider>
        <DraggableSidebar>
          <SidebarTabs
            mode={sidebarMode}
            onSetMode={setSidebarMode}
            auditCount={coverageData?.audit_count}
          />
          {sidebarMode === "audits" && (
            <AuditsListPanel
              coverageData={coverageData}
              activeAuditId={activeAuditId}
              onPickAudit={setActiveAuditId}
              loading={coverageLoading}
              error={coverageError}
            />
          )}
          {sidebarMode === "detail" && (
            <Breadcrumbs items={breadcrumbs} onNavigate={handleBreadcrumbNav} />
          )}
          {sidebarMode === "detail" && selectedPrincipal && (
            <PrincipalDetail
              key={selectedPrincipal.address}
              principal={selectedPrincipal}
              machines={machines}
              onNavigate={handleNavigate}
              onFocusContract={(addr) => triggerFocus(addr)}
            />
          )}
          {sidebarMode === "detail" && selectedMachine && !selectedPrincipal && (
            <ContractMachine
              key={selectedMachine.address}
              machine={selectedMachine}
              onSelectGuard={setSelectedGuard}
              onNavigate={handleNavigate}
              companyName={companyName}
            />
          )}
          {sidebarMode === "detail" && !selectedPrincipal && <InspectorCard selected={selectedGuard} onNavigate={handleNavigate} />}
        </DraggableSidebar>
      </div>
    </div>
  );
}
