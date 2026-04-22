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
<<<<<<< HEAD
import { listAddressLabels } from "./api/addressLabels.js";
import AddressLabelInline from "./AddressLabelInline.jsx";
=======
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
>>>>>>> ab768f72da932ba7ac31f8b62120dbfcd85b7a23

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
    sublabel = principal.label || "contract";
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

function PrincipalDetail({ principal, machines, onNavigate, onFocusContract, addressLabels, refreshAddressLabels }) {
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

function SidebarTabs({ mode, onSetMode, auditCount }) {
  return (
    <div className="ps-sidebar-tabs">
      <button
        className={`ps-sidebar-tab ${mode === "detail" ? "active" : ""}`}
        onClick={() => onSetMode("detail")}
      >
        Detail
      </button>
      <button
        className={`ps-sidebar-tab ${mode === "audits" ? "active" : ""}`}
        onClick={() => onSetMode("audits")}
      >
        Audits{auditCount != null ? ` (${auditCount})` : ""}
      </button>
    </div>
  );
}

function AuditsListPanel({ coverageData, activeAuditId, onPickAudit, loading, error, machines }) {
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
        <div className="ps-audits-panel-hdr">All audits ({entries.length})</div>
        <div className="ps-audits-panel-hint">
          Click an audit to highlight its covered contracts on the canvas.
        </div>

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
                    covers {addresses.size} contract{addresses.size === 1 ? "" : "s"}
                    {audit.match_type ? ` · ${audit.match_type}` : ""}
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
      {/* Mode pills (All / Safes / EOAs / Timelocks / Has Funds) now render
          at top-left via <SearchModesBar />. The rest of the search nav
          (input, sort, arrows, preview) stays in the centre overlay. */}
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
  const [headerCollapsed, setHeaderCollapsed] = useState(true);

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

  // Clicking a Safe/Timelock/EOA node on the canvas selects the principal
  // (opens the detail panel with signers / delay / controlled contracts)
  // and focuses it — same behaviour as clicking a single-principal guard
  // badge, just driven from the node itself.
  const handleSelectPrincipal = useCallback((principal) => {
    if (!principal) return;
    setSelectedPrincipal(principal);
    setSelectedMachine(null);
    setSelectedGuard(null);
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

    if (target.type === "contract") {
      const machine = machines.find((m) => m.address?.toLowerCase() === target.address?.toLowerCase());
      if (machine) {
        setSelectedMachine(machine);
        setSelectedPrincipal(null);
        setSelectedGuard(null);
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
            onSelectPrincipal={handleSelectPrincipal}
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
              machines={machines}
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
              addressLabels={addressLabels}
              refreshAddressLabels={refreshAddressLabels}
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
