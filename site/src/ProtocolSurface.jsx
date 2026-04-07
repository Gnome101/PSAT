import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ELK from "elkjs/lib/elk.bundled.js";
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  ReactFlowProvider,
  Handle,
  Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import hourglassIcon from "./assets/hourglass-empty.svg";
import questionMarkIcon from "./assets/question-mark.svg";
import vaultIcon from "./assets/vault.svg";

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

function collectPrincipals(fn) {
  const byAddress = new Map();

  function pushPrincipal(principal, origin) {
    const address = String(principal?.address || "").toLowerCase();
    if (!address.startsWith("0x")) return;
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

function guardSummary(fn) {
  const principals = collectPrincipals(fn);

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
          guard: guardSummary(fn),
          principals: collectPrincipals(fn),
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

function GuardButton({ fnView, onSelect }) {
  const isUnknown = fnView.guard.kind === "unknown";
  return (
    <button
      type="button"
      className={`ps-guard-button${isUnknown ? " ps-guard-icon-only" : ""}`}
      style={{ "--guard-accent": fnView.guard.accent }}
      onClick={() => onSelect(fnView)}
      title={isUnknown ? "Unresolved guard" : `Inspect guard details for ${fnView.name}`}
    >
      <span className="ps-guard-icon">
        <GuardGlyph kind={fnView.guard.kind} accent={fnView.guard.accent} title={fnView.guard.label} />
      </span>
      {!isUnknown && (
        <span className="ps-guard-copy">
          <span className="ps-guard-label">{fnView.guard.label}</span>
          <span className="ps-guard-meta">{fnView.guard.sublabel}</span>
        </span>
      )}
    </button>
  );
}

function FunctionPort({ fnView, onSelect, orientation }) {
  return (
    <div className={`ps-port ps-port-${orientation}`} style={{ "--port-accent": fnView.tone }}>
      <div className="ps-port-copy">
        <div className="ps-port-name">{fnView.name}</div>
        {fnView.action && <div className="ps-port-action">{fnView.action}</div>}
      </div>
      <GuardButton fnView={fnView} onSelect={onSelect} />
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

function OpsCategory({ category, onSelect }) {
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
            <FunctionPort key={fnView.key} fnView={fnView} orientation="ops" onSelect={onSelect} />
          ))}
        </div>
      )}
    </div>
  );
}

function OpsLane({ items, onSelect }) {
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
            <OpsCategory key={cat.key} category={cat} onSelect={onSelect} />
          ))
        ) : (
          <div className="ps-lane-empty">No mapped functions</div>
        )}
      </div>
    </section>
  );
}

function LaneColumn({ title, laneKey, items, onSelect }) {
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
            <FunctionPort key={fnView.key} fnView={fnView} orientation={laneKey} onSelect={onSelect} />
          ))
        ) : (
          <div className="ps-lane-empty">No mapped functions</div>
        )}
      </div>
    </section>
  );
}

function ContractMachine({ machine, onSelectGuard }) {
  return (
    <article className="ps-machine">
      <header className="ps-machine-header">
        <div className="ps-machine-name">{machine.name || shortAddr(machine.address)}</div>
        <div className="ps-machine-address">{shortAddr(machine.address)}</div>
        <div className="ps-machine-badges">
          <span className="ps-badge" style={{ "--badge-accent": (ROLE_META[machine.role] || ROLE_META.utility).color }}>{(ROLE_META[machine.role] || ROLE_META.utility).label.replace(/s$/, "")}</span>
          {machine.is_proxy ? <span className="ps-badge" style={{ "--badge-accent": "#9a8a6e" }}>{machine.proxy_type || "proxy"}</span> : null}
          {machine.upgrade_count != null ? <span className="ps-badge" style={{ "--badge-accent": "#8b92a8" }}>{machine.upgrade_count} upgrades</span> : null}
          <span className="ps-badge" style={{ "--badge-accent": "#6b7590" }}>{machine.totalFunctions} functions</span>
        </div>
      </header>

      <LaneColumn title={LANE_META.top.label} laneKey="top" items={machine.lanes.top} onSelect={onSelectGuard} />
      {machine.lanes.ops.length > 0 && <OpsLane items={machine.lanes.ops} onSelect={onSelectGuard} />}
      <LaneColumn title={LANE_META.left.label} laneKey="left" items={machine.lanes.left} onSelect={onSelectGuard} />
      <LaneColumn title={LANE_META.right.label} laneKey="right" items={machine.lanes.right} onSelect={onSelectGuard} />
    </article>
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

function InspectorCard({ selected }) {
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
                <div key={principal.address} className="ps-principal-card">
                  <div className="ps-principal-top">
                    <span className="ps-principal-type" style={{ "--principal-accent": type.accent }}>{type.label}</span>
                    <span className="ps-principal-address">{shortAddr(principal.address)}</span>
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
      className={`ps-node${data.selected ? " ps-node-selected" : ""}`}
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
    <div className="ps-principal-node" style={{ "--principal-color": color }}>
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

  const NODE_W = 300;
  const NODE_H = 220;
  // Scale columns based on node count — more nodes = wider layout
  const colCount = n <= 9 ? 3 : n <= 20 ? 4 : 5;
  const spread = NODE_W * 1.4;
  const positions = new Array(n);

  // Connected nodes: multi-column stagger, spreading wider as we go down
  for (let rank = 0; rank < connected.length; rank++) {
    const idx = connected[rank];
    const col = rank % colCount;
    const row = Math.floor(rank / colCount);
    const rowSpread = spread * (1 + row * 0.15);
    let x, y;
    y = row * NODE_H;
    // Spread columns evenly around center
    const colOffset = (col - (colCount - 1) / 2) * rowSpread;
    // Deterministic jitter
    const jx = ((rank * 7 + 13) % 60 - 30);
    const jy = ((rank * 11 + 7) % 30 - 15);
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
    const rx = connected.length > 0 ? (Math.max(...cxs) - Math.min(...cxs)) / 2 + NODE_W * 2.2 : NODE_W * 3;
    const ry = connected.length > 0 ? (Math.max(...cys) - Math.min(...cys)) / 2 + NODE_H * 1.8 : NODE_H * 3;

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
  const PRINCIPAL_OFFSET_Y = -160; // above their contracts
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

    // Avoid overlapping other principals
    for (const used of usedPrincipalPositions) {
      if (Math.abs(px - used.x) < 140 && Math.abs(py - used.y) < 80) {
        px += 150;
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
      "elk.layered.spacing.nodeNodeBetweenLayers": "100",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
      "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
      "elk.layered.edgeRouting": "ORTHOGONAL",
      "elk.spacing.edgeNode": "40",
      "elk.spacing.edgeEdge": "30",
      "elk.layered.spacing.edgeEdgeBetweenLayers": "25",
      "elk.layered.spacing.edgeNodeBetweenLayers": "35",
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

function SurfaceCanvas({ machines, fundFlows, principals, selectedAddress, onSelectMachine }) {
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

    setNodes(
      initNodes.map((n) => {
        const nid = n.id?.toLowerCase();
        const dimmed = sel && !connectedNodes.has(nid);
        return {
          ...n,
          style: dimmed ? { opacity: 0.25 } : {},
          data: {
            ...n.data,
            selected: n.id === selectedAddress,
            onSelect: () => onSelectMachine(n.data.machine),
          },
        };
      })
    );

    setEdges(
      initEdges.map((e) => {
        const src = e.source?.toLowerCase();
        const tgt = e.target?.toLowerCase();
        const related = !sel || src === sel || tgt === sel;
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
  }, [initNodes, initEdges, selectedAddress, onSelectMachine]);

  return (
    <div className="ps-canvas-wrap">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        minZoom={0.2}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1e293b" gap={24} size={1} />
        <Controls showInteractive={false} />
      </ReactFlow>
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

export default function ProtocolSurface({ companyName }) {
  const [companyData, setCompanyData] = useState(null);
  const [functionData, setFunctionData] = useState({});
  const [selectedGuard, setSelectedGuard] = useState(null);
  const [selectedMachine, setSelectedMachine] = useState(null);
  const [error, setError] = useState(null);
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
    setSelectedGuard(null);
  }, []);

  const visiblePrincipals = useMemo(() => {
    const visibleAddrs = new Set(machines.map((m) => m.address?.toLowerCase()));
    return (companyData?.principals || []).filter((p) =>
      (p.controls || []).some((a) => visibleAddrs.has(a.toLowerCase()))
    );
  }, [machines, companyData]);

  const totals = useMemo(() => {
    return machines.reduce(
      (acc, machine) => {
        acc.contracts += 1;
        acc.functions += machine.totalFunctions;
        return acc;
      },
      { contracts: 0, functions: 0 }
    );
  }, [machines]);

  if (error) return <p className="empty">Failed: {error}</p>;
  if (!companyData) return <p className="empty">Loading surface...</p>;

  return (
    <div className="ps-surface">
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
        </div>
      </div>

      <RoleFilterBar machines={allMachines} enabledRoles={enabledRoles} onToggle={handleToggleRole} />

      <div className="ps-layout">
        <ReactFlowProvider>
          <SurfaceCanvas
            machines={machines}
            fundFlows={companyData?.fund_flows}
            principals={visiblePrincipals}
            selectedAddress={selectedMachine?.address}
            onSelectMachine={handleSelectMachine}
          />
        </ReactFlowProvider>
        <DraggableSidebar>
          {selectedMachine && (
            <ContractMachine
              key={selectedMachine.address}
              machine={selectedMachine}
              onSelectGuard={setSelectedGuard}
            />
          )}
          <InspectorCard selected={selectedGuard} />
        </DraggableSidebar>
      </div>
    </div>
  );
}
