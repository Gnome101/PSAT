import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { ReactFlowProvider } from "@xyflow/react";
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
import {
  ALL_ROLES,
  EVENT_ACCENTS,
  EVENT_LABELS,
  LANE_META,
  MACHINE_TABS,
  MONITOR_ALERT_GROUPS,
  MONITOR_FLAGS,
  PRINCIPAL_COLORS,
  ROLE_META,
  SEARCH_MODES,
  SORT_OPTIONS,
  TYPE_META,
} from "./surface/meta.js";
import {
  dedupeShas,
  formatDelay,
  formatEventAgo,
  formatUsd,
  isHexAddress,
  isRoleIdAddress,
  maskWebhook,
  shortAddr,
} from "./surface/format.js";
import { categorizeOps, findFunctionView, machineFunctions, tabForLane } from "./surface/lane.js";
import { buildMachines } from "./surface/layout/buildMachines.js";
import { elkLayout } from "./surface/layout/elkLayout.js";
import { buildSearchResults } from "./surface/layout/search.js";
import { buildFallbackDependencyGraph } from "./surface/layout/dependencyFallback.js";
import { ContractMachine } from "./surface/lanes/ContractMachine.jsx";
import { SurfaceCanvas } from "./surface/canvas/SurfaceCanvas.jsx";
import { Breadcrumbs } from "./surface/sidebar/Breadcrumbs.jsx";
import { DetailEmptyState } from "./surface/sidebar/DetailEmptyState.jsx";
import { DraggableSidebar } from "./surface/sidebar/DraggableSidebar.jsx";
import { InspectorCard } from "./surface/sidebar/InspectorCard.jsx";
import { PrincipalDetail } from "./surface/sidebar/PrincipalDetail.jsx";
import { RoleFilterBar } from "./surface/sidebar/RoleFilterBar.jsx";
import { SidebarTabs } from "./surface/sidebar/SidebarTabs.jsx";
import { AuditsListPanel } from "./surface/sidebar/AuditsListPanel.jsx";
import { UpgradesSidebarPanel } from "./surface/sidebar/UpgradesSidebarPanel.jsx";
import { SearchModesBar } from "./surface/sidebar/search/SearchModesBar.jsx";
import { SearchNavigator } from "./surface/sidebar/search/SearchNavigator.jsx";
import { DependencyGraphModal } from "./surface/modals/DependencyGraphModal.jsx";














// Sidebar Upgrades tab. Two states:
//   - No machine selected: list proxies in this protocol with upgrade counts.
//     Click a row → focus that proxy on canvas (parent handles selection).
//   - Machine selected (proxy): lazy-fetch the analysis blob for that contract
//     (the per-contract upgrade_history isn't included in /api/company/{name},
//     so we go via /api/analyses/{job_id}) and render the existing
//     UpgradesPanel — same layout as the standalone /address/<addr>/upgrades
//     page so the per-impl audit cards (UpgradeAuditCard) appear identically.

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
