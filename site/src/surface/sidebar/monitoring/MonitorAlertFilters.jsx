import { MONITOR_ALERT_GROUPS } from "../../meta.js";
import { MonitorEventIcon, WebhookGlyph } from "./icons.jsx";

export function MonitorAlertFilters({
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
