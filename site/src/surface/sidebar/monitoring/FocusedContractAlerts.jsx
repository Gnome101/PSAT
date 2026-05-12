import { shortAddr } from "../../format.js";
import { matchingWebhookCountForConfig } from "./helpers.js";
import { monitoringChips, MonitorWebhookIndicator } from "./icons.jsx";

export function FocusedContractAlerts({
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
