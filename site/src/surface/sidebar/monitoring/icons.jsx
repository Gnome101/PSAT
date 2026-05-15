import { GuardGlyph } from "../../../ui/GuardGlyph.jsx";
import { MONITOR_ALERT_GROUPS, MONITOR_FLAGS } from "../../meta.js";
import { groupKeysFromConfig } from "./helpers.js";

export function monitoringChips(config) {
  const active = MONITOR_FLAGS.filter((flag) => {
    if (config?.[flag.key]) return true;
    return (flag.aliases || []).some((alias) => config?.[alias]);
  });
  if (!active.length) return <span className="ps-monitor-muted">none</span>;
  return active.map((flag) => (
    <span key={flag.key} className="ps-monitor-chip">{flag.label}</span>
  ));
}

export function MonitorEventIcon({ kind }) {
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

export function MonitorEventIcons({ config }) {
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

export function WebhookGlyph() {
  return (
    <svg width="13" height="13" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M5.2 10.8C3.8 10.8 2.7 9.7 2.7 8.3C2.7 6.9 3.8 5.8 5.2 5.8H6.4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <path d="M9.6 5.2H10.8C12.2 5.2 13.3 6.3 13.3 7.7C13.3 9.1 12.2 10.2 10.8 10.2H9.6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      <path d="M6.1 8H9.9" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

export function MonitorWebhookIndicator({ count }) {
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
