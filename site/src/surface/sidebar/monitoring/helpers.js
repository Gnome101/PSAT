// Pure helpers shared by every component under surface/sidebar/monitoring/.
// JSX-returning helpers live in icons.jsx; only structural data conversion
// belongs here.

import { MONITOR_ALERT_GROUPS } from "../../meta.js";

export function groupKeysFromConfig(config = {}) {
  return MONITOR_ALERT_GROUPS
    .filter((group) => group.flags.some((flag) => config?.[flag]))
    .map((group) => group.key);
}

export function configFromGroupKeys(groupKeys) {
  const selected = new Set(groupKeys);
  const config = {};
  for (const group of MONITOR_ALERT_GROUPS) {
    for (const flag of group.flags) {
      config[flag] = selected.has(group.key);
    }
  }
  return config;
}

export function eventTypesFromGroupKeys(groupKeys) {
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

export function needsPollingFromGroupKeys(groupKeys) {
  const selected = new Set(groupKeys);
  return MONITOR_ALERT_GROUPS.some((group) => group.needsPolling && selected.has(group.key));
}

export function subscriptionEventTypeSet(subscription) {
  const raw = subscription?.event_filter?.event_types;
  if (!Array.isArray(raw) || raw.length === 0) return null;
  return new Set(raw.map((eventType) => String(eventType).toLowerCase()));
}

export function matchingWebhookCountForConfig(config, subscriptions = []) {
  if (!subscriptions.length) return 0;
  const eventTypes = eventTypesFromGroupKeys(groupKeysFromConfig(config))
    .map((eventType) => eventType.toLowerCase());
  return subscriptions.filter((subscription) => {
    const allowed = subscriptionEventTypeSet(subscription);
    if (!allowed) return true;
    return eventTypes.some((eventType) => allowed.has(eventType));
  }).length;
}

export function contractTypeForMachine(machine) {
  if (machine?.is_proxy) return "proxy";
  if (machine?.is_pausable || machine?.capabilities?.includes("pause")) return "pausable";
  if (machine?.role === "governance") return "governance";
  return "regular";
}
