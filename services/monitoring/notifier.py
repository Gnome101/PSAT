"""Discord notification dispatch for proxy upgrade and governance events."""

from __future__ import annotations

import logging

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import (
    MonitoredContract,
    MonitoredEvent,
    ProtocolSubscription,
    ProxySubscription,
    ProxyUpgradeEvent,
)

logger = logging.getLogger(__name__)

DISCORD_TIMEOUT = 10


def _format_embed(event: ProxyUpgradeEvent) -> dict:
    """Build a Discord embed dict for a single upgrade event."""
    proxy = event.watched_proxy
    label = proxy.label or proxy.proxy_address
    fields = [
        {"name": "Proxy", "value": f"`{proxy.proxy_address}`", "inline": True},
        {"name": "Chain", "value": proxy.chain, "inline": True},
        {"name": "Event", "value": event.event_type, "inline": True},
        {"name": "New Implementation", "value": f"`{event.new_implementation}`", "inline": False},
    ]
    if event.old_implementation:
        fields.insert(3, {"name": "Old Implementation", "value": f"`{event.old_implementation}`", "inline": False})
    if event.block_number:
        fields.append({"name": "Block", "value": str(event.block_number), "inline": True})
    if event.tx_hash:
        fields.append({"name": "Tx", "value": f"`{event.tx_hash}`", "inline": False})

    return {
        "title": f"Proxy Upgrade: {label}",
        "color": 0xFF9900,
        "fields": fields,
    }


def _send_discord(webhook_url: str, embed: dict) -> None:
    resp = requests.post(
        webhook_url,
        json={"embeds": [embed]},
        timeout=DISCORD_TIMEOUT,
    )
    if not resp.ok:
        logger.warning("Discord webhook failed (%s): %s", resp.status_code, resp.text[:200])


def notify_upgrades(session: Session, events: list[ProxyUpgradeEvent]) -> None:
    """Send Discord notifications for detected upgrade events.

    Looks up all subscriptions for each event's watched proxy and POSTs
    to each configured Discord webhook. Failures are logged, never raised.
    """
    if not events:
        return

    proxy_ids = {e.watched_proxy_id for e in events}
    subs = (
        session.execute(
            select(ProxySubscription).where(
                ProxySubscription.watched_proxy_id.in_(proxy_ids),
                ProxySubscription.discord_webhook_url.isnot(None),
            )
        )
        .scalars()
        .all()
    )

    if not subs:
        return

    subs_by_proxy: dict[str, list[ProxySubscription]] = {}
    for sub in subs:
        subs_by_proxy.setdefault(str(sub.watched_proxy_id), []).append(sub)

    sent = 0
    for event in events:
        proxy_subs = subs_by_proxy.get(str(event.watched_proxy_id), [])
        if not proxy_subs:
            continue

        embed = _format_embed(event)
        for sub in proxy_subs:
            try:
                _send_discord(sub.discord_webhook_url, embed)  # type: ignore[arg-type]  # filtered by isnot(None)
                sent += 1
            except Exception:
                logger.exception("Discord notification failed for subscription %s", sub.id)

    if sent:
        logger.info("Sent %d Discord notification(s) for %d event(s)", sent, len(events))


# ---------------------------------------------------------------------------
# Protocol-level governance event notifications
# ---------------------------------------------------------------------------

# Color mapping by severity
_EVENT_COLORS = {
    "ownership_transferred": 0xFF0000,  # red
    "paused": 0xFF0000,
    "unpaused": 0xFF0000,
    "upgraded": 0xFF9900,  # orange
    "admin_changed": 0xFF9900,
    "beacon_upgraded": 0xFF9900,
    "timelock_executed": 0xFF9900,
    "timelock_scheduled": 0x3498DB,  # blue
    "signer_added": 0x3498DB,
    "signer_removed": 0x3498DB,
    "role_granted": 0xF39C12,  # amber
    "role_revoked": 0xF39C12,
    "threshold_changed": 0xF39C12,
    "delay_changed": 0xF39C12,
    "state_changed_poll": 0x9B59B6,  # purple
}


def _format_governance_embed(event: MonitoredEvent) -> dict:
    """Build a Discord embed for a governance/monitoring event."""
    mc = event.monitored_contract
    label = mc.address
    data = event.data or {}

    fields = [
        {"name": "Contract", "value": f"`{mc.address}`", "inline": True},
        {"name": "Chain", "value": mc.chain, "inline": True},
        {"name": "Event", "value": event.event_type, "inline": True},
    ]

    # Add event-specific fields
    if event.event_type == "ownership_transferred":
        if data.get("old_owner"):
            fields.append({"name": "Old Owner", "value": f"`{data['old_owner']}`", "inline": False})
        if data.get("new_owner"):
            fields.append({"name": "New Owner", "value": f"`{data['new_owner']}`", "inline": False})
    elif event.event_type in ("paused", "unpaused"):
        if data.get("account"):
            fields.append({"name": "Account", "value": f"`{data['account']}`", "inline": False})
    elif event.event_type in ("signer_added", "signer_removed"):
        if data.get("owner"):
            fields.append({"name": "Signer", "value": f"`{data['owner']}`", "inline": False})
    elif event.event_type == "threshold_changed":
        if data.get("threshold"):
            fields.append({"name": "New Threshold", "value": str(data["threshold"]), "inline": True})
    elif event.event_type == "delay_changed":
        if data.get("old_delay") is not None:
            fields.append({"name": "Old Delay", "value": str(data["old_delay"]), "inline": True})
        if data.get("new_delay") is not None:
            fields.append({"name": "New Delay", "value": str(data["new_delay"]), "inline": True})
    elif event.event_type == "state_changed_poll":
        if data.get("field"):
            fields.append({"name": "Field", "value": data["field"], "inline": True})
        if data.get("old_value"):
            fields.append({"name": "Old", "value": f"`{data['old_value']}`", "inline": True})
        if data.get("new_value"):
            fields.append({"name": "New", "value": f"`{data['new_value']}`", "inline": True})

    if event.block_number:
        fields.append({"name": "Block", "value": str(event.block_number), "inline": True})
    if event.tx_hash:
        fields.append({"name": "Tx", "value": f"`{event.tx_hash}`", "inline": False})

    color = _EVENT_COLORS.get(event.event_type, 0x95A5A6)

    return {
        "title": f"Protocol Event: {event.event_type} on {label[:10]}...{label[-4:]}",
        "color": color,
        "fields": fields,
    }


def notify_protocol_events(session: Session, events: list[MonitoredEvent]) -> None:
    """Send Discord notifications for detected governance/monitoring events.

    Groups events by protocol_id, loads ProtocolSubscription rows, filters
    by event_filter (if set), and sends Discord embeds.
    """
    if not events:
        return

    # Group events by protocol_id
    events_by_protocol: dict[int, list[MonitoredEvent]] = {}
    for event in events:
        mc = event.monitored_contract
        if mc and mc.protocol_id:
            events_by_protocol.setdefault(mc.protocol_id, []).append(event)

    if not events_by_protocol:
        return

    # Load subscriptions
    protocol_ids = list(events_by_protocol.keys())
    subs = (
        session.execute(
            select(ProtocolSubscription).where(
                ProtocolSubscription.protocol_id.in_(protocol_ids),
                ProtocolSubscription.discord_webhook_url.isnot(None),
            )
        )
        .scalars()
        .all()
    )

    if not subs:
        return

    subs_by_protocol: dict[int, list[ProtocolSubscription]] = {}
    for sub in subs:
        subs_by_protocol.setdefault(sub.protocol_id, []).append(sub)

    sent = 0
    for protocol_id, proto_events in events_by_protocol.items():
        proto_subs = subs_by_protocol.get(protocol_id, [])
        if not proto_subs:
            continue

        for event in proto_events:
            embed = _format_governance_embed(event)
            for sub in proto_subs:
                # Check event filter
                if sub.event_filter and isinstance(sub.event_filter, dict):
                    allowed_types = sub.event_filter.get("event_types")
                    if allowed_types and event.event_type not in allowed_types:
                        continue

                try:
                    _send_discord(sub.discord_webhook_url, embed)  # type: ignore[arg-type]
                    sent += 1
                except Exception:
                    logger.exception(
                        "Discord notification failed for protocol subscription %s",
                        sub.id,
                    )

    if sent:
        logger.info(
            "Sent %d protocol notification(s) for %d event(s)",
            sent,
            len(events),
        )
