"""Discord notification dispatch for proxy upgrade events."""

from __future__ import annotations

import logging

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import ProxySubscription, ProxyUpgradeEvent

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
