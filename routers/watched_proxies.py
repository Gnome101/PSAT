"""Watched-proxy registration, listing, and event/subscription management."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from db.models import (
    MonitoredContract,
    ProxySubscription,
    ProxyUpgradeEvent,
    WatchedProxy,
)
from schemas.api_requests import SubscribeRequest, WatchProxyRequest

from . import deps

router = APIRouter()


def _watched_proxy_to_dict(proxy: WatchedProxy) -> dict[str, Any]:
    return {
        "id": str(proxy.id),
        "proxy_address": proxy.proxy_address,
        "chain": proxy.chain,
        "label": proxy.label,
        "proxy_type": proxy.proxy_type,
        "needs_polling": proxy.needs_polling,
        "last_known_implementation": proxy.last_known_implementation,
        "last_scanned_block": proxy.last_scanned_block,
        "created_at": proxy.created_at.isoformat(),
    }


@router.post("/api/watched-proxies", dependencies=[Depends(deps.require_admin_key)])
def add_watched_proxy(request: WatchProxyRequest) -> dict[str, Any]:
    """Subscribe to proxy upgrade notifications."""
    if not request.address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Address must start with 0x")
    address = request.address.lower()

    # Resolve RPC URL: explicit param > env default
    rpc_url = request.rpc_url or deps.DEFAULT_RPC_URL

    # Block SSRF: reject non-http(s) schemes and private/internal URLs
    # Skip check when using the server's own default RPC (from ETH_RPC env var)
    if request.rpc_url:
        from urllib.parse import urlparse

        parsed = urlparse(rpc_url)
        if parsed.scheme not in ("http", "https"):
            raise HTTPException(status_code=400, detail="rpc_url must use http or https")
        hostname = parsed.hostname or ""
        if (
            hostname in ("localhost", "127.0.0.1", "0.0.0.0", "::1")
            or hostname.startswith("169.254.")
            or hostname.startswith("10.")
            or hostname.startswith("192.168.")
        ):
            raise HTTPException(status_code=400, detail="rpc_url must not point to internal addresses")

    # Classify the proxy to determine type, needs_polling, and current implementation
    from services.discovery.classifier import _KNOWN_EVENT_PROXY_TYPES, classify_single
    from services.monitoring.proxy_watcher import get_latest_block, resolve_current_implementation

    proxy_type = None
    needs_polling = False
    try:
        classification = classify_single(address, rpc_url)
        if classification.get("type") == "proxy":
            proxy_type = classification.get("proxy_type")
            needs_polling = proxy_type not in _KNOWN_EVENT_PROXY_TYPES
    except Exception:
        pass  # classification failure is non-fatal — watch with fallback resolution

    current_impl = resolve_current_implementation(address, rpc_url, proxy_type=proxy_type)

    # Starting scan point: explicit from_block > current block
    if request.from_block is not None:
        from_block = request.from_block
    else:
        try:
            from_block = get_latest_block(rpc_url)
        except Exception:
            raise HTTPException(
                status_code=502,
                detail="Could not determine current block. Provide from_block explicitly.",
            )

    with deps.SessionLocal() as session:
        existing = session.execute(
            select(WatchedProxy).where(
                WatchedProxy.proxy_address == address,
                WatchedProxy.chain == request.chain,
            )
        ).scalar_one_or_none()

        if existing:
            proxy = existing
        else:
            proxy = WatchedProxy(
                proxy_address=address,
                chain=request.chain,
                label=request.label,
                proxy_type=proxy_type,
                needs_polling=needs_polling,
                last_known_implementation=current_impl,
                last_scanned_block=from_block,
            )
            session.add(proxy)
            session.flush()

        subscription = None
        if request.discord_webhook_url:
            subscription = ProxySubscription(
                watched_proxy_id=proxy.id,
                discord_webhook_url=request.discord_webhook_url,
                label=request.label,
            )
            session.add(subscription)

        # Also create a MonitoredContract for unified monitoring
        existing_mc = session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == address,
                MonitoredContract.chain == request.chain,
            )
        ).scalar_one_or_none()
        if not existing_mc:
            mc = MonitoredContract(
                address=address,
                chain=request.chain,
                contract_type="proxy",
                watched_proxy_id=proxy.id,
                monitoring_config={"watch_upgrades": True, "watch_ownership": True},
                last_known_state={"implementation": current_impl} if current_impl else {},
                last_scanned_block=from_block,
                needs_polling=needs_polling,
                is_active=True,
                enrollment_source="proxy_watch",
            )
            session.add(mc)

        session.commit()
        session.refresh(proxy)
        result = _watched_proxy_to_dict(proxy)
        if subscription:
            session.refresh(subscription)
            result["subscription_id"] = str(subscription.id)
        return result


@router.get("/api/watched-proxies")
def list_watched_proxies() -> list[dict[str, Any]]:
    """List all watched proxy contracts."""
    with deps.SessionLocal() as session:
        stmt = select(WatchedProxy).order_by(WatchedProxy.created_at.desc())
        proxies = session.execute(stmt).scalars().all()
        return [_watched_proxy_to_dict(p) for p in proxies]


@router.delete("/api/watched-proxies/{proxy_id}", dependencies=[Depends(deps.require_admin_key)])
def remove_watched_proxy(proxy_id: str) -> dict[str, str]:
    """Stop watching a proxy contract."""
    with deps.SessionLocal() as session:
        proxy = session.get(WatchedProxy, uuid.UUID(proxy_id))
        if proxy is None:
            raise HTTPException(status_code=404, detail="Watched proxy not found")
        session.delete(proxy)
        session.commit()
        return {"status": "removed"}


@router.get("/api/proxy-events")
def list_proxy_events(proxy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """List detected proxy upgrade events."""
    with deps.SessionLocal() as session:
        stmt = select(ProxyUpgradeEvent).order_by(ProxyUpgradeEvent.detected_at.desc()).limit(limit)
        if proxy_id:
            stmt = stmt.where(ProxyUpgradeEvent.watched_proxy_id == proxy_id)
        events = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(e.id),
                "watched_proxy_id": str(e.watched_proxy_id),
                "block_number": e.block_number,
                "tx_hash": e.tx_hash,
                "old_implementation": e.old_implementation,
                "new_implementation": e.new_implementation,
                "event_type": e.event_type,
                "detected_at": e.detected_at.isoformat(),
            }
            for e in events
        ]


@router.get("/api/watched-proxies/{proxy_id}/subscriptions")
def list_subscriptions(proxy_id: str) -> list[dict[str, Any]]:
    """List notification subscriptions for a watched proxy."""
    with deps.SessionLocal() as session:
        proxy = session.get(WatchedProxy, uuid.UUID(proxy_id))
        if proxy is None:
            raise HTTPException(status_code=404, detail="Watched proxy not found")
        stmt = select(ProxySubscription).where(ProxySubscription.watched_proxy_id == proxy.id)
        subs = session.execute(stmt).scalars().all()
        return [
            {
                "id": str(s.id),
                "watched_proxy_id": str(s.watched_proxy_id),
                "discord_webhook_url": s.discord_webhook_url,
                "label": s.label,
                "created_at": s.created_at.isoformat(),
            }
            for s in subs
        ]


@router.post("/api/watched-proxies/{proxy_id}/subscriptions", dependencies=[Depends(deps.require_admin_key)])
def add_subscription(proxy_id: str, request: SubscribeRequest) -> dict[str, Any]:
    """Add a notification subscription to an existing watched proxy."""
    with deps.SessionLocal() as session:
        proxy = session.get(WatchedProxy, uuid.UUID(proxy_id))
        if proxy is None:
            raise HTTPException(status_code=404, detail="Watched proxy not found")
        sub = ProxySubscription(
            watched_proxy_id=proxy.id,
            discord_webhook_url=request.discord_webhook_url,
            label=request.label,
        )
        session.add(sub)
        session.commit()
        session.refresh(sub)
        return {
            "id": str(sub.id),
            "watched_proxy_id": str(sub.watched_proxy_id),
            "discord_webhook_url": sub.discord_webhook_url,
            "label": sub.label,
            "created_at": sub.created_at.isoformat(),
        }


@router.delete("/api/subscriptions/{subscription_id}", dependencies=[Depends(deps.require_admin_key)])
def remove_subscription(subscription_id: str) -> dict[str, str]:
    """Remove a notification subscription."""
    with deps.SessionLocal() as session:
        sub = session.get(ProxySubscription, uuid.UUID(subscription_id))
        if sub is None:
            raise HTTPException(status_code=404, detail="Subscription not found")
        session.delete(sub)
        session.commit()
        return {"status": "removed"}
