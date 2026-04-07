"""Resolve a company/protocol name to DefiLlama slug and DApp URL."""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
_protocols_cache: list[dict] | None = None


def _fetch_protocols() -> list[dict]:
    global _protocols_cache
    if _protocols_cache is not None:
        return _protocols_cache
    resp = requests.get(DEFILLAMA_PROTOCOLS_URL, timeout=15)
    resp.raise_for_status()
    _protocols_cache = resp.json()
    return _protocols_cache


def resolve_protocol(name: str) -> dict:
    """Resolve a company name to DefiLlama slug and DApp URL.

    Returns {"slug": str|None, "url": str|None, "name": str|None, "chains": list}.
    """
    name_lower = name.lower().strip()
    try:
        protocols = _fetch_protocols()
    except Exception as exc:
        logger.warning("Failed to fetch DefiLlama protocols: %s", exc)
        return {"slug": None, "url": None, "name": None, "chains": []}

    # Exact slug match first
    for p in protocols:
        if p.get("slug", "").lower() == name_lower:
            return {
                "slug": p["slug"],
                "url": p.get("url"),
                "name": p.get("name"),
                "chains": p.get("chains", []),
            }

    # Exact name match
    for p in protocols:
        if p.get("name", "").lower() == name_lower:
            return {
                "slug": p.get("slug"),
                "url": p.get("url"),
                "name": p.get("name"),
                "chains": p.get("chains", []),
            }

    # Substring match (name contains search term)
    for p in protocols:
        if name_lower in p.get("name", "").lower() or name_lower in p.get("slug", "").lower():
            return {
                "slug": p.get("slug"),
                "url": p.get("url"),
                "name": p.get("name"),
                "chains": p.get("chains", []),
            }

    return {"slug": None, "url": None, "name": None, "chains": []}
