"""Resolve a company/protocol name to DefiLlama slug and DApp URL."""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher

import requests

logger = logging.getLogger(__name__)

DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
_protocols_cache: list[dict] | None = None

_SIMILARITY_THRESHOLD = 0.90


def _fetch_protocols() -> list[dict]:
    global _protocols_cache
    if _protocols_cache is not None:
        return _protocols_cache
    resp = requests.get(DEFILLAMA_PROTOCOLS_URL, timeout=15)
    resp.raise_for_status()
    # Sort by TVL descending so the most important protocol wins on ties
    data = resp.json()
    data.sort(key=lambda p: p.get("tvl") or 0, reverse=True)
    _protocols_cache = data
    return data


def _normalize(s: str) -> str:
    """Strip punctuation, spaces, and lowercase for comparison."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _make_result(primary: dict, siblings: list[dict]) -> dict:
    return {
        "slug": primary.get("slug"),
        "url": primary.get("url"),
        "name": primary.get("name"),
        "chains": primary.get("chains", []),
        "all_slugs": [s.get("slug") for s in siblings if s.get("slug")],
    }


def _find_siblings(match: dict, protocols: list[dict]) -> list[dict]:
    """Return all protocols sharing the same parentProtocol, sorted by TVL desc.

    If the match has no parent, returns just the match itself.
    """
    parent = match.get("parentProtocol")
    if not parent:
        return [match]
    return [p for p in protocols if p.get("parentProtocol") == parent]


def _match_protocol(name: str, protocols: list[dict]) -> dict | None:
    """Find the best matching protocol entry. Returns None if no match."""
    name_lower = name.lower().strip()
    name_norm = _normalize(name)

    if not name_norm:
        return None

    # 1. Exact slug match
    for p in protocols:
        if p.get("slug", "").lower() == name_lower:
            return p

    # 2. Exact name match
    for p in protocols:
        if p.get("name", "").lower() == name_lower:
            return p

    # 3. Normalized match (etherfi == ether.fi == ether-fi)
    for p in protocols:
        if _normalize(p.get("slug", "")) == name_norm or _normalize(p.get("name", "")) == name_norm:
            return p

    # 4. Normalized substring — require search term covers ≥50% of target
    for p in protocols:
        slug_norm = _normalize(p.get("slug", ""))
        p_name_norm = _normalize(p.get("name", ""))
        if slug_norm and name_norm in slug_norm and len(name_norm) / len(slug_norm) >= 0.5:
            return p
        if p_name_norm and name_norm in p_name_norm and len(name_norm) / len(p_name_norm) >= 0.5:
            return p

    # 5. Similarity search — compare normalized strings, pick best above threshold
    best_score = 0.0
    best_protocol = None
    for p in protocols:
        slug_score = SequenceMatcher(None, name_norm, _normalize(p.get("slug", ""))).ratio()
        name_score = SequenceMatcher(None, name_norm, _normalize(p.get("name", ""))).ratio()
        score = max(slug_score, name_score)
        if score > best_score:
            best_score = score
            best_protocol = p

    if best_protocol and best_score >= _SIMILARITY_THRESHOLD:
        logger.info(
            "Fuzzy matched '%s' → '%s' (slug=%s, score=%.2f)",
            name,
            best_protocol.get("name"),
            best_protocol.get("slug"),
            best_score,
        )
        return best_protocol

    return None


def resolve_protocol(name: str) -> dict:
    """Resolve a company name to DefiLlama slug and DApp URL.

    Finds the best matching protocol, then returns it along with all sibling
    protocols sharing the same ``parentProtocol`` (e.g. "etherfi" returns
    ether.fi-stake, ether.fi-liquid, etherfi-cash-liquid, etc.).

    Returns {"slug", "url", "name", "chains", "all_slugs"}.
    """
    try:
        protocols = _fetch_protocols()
    except Exception as exc:
        logger.warning("Failed to fetch DefiLlama protocols: %s", exc)
        return {"slug": None, "url": None, "name": None, "chains": [], "all_slugs": []}

    match = _match_protocol(name, protocols)
    if not match:
        return {"slug": None, "url": None, "name": None, "chains": [], "all_slugs": []}

    siblings = _find_siblings(match, protocols)
    result = _make_result(match, siblings)
    if len(siblings) > 1:
        logger.info(
            "Resolved '%s' → %s (%d sibling protocols)",
            name,
            match.get("slug"),
            len(siblings),
        )
    return result
