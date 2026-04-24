#!/usr/bin/env python3
"""Brave Search client — Tavily-shape-compatible."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
REQUEST_TIMEOUT_SECONDS = 20


class BraveError(RuntimeError):
    def __init__(self, error: dict[str, Any]):
        super().__init__(error.get("error", "Brave request failed"))
        self.error = error


def normalize_error(
    message: str,
    *,
    status_code: int | None = None,
    retryable: bool | None = None,
    detail: str | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"provider": "brave", "error": message}
    if status_code is not None:
        error["status_code"] = status_code
    if retryable is not None:
        error["retryable"] = retryable
    if detail:
        error["detail"] = detail
    return error


def error_from_exception(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, BraveError):
        return dict(exc.error)
    return normalize_error(str(exc), retryable=False)


def _get_api_key() -> str:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    key = (os.environ.get("BRAVE_API_KEY") or "").strip()
    if not key:
        raise BraveError(
            normalize_error(
                "Missing BRAVE_API_KEY environment variable",
                retryable=False,
            )
        )
    return key


def search(
    query: str,
    max_results: int,
    mode: str = "default",
) -> list[dict[str, Any]]:
    """Search Brave and return Tavily-compatible result dicts.

    ``mode`` is ignored — Brave's free tier only exposes one web-search
    endpoint. Kept in the signature for symmetry with exa.search().
    """
    del mode
    clean_query = query.strip()
    if not clean_query:
        raise ValueError("query must not be empty")
    if max_results < 1:
        raise ValueError("max_results must be >= 1")

    headers = {
        "X-Subscription-Token": _get_api_key(),
        "Accept": "application/json",
    }
    params = {
        "q": clean_query,
        "count": min(max_results, 20),  # Brave caps count at 20 per request
    }
    try:
        resp = requests.get(
            BRAVE_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise BraveError(normalize_error(f"Brave network error: {exc}", retryable=True)) from exc

    if resp.status_code >= 400:
        retryable = resp.status_code in (429, 500, 502, 503, 504)
        raise BraveError(
            normalize_error(
                f"Brave HTTP {resp.status_code}",
                status_code=resp.status_code,
                retryable=retryable,
                detail=resp.text[:300],
            )
        )

    data = resp.json()
    items = (data.get("web") or {}).get("results") or []
    normalized: list[dict[str, Any]] = []
    for item in items:
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        description = item.get("description") or item.get("snippet") or ""
        normalized.append(
            {
                "url": url,
                "title": (item.get("title") or "").strip(),
                "content": str(description)[:1000],
                "score": None,
            }
        )
    return normalized
