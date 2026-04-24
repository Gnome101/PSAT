#!/usr/bin/env python3
"""Exa (formerly Metaphor) search client — drop-in shape-compatible with
``utils.tavily``.

Exa's neural/auto search mode embeds the query semantically which is a
much better fit for our use case than Tavily's phrase-match: "ether fi"
and "ether.fi" cluster together in embedding space, so quoted / spaced
/ dotted slug variants all return the same high-quality on-protocol
URLs. See ``/tmp/exa_vs_tavily.py`` for the benchmark that motivated
adding this.

Returns objects shaped like Tavily's — ``{title, url, content, score}``
— so the rest of the pipeline (domain-picker, page-picker, classifier)
is agnostic to the backend.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

EXA_SEARCH_URL = "https://api.exa.ai/search"
REQUEST_TIMEOUT_SECONDS = 30


class ExaError(RuntimeError):
    """Raised when Exa cannot return a usable response."""

    def __init__(self, error: dict[str, Any]):
        super().__init__(error.get("error", "Exa request failed"))
        self.error = error


def normalize_error(
    message: str,
    *,
    status_code: int | None = None,
    retryable: bool | None = None,
    detail: str | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"provider": "exa", "error": message}
    if status_code is not None:
        error["status_code"] = status_code
    if retryable is not None:
        error["retryable"] = retryable
    if detail:
        error["detail"] = detail
    return error


def error_from_exception(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, ExaError):
        return dict(exc.error)
    return normalize_error(str(exc), retryable=False)


def _get_api_key() -> str:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    key = (os.environ.get("EXA_API_KEY") or "").strip()
    if not key:
        raise ExaError(
            normalize_error(
                "Missing EXA_API_KEY environment variable",
                retryable=False,
            )
        )
    return key


def search(
    query: str,
    max_results: int,
    mode: str = "auto",
    include_text: bool = True,
) -> list[dict[str, Any]]:
    """Search Exa and return Tavily-compatible result dicts.

    Fields returned per result: ``url``, ``title``, ``content``, ``score``.
    Supported modes (with aliases used in the benchmark todolist):

    - ``"deep"``     → ``"neural"``   — embedding-based, best recall on fuzzy slugs
    - ``"regular"``  → ``"auto"``     — Exa picks neural vs keyword per-query
    - ``"instant"``  → ``"keyword"``  — fastest, phrase-match only
    """
    clean_query = query.strip()
    if not clean_query:
        raise ValueError("query must not be empty")
    if max_results < 1:
        raise ValueError("max_results must be >= 1")
    mode_aliases = {"deep": "neural", "regular": "auto", "instant": "keyword"}
    resolved_mode = mode_aliases.get(mode, mode)
    if resolved_mode not in ("auto", "neural", "keyword"):
        raise ValueError(f"unsupported mode: {mode!r}")

    payload: dict[str, Any] = {
        "query": clean_query,
        "numResults": max_results,
        "type": resolved_mode,
    }
    if include_text:
        # Short snippet is enough for the classifier / domain picker — we
        # fetch full pages directly when we need their body.
        payload["contents"] = {"text": {"maxCharacters": 300}}

    headers = {
        "x-api-key": _get_api_key(),
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            EXA_SEARCH_URL,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise ExaError(normalize_error(f"Exa network error: {exc}", retryable=True)) from exc

    if resp.status_code >= 400:
        retryable = resp.status_code in (429, 500, 502, 503, 504)
        raise ExaError(
            normalize_error(
                f"Exa HTTP {resp.status_code}",
                status_code=resp.status_code,
                retryable=retryable,
                detail=resp.text[:300],
            )
        )

    data = resp.json()
    results = data.get("results") or []
    normalized: list[dict[str, Any]] = []
    for item in results:
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        text = item.get("text") or item.get("content") or ""
        if isinstance(text, dict):
            text = text.get("text", "") or ""
        normalized.append(
            {
                "url": url,
                "title": (item.get("title") or "").strip(),
                # Tavily consumers read ``content``; keep that field name.
                "content": str(text)[:1000],
                "score": item.get("score"),
            }
        )
    return normalized
