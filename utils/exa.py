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
EXA_RESEARCH_CREATE_URL = "https://api.exa.ai/research/v0/tasks"
EXA_RESEARCH_GET_URL = "https://api.exa.ai/research/v0/tasks/{task_id}"
REQUEST_TIMEOUT_SECONDS = 30
DEEP_RESEARCH_POLL_INTERVAL_SECONDS = 5
DEEP_RESEARCH_MAX_POLL_SECONDS = 600


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


_AUDIT_RESEARCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["auditReports"],
    "additionalProperties": False,
    "properties": {
        "auditReports": {
            "type": "array",
            "description": "Audit reports with auditor + URL",
            "items": {
                "type": "object",
                "required": ["auditor", "url"],
                "additionalProperties": False,
                "properties": {
                    "auditor": {"type": "string"},
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "date": {"type": "string"},
                },
            },
        }
    },
}


def deep_research(
    instructions: str,
    *,
    model: str = "exa-research",
    schema: dict[str, Any] | None = None,
    timeout_seconds: int = DEEP_RESEARCH_MAX_POLL_SECONDS,
) -> dict[str, Any]:
    """Run Exa's Deep Research endpoint (multi-step search + synthesis).

    Returns ``{"data": <schema-typed>, "task_id": str, "status": str}``.
    Pass ``schema`` to constrain output; defaults to an audit-report schema
    suitable for this benchmark.
    """
    import time

    api_key = _get_api_key()
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}

    create_payload: dict[str, Any] = {"instructions": instructions, "model": model}
    create_payload["output"] = {"schema": schema or _AUDIT_RESEARCH_SCHEMA}
    resp = requests.post(
        EXA_RESEARCH_CREATE_URL,
        json=create_payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if resp.status_code >= 400:
        raise ExaError(
            normalize_error(
                f"Exa /research create HTTP {resp.status_code}",
                status_code=resp.status_code,
                detail=resp.text[:400],
            )
        )
    task = resp.json()
    task_id = str(task.get("id") or task.get("task_id") or "")
    if not task_id:
        raise ExaError(normalize_error("Exa /research returned no task id", detail=str(task)[:300]))

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        time.sleep(DEEP_RESEARCH_POLL_INTERVAL_SECONDS)
        poll = requests.get(
            EXA_RESEARCH_GET_URL.format(task_id=task_id),
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if poll.status_code >= 400:
            raise ExaError(
                normalize_error(
                    f"Exa /research poll HTTP {poll.status_code}",
                    status_code=poll.status_code,
                    detail=poll.text[:400],
                )
            )
        resp_data = poll.json()
        status = str(resp_data.get("status") or "").lower()
        if status in ("completed", "done", "success"):
            return {"data": resp_data.get("data") or {}, "task_id": task_id, "status": status}
        if status in ("failed", "error", "cancelled"):
            raise ExaError(
                normalize_error(
                    f"Exa /research task {task_id} status={status}",
                    detail=str(resp_data.get("error") or resp_data)[:400],
                )
            )
    raise ExaError(normalize_error(f"Exa /research task {task_id} timed out after {timeout_seconds}s"))
