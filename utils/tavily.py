#!/usr/bin/env python3
"""Tavily search client with retries and normalized errors."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 2
BACKOFF_BASE_SECONDS = 0.75


def normalize_error(
    message: str,
    *,
    status_code: int | None = None,
    retryable: bool | None = None,
    detail: str | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"provider": "tavily", "error": message}
    if status_code is not None:
        error["status_code"] = status_code
    if retryable is not None:
        error["retryable"] = retryable
    if detail:
        error["detail"] = detail
    return error


class TavilyError(RuntimeError):
    """Raised when Tavily cannot return a usable response."""

    def __init__(self, error: dict[str, Any]):
        super().__init__(error.get("error", "Tavily request failed"))
        self.error = error


def error_from_exception(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, TavilyError):
        return dict(exc.error)
    return normalize_error(str(exc), retryable=False)


def _build_payload(
    query: str,
    max_results: int,
    topic: str,
    search_depth: str,
    include_raw_content: bool,
) -> dict[str, Any]:
    api_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not api_key:
        raise TavilyError(
            normalize_error(
                "Missing TAVILY_API_KEY environment variable",
                retryable=False,
            )
        )

    return {
        "api_key": api_key,
        "query": query,
        "max_results": max_results,
        "topic": topic,
        "search_depth": search_depth,
        "include_raw_content": include_raw_content,
    }


def search(
    query: str,
    max_results: int,
    topic: str = "general",
    search_depth: str = "advanced",
    include_raw_content: bool = True,
) -> list[dict[str, Any]]:
    """Search Tavily and return the normalized list of result objects."""
    clean_query = query.strip()
    if not clean_query:
        raise ValueError("query must not be empty")
    if max_results < 1:
        raise ValueError("max_results must be >= 1")

    payload = _build_payload(
        clean_query,
        max_results=max_results,
        topic=topic,
        search_depth=search_depth,
        include_raw_content=include_raw_content,
    )

    last_error: TavilyError | None = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.post(
                TAVILY_SEARCH_URL,
                json=payload,
                headers={"Accept": "application/json", "User-Agent": "PSAT/0.1"},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code >= 400:
                retryable = response.status_code >= 500 or response.status_code == 429
                raise TavilyError(
                    normalize_error(
                        f"Tavily API returned HTTP {response.status_code}",
                        status_code=response.status_code,
                        retryable=retryable,
                        detail=response.text[:400].strip() or None,
                    )
                )

            data = response.json()
            results = data.get("results", [])
            if not isinstance(results, list):
                raise TavilyError(
                    normalize_error(
                        "Tavily response did not include a list for 'results'",
                        retryable=False,
                    )
                )
            return [item for item in results if isinstance(item, dict)]

        except requests.Timeout as exc:
            last_error = TavilyError(
                normalize_error(
                    "Tavily request timed out",
                    retryable=True,
                    detail=str(exc),
                )
            )
        except requests.RequestException as exc:
            last_error = TavilyError(
                normalize_error(
                    "Tavily request failed",
                    retryable=True,
                    detail=str(exc),
                )
            )
        except ValueError as exc:
            last_error = TavilyError(
                normalize_error(
                    "Invalid JSON returned from Tavily",
                    retryable=False,
                    detail=str(exc),
                )
            )
        except TavilyError as exc:
            last_error = exc

        if not last_error:
            continue
        if attempt >= MAX_RETRIES or not bool(last_error.error.get("retryable")):
            raise last_error
        time.sleep(BACKOFF_BASE_SECONDS * (2**attempt))

    if last_error:
        raise last_error
    raise TavilyError(normalize_error("Unknown Tavily error", retryable=False))
