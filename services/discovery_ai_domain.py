"""LLM-based domain selection and contract-listing page discovery."""

from __future__ import annotations

import json
import re
from collections import defaultdict

import requests as _requests

from utils import llm, tavily

from services.discovery_ai_evidence import (
    DOMAIN_RE,
    URL_RE,
    _debug_log,
    _get_domain,
    _is_allowed_domain,
    _is_explorer_domain,
    _is_low_trust_domain,
)


def _maybe_domain(value: str) -> str | None:
    clean = value.strip().lower().replace("https://", "").replace("http://", "").split("/")[0]
    if clean.startswith("www."):
        clean = clean[4:]
    if " " in clean or not DOMAIN_RE.match(clean) or _is_explorer_domain(clean):
        return None
    return clean


def _tavily_search(
    query: str,
    max_results: int,
    queries_used: list[int],
    max_queries: int,
    errors: list[dict],
    debug: bool = False,
) -> list[dict]:
    """Run a single Tavily search, respecting query budget.

    Always uses include_raw_content=False — page content is fetched directly
    via HTTP where needed, avoiding Tavily's per-result content charges.
    """
    if queries_used[0] >= max_queries:
        _debug_log(debug, f"Skipping Tavily query (budget exhausted): {query!r}")
        return []
    queries_used[0] += 1
    _debug_log(
        debug,
        f"Tavily query {queries_used[0]}/{max_queries}: {query!r} (max_results={max_results})",
    )
    try:
        results = tavily.search(
            query,
            max_results=max_results,
            topic="general",
            include_raw_content=False,
        )
        _debug_log(debug, f"Tavily returned {len(results)} result(s)")
        return results
    except (tavily.TavilyError, _requests.RequestException) as exc:
        errors.append(tavily.error_from_exception(exc))
        _debug_log(debug, f"Tavily query failed: {exc!r}")
        return []


def _llm_select_domain(
    results: list[dict],
    company: str,
    debug: bool = False,
) -> str | None:
    """Ask the LLM to identify the official domain for a company from search results."""
    if not results:
        return None

    # Collect unique non-explorer, non-low-trust domains with their URLs/titles
    domain_info: dict[str, list[str]] = defaultdict(list)
    for r in results:
        url = str(r.get("url", "")).strip()
        title = str(r.get("title", "")).strip()
        if not url:
            continue
        domain = _get_domain(url)
        if not domain or _is_explorer_domain(domain) or _is_low_trust_domain(domain):
            continue
        domain_info[domain].append(title or url)

    if not domain_info:
        return None

    # Present as numbered choices (more reliable across LLM models)
    choices = "\n".join(
        f"{i + 1}. {domain} (pages: {', '.join(titles[:2])})"
        for i, (domain, titles) in enumerate(
            sorted(domain_info.items(), key=lambda x: -len(x[1]))
        )
    )

    prompt = (
        f"Which of these is the official documentation or website for the "
        f"{company} protocol?\n\n{choices}\n\n"
        f"Reply with ONLY the number."
    )

    try:
        response = llm.chat(
            [{"role": "user", "content": prompt}],
            max_tokens=32,
            temperature=0.0,
        )
        # Accept only purely numeric replies (e.g., "2"), not arbitrary text with digits.
        choice = response.strip()
        if re.fullmatch(r"\d+", choice):
            idx = int(choice) - 1
            sorted_domains = sorted(domain_info.keys(), key=lambda d: -len(domain_info[d]))
            if 0 <= idx < len(sorted_domains):
                selected = sorted_domains[idx]
                _debug_log(debug, f"LLM selected domain #{idx + 1}: {selected}")
                return selected
        _debug_log(debug, f"LLM returned unparseable response: {response!r}")
    except (_requests.RequestException, json.JSONDecodeError, RuntimeError) as exc:
        _debug_log(debug, f"LLM domain selection failed: {exc!r}")
    return None


def _discover_contract_listing_pages(
    domain: str,
    company: str,
    queries_used: list[int],
    max_queries: int,
    errors: list[dict],
    debug: bool = False,
) -> tuple[list[dict], list[str]]:
    """Search the official domain for pages, then ask the LLM which ones list contract addresses.

    Returns (tavily_results, recommended_urls).  Falls back gracefully if the
    LLM call fails — recommended_urls will be empty.
    """
    _debug_log(debug, f"Phase 2: discovering contract-listing pages on domain={domain}")
    results = _tavily_search(
        f"site:{domain} {company} smart contract addresses",
        max_results=10,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
        debug=debug,
    )
    if not results:
        _debug_log(debug, "No page-discovery results returned")
        return [], []

    # Collect unique URLs from this domain with their titles
    seen_urls: set[str] = set()
    page_info: list[dict[str, str]] = []
    for r in results:
        url = str(r.get("url", "")).strip()
        title = str(r.get("title", "")).strip()
        if not url or url in seen_urls:
            continue
        if _is_allowed_domain(_get_domain(url), [domain]):
            seen_urls.add(url)
            page_info.append({"url": url, "title": title})

    if not page_info:
        _debug_log(debug, "No unique in-domain pages found for LLM recommendation")
        return results, []

    # Ask the LLM to pick the most relevant page(s)
    page_list = "\n".join(f"- {p['title']}: {p['url']}" for p in page_info)
    prompt = (
        f"Below are pages from the {company} documentation site ({domain}).\n"
        f"Which of these pages is most likely to contain a comprehensive, "
        f"authoritative list of deployed smart contract addresses?\n\n"
        f"{page_list}\n\n"
        f"Reply with ONLY the best URL(s), one per line — nothing else."
    )

    try:
        response = llm.chat(
            [{"role": "user", "content": prompt}],
            max_tokens=2048,
            temperature=0.0,
        )
        recommended: list[str] = []
        for url_match in URL_RE.findall(response):
            clean_url = url_match.rstrip(".,;:!?)")
            if _is_allowed_domain(_get_domain(clean_url), [domain]):
                recommended.append(clean_url)
        _debug_log(debug, f"LLM recommended {len(recommended)} in-domain URL(s)")
        return results, recommended
    except (_requests.RequestException, json.JSONDecodeError, RuntimeError) as exc:
        # LLM unavailable — return results without recommendations
        _debug_log(debug, f"LLM recommendation step failed: {exc!r}")
        return results, []
