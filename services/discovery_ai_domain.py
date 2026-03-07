"""LLM-based domain selection and discovery of relevant contract pages."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

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


def _domain_candidates_from_results(results: list[dict[str, Any]]) -> list[str]:
    """Return ordered non-explorer, non-low-trust domains seen in search results."""
    domain_info: dict[str, list[str]] = defaultdict(list)
    for result in results:
        url = str(result.get("url", "")).strip()
        title = str(result.get("title", "")).strip()
        if not url:
            continue
        domain = _get_domain(url)
        if not domain or _is_explorer_domain(domain) or _is_low_trust_domain(domain):
            continue
        domain_info[domain].append(title or url)
    return sorted(domain_info.keys(), key=lambda d: (-len(domain_info[d]), d))


def _collect_in_domain_pages(results: list[dict[str, Any]], domain: str) -> list[dict[str, str]]:
    """Collect unique page URLs, titles, and snippets for a domain."""
    seen_urls: set[str] = set()
    page_info: list[dict[str, str]] = []
    for result in results:
        url = str(result.get("url", "")).strip()
        title = str(result.get("title", "")).strip()
        snippet = str(result.get("content", "")).strip()
        if not url or url in seen_urls:
            continue
        if not _is_allowed_domain(_get_domain(url), [domain]):
            continue
        seen_urls.add(url)
        page_info.append({"url": url, "title": title, "snippet": snippet})
    return page_info


def _llm_select_pages(
    page_info: list[dict[str, str]],
    company: str,
    domain: str,
    prompt: str,
    debug: bool = False,
) -> list[str]:
    """Ask the LLM to choose the most relevant in-domain pages from a candidate list."""
    if not page_info:
        return []

    page_list = "\n".join(
        (
            f"- {page['title'] or '(untitled)'}: {page['url']}\n"
            f"  Snippet: {(page['snippet'] or 'none')[:240]}"
        )
        for page in page_info
    )

    try:
        response = llm.chat(
            [{"role": "user", "content": prompt.format(company=company, domain=domain, page_list=page_list)}],
            max_tokens=2048,
            temperature=0.0,
        )
        recommended: list[str] = []
        seen: set[str] = set()
        for url_match in URL_RE.findall(response):
            clean_url = url_match.rstrip(".,;:!?)")
            if clean_url in seen:
                continue
            if _is_allowed_domain(_get_domain(clean_url), [domain]):
                seen.add(clean_url)
                recommended.append(clean_url)
        _debug_log(debug, f"LLM recommended {len(recommended)} in-domain URL(s)")
        return recommended
    except (_requests.RequestException, json.JSONDecodeError, RuntimeError) as exc:
        _debug_log(debug, f"LLM page selection failed: {exc!r}")
        return []


def _dedupe_results_by_url(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge search results by URL, preferring richer snippets."""
    by_url: dict[str, dict[str, Any]] = {}
    for result in results:
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        prev = by_url.get(url)
        if prev is None:
            by_url[url] = result
            continue
        prev_content = str(prev.get("content", "")).strip()
        new_content = str(result.get("content", "")).strip()
        if len(new_content) > len(prev_content):
            merged = dict(prev)
            merged.update(result)
            by_url[url] = merged
    return list(by_url.values())


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

    page_info = _collect_in_domain_pages(results, domain)
    if not page_info:
        _debug_log(debug, "No unique in-domain pages found for LLM recommendation")
        return results, []

    prompt = (
        f"Below are pages from the {company} documentation site ({domain}).\n"
        f"Which of these pages is most likely to contain a comprehensive, "
        f"authoritative list of deployed smart contract addresses?\n\n"
        "{page_list}\n\n"
        f"Reply with ONLY the best URL(s), one per line — nothing else."
    )
    return results, _llm_select_pages(page_info, company, domain, prompt, debug=debug)


def _discover_contract_inventory_pages(
    domain: str,
    company: str,
    broad_results: list[dict[str, Any]],
    queries_used: list[int],
    max_queries: int,
    errors: list[dict],
    debug: bool = False,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Discover pages that likely contain official multi-contract inventory information."""
    _debug_log(debug, f"Inventory page discovery on domain={domain}")
    site_results = _tavily_search(
        f"site:{domain} {company} contract addresses deployments smart contracts",
        max_results=12,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
        debug=debug,
    )

    combined = _dedupe_results_by_url(
        [r for r in broad_results if _is_allowed_domain(_get_domain(str(r.get("url", ""))), [domain])] + site_results
    )
    if not combined:
        _debug_log(debug, "No inventory page candidates returned")
        return [], []

    page_info = _collect_in_domain_pages(combined, domain)
    if not page_info:
        _debug_log(debug, "No in-domain inventory page candidates available")
        return combined, []

    prompt = (
        "Below are pages from the {company} documentation or official website ({domain}).\n"
        "Which of these pages are most likely to contain an authoritative inventory of deployed "
        "smart contract addresses for the protocol?\n"
        "Prefer pages that enumerate multiple contracts, chains, or deployments. Avoid blog posts, "
        "news, governance posts, and pages about only one isolated contract unless they clearly link to "
        "a broader contract inventory.\n\n"
        "{page_list}\n\n"
        "Reply with ONLY the best URL(s), one per line — nothing else."
    )
    recommended = _llm_select_pages(page_info, company, domain, prompt, debug=debug)
    return combined, recommended
