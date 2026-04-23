"""LLM-based classification and extraction for audit report discovery.

Stage 1 — classify_search_results: given Tavily search results, determine which
are actual third-party audit reports and extract basic metadata.

Stage 2 — extract_report_details: given a fetched page's text, extract structured
audit metadata AND discover links to additional audit reports on the page.
"""

from __future__ import annotations

import json
import re
from typing import Any

from utils import llm

from .inventory_domain import _debug_log

_JSON_ARRAY_RE = re.compile(r"\[.*\]", re.DOTALL)
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)
_MARKDOWN_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)```", re.DOTALL)


def _parse_json_array(text: str) -> list[dict[str, Any]] | None:
    """Best-effort extraction of a JSON array from LLM output."""
    # Strip markdown fences first
    fence_match = _MARKDOWN_FENCE_RE.search(text)
    if fence_match:
        text = fence_match.group(1).strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass

    # Try to extract the first [...] block
    match = _JSON_ARRAY_RE.search(text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass

    return None


def _parse_json_object(text: str) -> dict[str, Any] | None:
    """Best-effort extraction of a JSON object from LLM output."""
    fence_match = _MARKDOWN_FENCE_RE.search(text)
    if fence_match:
        text = fence_match.group(1).strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass

    match = _JSON_OBJECT_RE.search(text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass

    return None


_KNOWN_AUDITORS = (
    "Trail of Bits (often abbreviated ToB), OpenZeppelin (often OZ), Spearbit, "
    "Consensys Diligence, Halborn, Quantstamp, Sigma Prime, Sherlock, Code4rena (C4), "
    "Cantina, Zellic, Cyfrin, Hexens, MixBytes, Dedaub, "
    "Nethermind (their report IDs are formatted NM-#### or NM_####), ChainSecurity, "
    "Certora, PeckShield, SlowMist, Ackee, Omniscia, Solidified, Paladin, Decurity, "
    "Pashov, OtterSec, Statemind, Runtime Verification, Hats Finance, "
    "Pessimistic, Blackthorn, ABDK Consulting, CertiK, Lexfo, Securing, "
    "Immunefi, Zokyo, Kudelski Security, Secure3, Veridise, Chainlight, "
    "0xMacro, Guardian Audits, Three Sigma, Pragma, Certik, Solidprof, "
    "Sec3 (formerly Soteria), Fuzzland, Oak Security, Block Analitica"
)

_CLASSIFICATION_PROMPT = """\
You are triaging web search results to find third-party security audit material \
about the **{company}** smart contract protocol. Be GENEROUS — downstream code \
validates each hit before it reaches the user, so the cost of including a weak \
match is low, and the cost of rejecting a real audit is high.

Protocol name note: the slug may be stylized different ways — "{company}", the \
same with dots/hyphens/underscores instead of spaces, lowercased, or \
TitleCased. Treat all of these as the same protocol (e.g. "ether fi", \
"ether.fi", "Ether.Fi", "etherfi", "EtherFi" are the same).

An audit report is any document or page produced by an independent security \
firm that reviews the protocol's smart contracts. Common firms: {auditors}. \
Audits are often hosted on:
  - the firm's own publication directory (zellic, spearbit, trailofbits, etc.)
  - the protocol's docs/GitBook/security page
  - the protocol's GitHub repo under audits/ or similar
  - audit-platform aggregators (Code4rena, Sherlock, Cantina — include these \
when they clearly host a {company} report, even if the aggregator covers many \
protocols)
  - case studies and auditor blog posts that describe the {company} engagement

Mark is_audit: true for any of:
  - a direct PDF audit report on {company}
  - a listing page that indexes multiple {company} audits (mark type="listing")
  - an auditor's own case-study / blog post that describes a {company} audit
  - a GitHub repo path containing {company} audit files
  - a bug-bounty program page if it clearly links to the protocol's audits \
(otherwise mark false)

Only mark is_audit: false when the result is clearly unrelated, such as:
  - an audit for a DIFFERENT protocol whose name doesn't resemble {company}
  - generic "we do audits" marketing pages with no {company} reference
  - academic papers, journalism, or news summaries unrelated to specific audits
  - protocol docs that merely mention security at a high level

Search results:
{results}

Look at ALL of: the title, the snippet, and the URL path/filename. Auditor \
names and dates are often embedded in the URL filename (e.g. \
``Omniscia_Audit_EtherFi.pdf``, ``2024.06.25 - Halborn - EFIP.pdf``), or \
encoded as report IDs that follow a known auditor's convention. Use your \
knowledge of common auditor naming patterns to resolve those to a firm name.

Reply with ONLY a JSON array. Each element must have:
- "url": the exact URL from the search result
- "is_audit": true if this is plausibly an audit/audit-listing for {company}
- "type": "report" for a single audit report, "listing" for an index page, \
"pdf" for a direct PDF, "case_study" for an auditor's case-study page, \
"bounty" for a bug-bounty page that hosts audits, null if not an audit
- "auditor": name of the auditing firm if identifiable from the title, \
snippet, OR url filename — null only when truly no signal is available
- "title": short clean human-readable title (drop boilerplate like "[PDF]"), \
null if not applicable
- "date": audit date as YYYY-MM-DD if visible in the title, snippet or URL \
(or YYYY-MM / YYYY), null if not present
- "confidence": 0.0 to 1.0. Use >=0.5 when the title or URL contains a name \
variant of {company} AND an audit-shape signal (PDF, "audit", "security", \
auditor firm name). Use 0.3–0.5 for weaker but plausible matches. Reserve \
<0.3 for clear non-audits."""

_EXTRACTION_PROMPT = """\
Identify third-party security audits of the **{company}** smart-contract protocol \
listed on the following page. We only need enough metadata to *identify* each \
audit — no findings, scope, or summary is required.

Page URL: {url}
Page content (truncated):
{page_text}

CRITICAL FILTER: Many pages are auditor publication directories or aggregator \
indexes that list audits for many DIFFERENT protocols. You must extract ONLY \
the audits that are clearly for {company}. Audits whose title or filename \
references a different protocol must be excluded — even if they are listed on \
the same page. When in doubt, exclude.

Return ONLY a JSON object with these fields:
- "reports": a list of audit report objects (audits of {company} only), each with:
  - "auditor": the auditing firm name (string)
  - "title": the audit report title (string) — should reference {company} or one of its products
  - "date": the audit date in ISO format YYYY-MM-DD if available, or YYYY-MM, or YYYY (string or null)
  - "pdf_url": direct URL to the PDF report if found on the page (string or null)
- "linked_urls": list of URLs found on this page that point to other audit reports, \
audit PDFs, or audit listing pages for {company} that are NOT already covered in the \
"reports" list above (list of strings, may be empty). Include direct PDF links, links \
to auditor blog posts, **GitHub repository URLs that belong to the protocol** (the \
downstream pipeline will explore them via the GitHub API to find audit folders), and \
links to other pages that list audits — but again, only links related to {company}, \
not to other protocols listed on the same page.

Return ONLY the JSON object, no other text."""


_FOLLOWUP_QUERY_PROMPT = """\
You have just searched the web for security audit reports for the {company} smart \
contract protocol. Below are the results from the initial search.

Based on what you see (and what's missing), suggest ONE follow-up search query that \
would find additional audit reports not already covered. Consider:
- Auditor firms mentioned in the results that might have their own blog posts
- The protocol might use a different name or have sub-protocols
- Audit contest platforms (Code4rena, Sherlock, Cantina) if not already found
- GitHub repositories where audit PDFs are often stored
- The protocol's official docs or security page if not already found

Initial results:
{results}

Reply with ONLY the search query string, nothing else. Keep it under 120 characters."""


def generate_followup_query(
    initial_results: list[dict[str, Any]],
    company: str,
    debug: bool = False,
) -> str | None:
    """Use the LLM to generate a targeted follow-up search query.

    Returns the query string, or ``None`` if the LLM call fails.
    """
    if not initial_results:
        return f'"{company}" security audit report findings'

    formatted = "\n".join(f"- {r.get('title', '(untitled)')}: {r.get('url', '')}" for r in initial_results[:15])

    prompt = _FOLLOWUP_QUERY_PROMPT.format(company=company, results=formatted)

    try:
        response = llm.chat(
            [{"role": "user", "content": prompt}],
            max_tokens=128,
            temperature=0.0,
        )
        query = response.strip().strip('"').strip("'").strip()
        # Fix mismatched quotes (LLM sometimes produces 'word" or "word')
        if query.count('"') % 2 != 0:
            query = query.replace('"', "")
        if query and len(query) < 200:
            _debug_log(debug, f"LLM generated follow-up query: {query!r}")
            return query
        _debug_log(debug, f"LLM follow-up query unusable: {response!r}")
    except Exception as exc:
        _debug_log(debug, f"LLM follow-up query generation failed: {exc!r}")

    return None


def _name_variants(company: str) -> list[str]:
    """Lowercased variants of a protocol slug for substring matching.

    ``"ether fi"`` → ``["ether fi", "etherfi", "ether.fi", "ether-fi", "ether_fi"]``.
    Used by the deterministic pre-filter to recognize that e.g.
    ``Audit_Report_-_ether.fi_26.10.2023.pdf`` references the same
    project as the ``"ether fi"`` slug we submitted."""
    base = (company or "").strip().lower()
    if not base:
        return []
    tokens = [t for t in re.split(r"[^a-z0-9]+", base) if t]
    out = {base}
    if tokens:
        out.add("".join(tokens))
        out.add(".".join(tokens))
        out.add("-".join(tokens))
        out.add("_".join(tokens))
        out.add(" ".join(tokens))
    return [v for v in out if v]


def _deterministic_audit_match(result: dict[str, Any], company: str) -> dict[str, Any] | None:
    """Fast-path: a Tavily result whose URL or title contains a company-name
    variant AND an audit-shape keyword is confidently audit-relevant.
    Bypasses the LLM for that result so obvious matches aren't at the
    mercy of Gemini's temperature-0 non-determinism (same prompt returns
    is_audit=true on one call and false on the next in practice).
    Returns a pre-classified record, or None to defer to the LLM.
    """
    title = (result.get("title") or "").lower()
    url = (result.get("url") or "").lower()
    haystack = f"{title} {url}"
    variants = _name_variants(company)
    if not any(v in haystack for v in variants):
        return None
    if not any(k in haystack for k in ("audit", "security review", "smart contract review", "assessment")):
        return None
    if url.endswith(".pdf"):
        kind = "pdf"
    elif "/audits" in url or "/security" in url:
        kind = "listing"
    else:
        kind = "report"
    return {
        "url": result.get("url") or "",
        "auditor": None,  # LLM doesn't enrich pre-accepted rows; downstream GitHub/dedup steps do
        "title": result.get("title"),
        "date": None,
        "type": kind,
        "confidence": 0.85,
    }


def classify_search_results(
    results: list[dict[str, Any]],
    company: str,
    debug: bool = False,
) -> list[dict[str, Any]]:
    """Stage 1: Classify Tavily results as audit/not-audit.

    Runs a deterministic pre-filter first (name-variant ∩ audit-keyword
    in URL or title → auto-accept), then sends only the ambiguous
    remainder to the LLM. This makes the pipeline robust to LLM
    non-determinism on obvious matches — previously a result titled
    ``Audit Report for ether.fi`` could be accepted on one call and
    rejected on the next, giving inconsistent audit counts between
    runs on identical inputs.

    Returns list of ``{url, is_audit, auditor, type, confidence}`` dicts.
    """
    if not results:
        return []

    # --- Deterministic pre-filter ---
    preaccepted: list[dict[str, Any]] = []
    needs_llm: list[dict[str, Any]] = []
    for r in results:
        hit = _deterministic_audit_match(r, company)
        if hit:
            preaccepted.append(hit)
        else:
            needs_llm.append(r)
    _debug_log(
        debug,
        f"Classifier pre-filter: {len(preaccepted)} auto-accepted, {len(needs_llm)} deferred to LLM",
    )

    if not needs_llm:
        return preaccepted

    formatted = "\n".join(
        f"- Title: {r.get('title', '(untitled)')}\n"
        f"  URL: {r.get('url', '')}\n"
        f"  Snippet: {(r.get('content', '') or '')[:300]}"
        for r in needs_llm
    )

    prompt = _CLASSIFICATION_PROMPT.format(
        company=company,
        results=formatted,
        auditors=_KNOWN_AUDITORS,
    )

    try:
        response = llm.chat(
            [{"role": "user", "content": prompt}],
            # 20 search results × 7 fields each can run ~3k output tokens once
            # title and date are included; the previous 2k cap silently
            # truncated and made the JSON unparseable.
            max_tokens=4096,
            temperature=0.0,
        )
    except Exception as exc:
        _debug_log(debug, f"Audit classification LLM call failed: {exc!r}")
        # LLM failure doesn't wipe the deterministic pre-filter's wins.
        return preaccepted

    parsed = _parse_json_array(response)
    if parsed is None:
        _debug_log(debug, "Audit classification: could not parse LLM response as JSON array")
        return preaccepted

    confirmed: list[dict[str, Any]] = list(preaccepted)
    for item in parsed:
        if not isinstance(item, dict):
            continue
        if not item.get("is_audit"):
            continue
        confidence = float(item.get("confidence", 0))
        # Threshold lowered from 0.5 → 0.3 alongside the "be generous"
        # prompt rewrite. Downstream deduplication + filename-match + URL
        # reachability checks throw out false positives, so accepting
        # borderline hits here costs little and catches audits the old
        # strict prompt missed (e.g. GitBook-hosted PDFs whose titles
        # don't say the word "audit" explicitly).
        if confidence < 0.3:
            continue
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        confirmed.append(
            {
                "url": url,
                "auditor": item.get("auditor"),
                "title": item.get("title"),
                "date": item.get("date"),
                "type": item.get("type"),
                "confidence": confidence,
            }
        )

    _debug_log(debug, f"Audit classification: {len(confirmed)} confirmed from {len(results)} results")
    return confirmed


# Per-chunk window for chunked extraction. 15k chars ≈ 4k tokens of input —
# leaves room for the prompt + a sizeable JSON response on the same call.
_CHUNK_SIZE = 15000
# Cap how many chunks we send for one page so a 300k-char gitbook doesn't
# burn 20 LLM calls. 3 covers ~45k chars which is enough for nearly every
# real audit listing we've seen.
_MAX_CHUNKS = 3


def _chunked_text(text: str) -> list[str]:
    """Split page text into ``_MAX_CHUNKS`` overlapping windows of
    ``_CHUNK_SIZE`` chars each. Pages that fit in one window stay one chunk.
    """
    if len(text) <= _CHUNK_SIZE:
        return [text]
    chunks: list[str] = []
    overlap = 500  # carry a little context across boundaries
    start = 0
    while start < len(text) and len(chunks) < _MAX_CHUNKS:
        chunks.append(text[start : start + _CHUNK_SIZE])
        start += _CHUNK_SIZE - overlap
    return chunks


def _extract_one_chunk(
    url: str,
    chunk: str,
    company: str,
    debug: bool = False,
) -> dict[str, Any] | None:
    """Run the extraction prompt over a single text chunk and parse the
    JSON response. Returns ``None`` on LLM failure or unparseable output."""
    prompt = _EXTRACTION_PROMPT.format(company=company, url=url, page_text=chunk)
    try:
        response = llm.chat(
            [{"role": "user", "content": prompt}],
            max_tokens=4096,
            temperature=0.0,
        )
    except Exception as exc:
        _debug_log(debug, f"Audit extraction LLM call failed for {url}: {exc!r}")
        return None

    parsed = _parse_json_object(response)
    if parsed is None:
        _debug_log(debug, f"Audit extraction: could not parse LLM response for {url}")
        return None
    return parsed


def extract_report_details(
    url: str,
    page_text: str,
    company: str,
    debug: bool = False,
) -> dict[str, Any] | None:
    """Stage 2: Extract structured audit data and discover links from page content.

    Returns a dict with ``reports`` (list of extracted report dicts) and
    ``linked_urls`` (list of URLs to follow), or ``None`` if extraction fails.

    Long pages are split into multiple windows so audit listings on heavyweight
    docs sites (gitbook, notion, SPAs) aren't truncated past the first ~15k
    characters.
    """
    chunks = _chunked_text(page_text)
    if len(chunks) > 1:
        _debug_log(debug, f"Extraction from {url}: splitting into {len(chunks)} chunks")

    raw_reports: list[Any] = []
    raw_links: list[Any] = []
    parsed_any = False
    for chunk in chunks:
        parsed = _extract_one_chunk(url, chunk, company, debug=debug)
        if parsed is None:
            continue
        parsed_any = True
        chunk_reports = parsed.get("reports")
        if isinstance(chunk_reports, list):
            raw_reports.extend(chunk_reports)
        elif parsed.get("auditor") and parsed.get("title"):
            # Backwards compat: a flat-field response is one report
            raw_reports.append(parsed)
        chunk_links = parsed.get("linked_urls")
        if isinstance(chunk_links, list):
            raw_links.extend(chunk_links)

    if not parsed_any:
        return None

    reports: list[dict[str, Any]] = []
    for raw in raw_reports:
        if not isinstance(raw, dict):
            continue
        auditor = str(raw.get("auditor") or "").strip()
        title = str(raw.get("title") or "").strip()
        if not auditor or not title:
            continue

        # Resolve pdf_url — may be relative
        pdf_url_raw = str(raw["pdf_url"]).strip() if raw.get("pdf_url") else None
        if pdf_url_raw and not pdf_url_raw.startswith(("http://", "https://")):
            from urllib.parse import urljoin

            pdf_url_raw = urljoin(url, pdf_url_raw)

        reports.append(
            {
                "auditor": auditor,
                "title": title,
                "date": str(raw["date"]).strip() if raw.get("date") else None,
                "pdf_url": pdf_url_raw,
            }
        )

    # Normalize linked URLs — resolve relative paths against the source page URL
    from urllib.parse import urljoin

    linked_urls: list[str] = []
    seen: set[str] = set()
    for link in raw_links:
        clean = str(link).strip() if link else ""
        if not clean:
            continue
        # Resolve relative URLs (e.g. "/audits" → "https://example.com/audits")
        if not clean.startswith(("http://", "https://")):
            clean = urljoin(url, clean)
        if clean.startswith(("http://", "https://")) and clean not in seen:
            seen.add(clean)
            linked_urls.append(clean)

    # When a page is split into multiple overlapping chunks the same audit can
    # be extracted from each chunk it falls inside. Dedup here so the caller
    # doesn't have to deal with within-page duplicates.
    if len(chunks) > 1:
        seen_keys: set[tuple[str, str, str]] = set()
        deduped_reports: list[dict[str, Any]] = []
        for r in reports:
            key = (
                (r.get("auditor") or "").strip().lower(),
                (r.get("title") or "").strip().lower(),
                (r.get("date") or "").strip(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_reports.append(r)
        reports = deduped_reports

    _debug_log(
        debug,
        f"Extraction from {url}: {len(reports)} report(s), {len(linked_urls)} linked URL(s)",
    )

    return {
        "reports": reports,
        "linked_urls": linked_urls,
    }
