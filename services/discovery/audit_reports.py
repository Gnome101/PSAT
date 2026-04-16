"""Orchestrator for protocol audit report discovery.

Given a company/protocol name, this module:
  1. Searches the web for third-party audit reports via Tavily
  2. Classifies results using an LLM (Stage 1)
  3. Fetches confirmed pages and extracts structured metadata via LLM (Stage 2)
  4. Follows links discovered on those pages to find additional reports (Stage 3)
  5. Deduplicates and returns a sorted list of audit reports

The ``merge_audit_reports`` function provides append-only merging across
successive discovery runs — reports are never removed.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests as _requests

from utils import llm

from .audit_reports_llm import classify_search_results, extract_report_details, generate_followup_query
from .inventory_domain import (
    TAG_RE,
    _debug_log,
    _fetch_page,
    _tavily_search,
)

# Maximum pages to fetch+extract in Stage 2 (initial confirmed hits).
_MAX_STAGE2_PAGES = 5

# Maximum additional pages to follow from links discovered in Stage 2.
_MAX_LINK_FOLLOWS = 5

# Total cap on LLM extraction calls across Stage 2 + Stage 3.
_MAX_TOTAL_EXTRACTIONS = 8


def _normalize_url(url: str) -> str:
    """Canonical form for dedup: lowercase scheme+host, strip trailing slash."""
    try:
        parsed = urlparse(url)
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            path=parsed.path.rstrip("/") or "/",
            fragment="",
        )
        return normalized.geturl()
    except Exception:
        return url.strip().rstrip("/")


def _dedupe_results_by_url(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate Tavily results by URL, keeping the first occurrence."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in results:
        url = str(r.get("url", "")).strip()
        if not url:
            continue
        key = _normalize_url(url)
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


_ANCHOR_RE = re.compile(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>')
_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style|noscript|svg)\b.*?</\1>")


def _page_to_text(page_html: str) -> str:
    """Convert HTML to LLM-friendly text, preserving link URLs.

    Anchor tags become ``link_text (href)`` so the LLM can see where links point —
    critical for GitHub directory pages where the PDF links ARE the content.
    """
    # Remove script/style blocks first
    cleaned = _SCRIPT_STYLE_RE.sub(" ", page_html)
    # Convert anchors to "text (url)" before stripping other tags
    cleaned = _ANCHOR_RE.sub(
        lambda m: f"{TAG_RE.sub(' ', m.group(2)).strip()} ({m.group(1)}) ",
        cleaned,
    )
    # Strip remaining HTML tags
    cleaned = TAG_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _is_pdf_url(url: str) -> bool:
    """Heuristic check if URL path ends in .pdf (ignoring query params)."""
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf")
    except Exception:
        return False


# Maximum page body to download (bytes). Prevents fetching huge PDFs or media.
_MAX_DOWNLOAD_BYTES = 512_000

_BINARY_CONTENT_TYPES = frozenset({"application/pdf", "application/octet-stream", "image/"})


def _fetch_html_page(url: str, debug: bool = False) -> str | None:
    """Fetch a page, rejecting binary content and capping download size.

    Returns the HTML text, or ``None`` if the page is binary, too large,
    or unreachable.
    """
    try:
        resp = _requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "PSAT/0.1"},
            stream=True,
        )
        if resp.status_code != 200:
            _debug_log(debug, f"Fetch {url}: HTTP {resp.status_code}")
            return None

        content_type = (resp.headers.get("content-type") or "").lower()
        if any(ct in content_type for ct in _BINARY_CONTENT_TYPES):
            _debug_log(debug, f"Skipping binary content ({content_type}): {url}")
            resp.close()
            return None

        # Read up to the cap
        chunks: list[bytes] = []
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=64_000):
            chunks.append(chunk)
            downloaded += len(chunk)
            if downloaded >= _MAX_DOWNLOAD_BYTES:
                _debug_log(debug, f"Truncated download at {downloaded} bytes: {url}")
                break
        resp.close()

        body = b"".join(chunks)
        try:
            text = body.decode("utf-8", errors="replace")
        except Exception:
            text = body.decode("latin-1", errors="replace")

        _debug_log(debug, f"Fetched {url} ({len(text)} chars)")
        return text

    except _requests.RequestException as exc:
        _debug_log(debug, f"Fetch {url} failed: {exc!r}")
        return None


# ---------------------------------------------------------------------------
# GitHub API integration
# ---------------------------------------------------------------------------

_GITHUB_TREE_RE = re.compile(
    r"^https?://github\.com/([^/]+)/([^/]+)/tree/([^/]+)/(.+?)/?$"
)
_GITHUB_BLOB_RE = re.compile(
    r"^https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+?)/?$"
)
_GITHUB_REPO_RE = re.compile(
    r"^https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$"
)
_GITHUB_ORG_RE = re.compile(r"^https?://github\.com/([^/?#]+)/?$")

# Single-segment GitHub paths that look like orgs but aren't ownable accounts.
_RESERVED_GITHUB_PATHS = frozenset({
    "orgs", "users", "settings", "marketplace", "topics", "search",
    "sponsors", "explore", "about", "pricing", "enterprise", "trending",
    "collections", "events", "features", "customer-stories", "team",
    "contact", "site", "login", "join", "new", "organizations",
    "notifications", "issues", "pulls", "watching", "stars", "codespaces",
})


def _github_api_headers() -> dict[str, str]:
    """Standard headers for GitHub API calls. Picks up GITHUB_TOKEN if set
    so the rate limit jumps from 60/hour (anon) to 5000/hour (token)."""
    import os
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "PSAT/0.1",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"
    return headers

# Process at most this many filenames per LLM call so the JSON response stays
# well under the model's output limit even for repos with 50+ audit files.
_FILENAME_BATCH_SIZE = 12


def _llm_extract_filename_metadata(
    filenames: list[str],
    company: str,
    debug: bool = False,
) -> dict[str, dict[str, Any]]:
    """Ask the LLM to extract auditor / date / title for each filename.

    Returns a mapping ``filename -> {auditor, date, title}``. Filenames the
    LLM couldn't classify are simply absent from the map; the caller is
    responsible for falling back gracefully.

    The call is split into small batches so the JSON output never exceeds
    the model's response cap — the previous single-shot call silently
    truncated for large directories.
    """
    from .audit_reports_llm import _parse_json_array

    if not filenames:
        return {}

    from .audit_reports_llm import _KNOWN_AUDITORS

    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(filenames), _FILENAME_BATCH_SIZE):
        batch = filenames[start : start + _FILENAME_BATCH_SIZE]
        file_list = "\n".join(f"- {name}" for name in batch)
        prompt = (
            f"Below are file names from a GitHub audit directory for the "
            f"{company} smart-contract protocol.\n"
            f"For each file, extract the auditing firm name, the audit date, "
            f"and a clean human-readable title.\n\n"
            f"Common auditing firms (use these names verbatim when you spot "
            f"a match — including from abbreviations or report-ID prefixes): "
            f"{_KNOWN_AUDITORS}.\n\n"
            f"{file_list}\n\n"
            f"Reply with ONLY a JSON array, one object per file, in the same "
            f"order. Each object MUST have these keys:\n"
            f'- "filename": the exact filename including extension\n'
            f'- "auditor": auditing firm name, or null if not identifiable\n'
            f'- "date": audit date as YYYY-MM-DD (or YYYY-MM / YYYY), or null\n'
            f'- "title": short human-readable title for the audit'
        )
        try:
            response = llm.chat(
                [{"role": "user", "content": prompt}],
                max_tokens=4096,
                temperature=0.0,
            )
        except Exception as exc:
            _debug_log(debug, f"GitHub tree LLM batch failed ({start}): {exc!r}")
            continue

        parsed = _parse_json_array(response)
        if not parsed:
            _debug_log(debug, f"GitHub tree LLM batch {start}: unparseable response")
            continue

        # Match LLM entries to filenames. Prefer the LLM's "filename" key
        # but fall back to positional matching when the LLM dropped or
        # rewrote the filename — common when models omit extensions.
        for idx, item in enumerate(parsed):
            if not isinstance(item, dict):
                continue
            requested = batch[idx] if idx < len(batch) else None
            llm_name = str(item.get("filename") or "").strip()

            target = None
            if llm_name in batch:
                target = llm_name
            elif requested and (
                llm_name == requested.rsplit(".", 1)[0] or not llm_name
            ):
                target = requested
            if target is None:
                continue
            out[target] = item

    _debug_log(
        debug,
        f"GitHub tree LLM: classified {len(out)}/{len(filenames)} filename(s)",
    )
    return out


def _parse_github_url(url: str) -> dict[str, str] | None:
    """Parse a GitHub URL into owner/repo (and ref/path for tree/blob)."""
    for pattern, kind in [(_GITHUB_TREE_RE, "tree"), (_GITHUB_BLOB_RE, "blob")]:
        m = pattern.match(url)
        if m:
            return {
                "kind": kind,
                "owner": m.group(1),
                "repo": m.group(2),
                "ref": m.group(3),
                "path": m.group(4),
            }
    m = _GITHUB_REPO_RE.match(url)
    if m:
        repo = m.group(2)
        # ``github.com/<org>`` (no repo) and known non-repo paths shouldn't match.
        if repo in _RESERVED_GITHUB_PATHS:
            return None
        return {"kind": "repo", "owner": m.group(1), "repo": repo, "ref": "", "path": ""}
    m = _GITHUB_ORG_RE.match(url)
    if m:
        owner = m.group(1)
        if owner in _RESERVED_GITHUB_PATHS:
            return None
        return {"kind": "org", "owner": owner, "repo": "", "ref": "", "path": ""}
    return None


# Conventional folder names protocols use to ship third-party audit reports.
# Probed in order via the GitHub contents API. Hits short-circuit later probes
# in the same repo since most repos publish under exactly one path.
_AUDIT_FOLDER_CANDIDATES: tuple[str, ...] = (
    "audits",
    "audit",
    "audit-reports",
    "security/audits",
    "docs/audits",
    "security",
)


# Cap the per-org repo enumeration so a vendor org (e.g. Certora, which
# holds hundreds of repos) can't blow out the GitHub API budget for a
# single Stage-2 hit. Sorted by pushed_at desc, so the newest repos —
# which is where current audits live — are probed first.
_MAX_ORG_REPOS = 30


def _list_org_repos(owner: str, debug: bool = False) -> list[str]:
    """List public repo names for a GitHub org or user, ordered by most
    recently pushed. Returns up to ``_MAX_ORG_REPOS`` names.

    Tries the ``/orgs/{owner}/repos`` endpoint first; falls back to
    ``/users/{owner}/repos`` so this works for both org and user accounts.
    """
    params = f"per_page={_MAX_ORG_REPOS}&sort=pushed&direction=desc&type=public"
    for endpoint in ("orgs", "users"):
        url = f"https://api.github.com/{endpoint}/{owner}/repos?{params}"
        try:
            resp = _requests.get(url, timeout=30, headers=_github_api_headers())
        except _requests.RequestException as exc:
            _debug_log(debug, f"GitHub {endpoint} list failed for {owner}: {exc!r}")
            continue
        if resp.status_code == 404:
            continue
        if resp.status_code != 200:
            _debug_log(debug, f"GitHub {endpoint} list {resp.status_code} for {owner}")
            return []
        data = resp.json()
        if not isinstance(data, list):
            return []
        # Include archived repos — protocol teams routinely archive the
        # previous major version's repo (e.g. ``morpho-optimizers``, the
        # legacy V1 Morpho codebase) while its audit files remain the
        # authoritative record for that version.
        names = [
            str(item.get("name"))
            for item in data
            if item.get("name")
        ]
        _debug_log(debug, f"GitHub {endpoint} {owner}: {len(names)} repo(s)")
        return names[:_MAX_ORG_REPOS]
    _debug_log(debug, f"GitHub {owner}: neither orgs nor users endpoint returned data")
    return []


def _fetch_github_org_as_reports(
    owner: str, company: str, url: str, debug: bool = False,
) -> dict[str, Any] | None:
    """Enumerate every repo in a GitHub org and pull audit folders from each.

    Protocol teams routinely split their codebase across sibling repos
    (core contracts in one repo, periphery in another, each vault version
    in its own) with a shared ``audits/`` folder pattern. A bare org URL
    like ``github.com/morpho-org`` has to expand into all of those to catch
    audits that aren't reachable from the single repo Tavily happens to
    surface.
    """
    repos = _list_org_repos(owner, debug=debug)
    if not repos:
        return None

    merged_reports: list[dict[str, Any]] = []
    merged_linked: list[str] = []
    probed_with_audits = 0

    for repo in repos:
        folders = _discover_repo_audit_folders(owner, repo, debug=debug)
        if not folders:
            continue
        probed_with_audits += 1
        for folder in folders:
            tree_url = (
                f"https://github.com/{owner}/{repo}"
                f"/tree/{folder['ref']}/{folder['path']}"
            )
            sub = _fetch_github_tree_as_reports(
                owner, repo, folder["path"],
                ref=folder["ref"], company=company, url=tree_url, debug=debug,
            )
            if not sub:
                continue
            merged_reports.extend(sub.get("reports", []))
            merged_linked.extend(sub.get("linked_urls", []))

    _debug_log(
        debug,
        f"GitHub org {owner}: {probed_with_audits}/{len(repos)} repo(s) "
        f"had audit folder(s); {len(merged_reports)} report(s) total",
    )
    return {"reports": merged_reports, "linked_urls": merged_linked}


def _discover_repo_audit_folders(
    owner: str, repo: str, debug: bool = False,
) -> list[dict[str, str]]:
    """Probe a GitHub repo via the API for directories that hold audit reports.

    Returns a list of ``{ref, path}`` dicts that ``_fetch_github_tree_as_reports``
    can consume directly. Costs 1 GitHub API call to resolve the default branch
    plus one recursive-tree call (or up to ~6 contents-API probes if the tree
    call is unavailable / truncated).
    """
    meta_url = f"https://api.github.com/repos/{owner}/{repo}"
    try:
        resp = _requests.get(meta_url, timeout=15, headers=_github_api_headers())
    except _requests.RequestException as exc:
        _debug_log(debug, f"GitHub repo metadata failed for {owner}/{repo}: {exc!r}")
        return []
    if resp.status_code != 200:
        _debug_log(debug, f"GitHub repo metadata {resp.status_code} for {owner}/{repo}")
        return []
    default_branch = (resp.json() or {}).get("default_branch") or "main"

    # Prefer the recursive tree call — one API request gives us every folder.
    tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{default_branch}?recursive=1"
    try:
        tree_resp = _requests.get(tree_url, timeout=30, headers=_github_api_headers())
    except _requests.RequestException as exc:
        _debug_log(debug, f"GitHub tree call failed for {owner}/{repo}: {exc!r}")
        tree_resp = None

    found_paths: list[str] = []
    if tree_resp is not None and tree_resp.status_code == 200:
        data = tree_resp.json() or {}
        for item in data.get("tree", []):
            if item.get("type") != "tree":
                continue
            path = (item.get("path") or "").strip()
            if not path:
                continue
            last_segment = path.rsplit("/", 1)[-1].lower()
            if last_segment in {"audit", "audits", "audit-reports", "security-audits"}:
                found_paths.append(path)
        if data.get("truncated"):
            _debug_log(debug, f"GitHub tree truncated for {owner}/{repo}; some folders may be missed")

    # Fallback: if the tree call didn't yield anything, probe a handful of
    # conventional paths via the contents endpoint.
    if not found_paths:
        for candidate in _AUDIT_FOLDER_CANDIDATES:
            probe_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{candidate}?ref={default_branch}"
            try:
                pr = _requests.get(probe_url, timeout=15, headers=_github_api_headers())
            except _requests.RequestException:
                continue
            if pr.status_code == 200 and isinstance(pr.json(), list):
                found_paths.append(candidate)

    _debug_log(
        debug,
        f"GitHub repo {owner}/{repo}: {len(found_paths)} audit folder(s) {found_paths}",
    )
    return [{"ref": default_branch, "path": p} for p in found_paths]


def _fetch_github_tree_as_reports(
    owner: str, repo: str, path: str, ref: str = "master",
    company: str = "", url: str = "", debug: bool = False,
) -> dict[str, Any] | None:
    """Fetch a GitHub directory via the contents API and build report entries directly.

    The API returns structured JSON (file names, sizes, download URLs), so we
    don't need an LLM to parse HTML. We use a single LLM call to extract
    auditor names and dates from the file names — a much smaller prompt than
    trying to produce 29 full report objects.
    """
    from .audit_reports_llm import _parse_json_array

    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"
    try:
        resp = _requests.get(api_url, timeout=15, headers=_github_api_headers())
        if resp.status_code != 200:
            _debug_log(debug, f"GitHub API {resp.status_code} for {api_url}")
            return None

        items = resp.json()
        if not isinstance(items, list):
            return None

        _debug_log(debug, f"GitHub API: {len(items)} items in {owner}/{repo}/{path}")

        # Collect audit-relevant files (PDFs, markdown reports)
        audit_files: list[dict[str, str]] = []
        subdirs: list[str] = []
        for item in items:
            name = item.get("name", "")
            item_type = item.get("type", "")
            download_url = item.get("download_url") or ""
            html_url = item.get("html_url") or ""

            if item_type == "dir":
                subdirs.append(html_url)
                continue
            if item_type != "file":
                continue
            lower = name.lower()
            if lower.endswith((".pdf", ".md")) and not lower.startswith((".gitkeep", "readme")):
                audit_files.append({
                    "name": name,
                    "download_url": download_url,
                    "html_url": html_url,
                })

        if not audit_files:
            _debug_log(debug, f"No audit files found in {owner}/{repo}/{path}")
            return {"reports": [], "linked_urls": subdirs}

        # Ask the LLM to extract structured metadata per filename. We batch
        # large directories so the JSON response never gets truncated by the
        # token cap (the previous 2048-token call silently truncated for
        # protocols like ether.fi that ship 30+ audit files).
        file_metadata = _llm_extract_filename_metadata(
            [f["name"] for f in audit_files], company, debug=debug,
        )

        # Build report entries from the LLM output. Anything the LLM couldn't
        # classify falls through with auditor="Unknown" and the bare filename
        # (extension stripped) as the title — same convention as the
        # Stage-1 PDF fallback path.
        reports: list[dict[str, Any]] = []
        for f in audit_files:
            name = f["name"]
            meta = file_metadata.get(name, {})

            base_name = name.rsplit(".", 1)[0] if "." in name else name
            auditor = str(meta.get("auditor") or "").strip() or "Unknown"
            title = str(meta.get("title") or "").strip() or base_name
            date = str(meta.get("date") or "").strip() or None

            is_pdf = name.lower().endswith(".pdf")
            pdf_url = f["download_url"] if is_pdf else None
            # ``report_url`` keeps each file's own GitHub link so multiple .md
            # files in the same directory don't all collapse onto the tree URL
            # during the URL-keyed dedup step.
            report_url = f.get("html_url") or f.get("download_url")

            reports.append({
                "auditor": auditor,
                "title": title,
                "date": date,
                "pdf_url": pdf_url,
                "report_url": report_url,
            })

        _debug_log(debug, f"GitHub tree: built {len(reports)} report(s) from {len(audit_files)} file(s)")
        return {"reports": reports, "linked_urls": subdirs}

    except _requests.RequestException as exc:
        _debug_log(debug, f"GitHub API failed for {api_url}: {exc!r}")
        return None


def _company_name_variants(company: str) -> list[str]:
    """Lowercase variants of the company name to substring-match against
    filenames. Handles common stylings like ``EtherFi`` / ``ether.fi`` /
    ``ether_fi`` without coding them per protocol.
    """
    base = company.strip().lower()
    if not base:
        return []
    variants = {base}
    # Strip non-alphanumerics and re-add separator variants
    stripped = re.sub(r"[^a-z0-9]", "", base)
    if stripped and stripped != base:
        variants.add(stripped)
    if len(stripped) >= 4:
        variants.add(stripped)
    return [v for v in variants if v]


def _filename_mentions_company(name: str, company_variants: list[str]) -> bool:
    """True when any company-name variant appears as a substring of the
    filename (case-insensitive, separator-agnostic)."""
    if not company_variants:
        return True  # no filter requested
    haystack = re.sub(r"[^a-z0-9]", "", name.lower())
    return any(v in haystack for v in company_variants)


def _expand_blob_to_directory(
    owner: str,
    repo: str,
    ref: str,
    blob_path: str,
    company: str,
    debug: bool = False,
) -> dict[str, Any] | None:
    """Given a confirmed audit blob URL, list its parent directory and pull
    every same-protocol audit file there.

    This handles the auditor-portfolio case: when Stage 1 confirms one
    ``Zellic/publications/blob/.../EtherFi - Zellic Audit Report.pdf``, we
    list the publications root and pick up any other ether.fi audits the
    same auditor published. Files for *other* protocols are filtered out
    by company-name substring match on the filename.
    """
    parent = blob_path.rsplit("/", 1)[0] if "/" in blob_path else ""
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{parent}?ref={ref}"
    try:
        resp = _requests.get(api_url, timeout=15, headers=_github_api_headers())
    except _requests.RequestException as exc:
        _debug_log(debug, f"GitHub directory list failed for {owner}/{repo}/{parent}: {exc!r}")
        return None
    if resp.status_code != 200:
        _debug_log(debug, f"GitHub directory list {resp.status_code} for {owner}/{repo}/{parent}")
        return None
    items = resp.json()
    if not isinstance(items, list):
        return None

    variants = _company_name_variants(company)
    audit_files: list[dict[str, str]] = []
    for item in items:
        if item.get("type") != "file":
            continue
        name = item.get("name", "")
        lower = name.lower()
        if not (lower.endswith((".pdf", ".md")) and not lower.startswith((".gitkeep", "readme"))):
            continue
        # When the parent directory holds many protocols' audits, filter
        # to the ones whose filename mentions the company. Skip the filter
        # entirely for protocol-owned repos (where every file is in-scope).
        if not _filename_mentions_company(name, variants):
            continue
        audit_files.append({
            "name": name,
            "download_url": item.get("download_url") or "",
            "html_url": item.get("html_url") or "",
        })

    if not audit_files:
        _debug_log(debug, f"No same-protocol siblings in {owner}/{repo}/{parent}")
        return None

    _debug_log(
        debug,
        f"Expanded blob → directory {owner}/{repo}/{parent}: "
        f"{len(audit_files)} same-protocol file(s)",
    )

    file_metadata = _llm_extract_filename_metadata(
        [f["name"] for f in audit_files], company, debug=debug,
    )

    reports: list[dict[str, Any]] = []
    for f in audit_files:
        name = f["name"]
        meta = file_metadata.get(name, {})
        base_name = name.rsplit(".", 1)[0] if "." in name else name
        auditor = str(meta.get("auditor") or "").strip() or "Unknown"
        title = str(meta.get("title") or "").strip() or base_name
        date = str(meta.get("date") or "").strip() or None
        is_pdf = name.lower().endswith(".pdf")
        reports.append({
            "auditor": auditor,
            "title": title,
            "date": date,
            "pdf_url": f["download_url"] if is_pdf else None,
            "report_url": f.get("html_url") or f.get("download_url"),
        })
    return {"reports": reports, "linked_urls": []}


def _fetch_github_raw(owner: str, repo: str, ref: str, path: str, debug: bool = False) -> str | None:
    """Fetch raw file content from GitHub for markdown/text files."""
    if _is_pdf_url(path):
        _debug_log(debug, f"Skipping raw fetch for PDF: {path}")
        return None

    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"
    try:
        resp = _requests.get(raw_url, timeout=15, headers={"User-Agent": "PSAT/0.1"})
        if resp.status_code != 200:
            _debug_log(debug, f"GitHub raw {resp.status_code} for {raw_url}")
            return None

        content_type = (resp.headers.get("content-type") or "").lower()
        if any(ct in content_type for ct in _BINARY_CONTENT_TYPES):
            _debug_log(debug, f"Skipping binary GitHub file ({content_type}): {path}")
            return None

        text = resp.text[:_MAX_DOWNLOAD_BYTES]
        _debug_log(debug, f"GitHub raw: fetched {path} ({len(text)} chars)")
        return text

    except _requests.RequestException as exc:
        _debug_log(debug, f"GitHub raw fetch failed for {raw_url}: {exc!r}")
        return None


# ---------------------------------------------------------------------------
# Fetch + extract orchestration
# ---------------------------------------------------------------------------


def _fetch_and_extract(
    url: str,
    company: str,
    all_results: list[dict[str, Any]],
    debug: bool = False,
) -> dict[str, Any] | None:
    """Fetch a page and run LLM extraction.

    For GitHub tree/blob URLs, uses the GitHub API for clean structured data
    instead of scraping the HTML (which is a React SPA with huge boilerplate).
    """
    github = _parse_github_url(url)

    if github:
        if github["kind"] == "tree":
            # Directory listing: use GitHub API directly, build reports from file metadata
            return _fetch_github_tree_as_reports(
                github["owner"], github["repo"], github["path"],
                ref=github["ref"], company=company, url=url, debug=debug,
            )
        if github["kind"] == "org":
            # Bare org URL: enumerate every repo in the org and probe each
            # for an audits folder. Protocol teams typically shard their
            # audits across sibling repos (core, periphery, v2, etc.), so
            # one Tavily hit on the org root needs to fan out.
            return _fetch_github_org_as_reports(
                github["owner"], company, url, debug=debug,
            )
        if github["kind"] == "repo":
            # Repo root URL: ask GitHub which subdirectories hold audits, then
            # consume each one as if Tavily had returned the /tree/audits/ URL
            # directly. Avoids scraping the React SPA at the repo root.
            folders = _discover_repo_audit_folders(github["owner"], github["repo"], debug=debug)
            if not folders:
                _debug_log(debug, f"No audit folders found in repo {github['owner']}/{github['repo']}")
                return {"reports": [], "linked_urls": []}

            merged_reports: list[dict[str, Any]] = []
            merged_linked: list[str] = []
            for folder in folders:
                tree_url = (
                    f"https://github.com/{github['owner']}/{github['repo']}"
                    f"/tree/{folder['ref']}/{folder['path']}"
                )
                sub = _fetch_github_tree_as_reports(
                    github["owner"], github["repo"], folder["path"],
                    ref=folder["ref"], company=company, url=tree_url, debug=debug,
                )
                if not sub:
                    continue
                merged_reports.extend(sub.get("reports", []))
                merged_linked.extend(sub.get("linked_urls", []))
            return {"reports": merged_reports, "linked_urls": merged_linked}
        # blob: two cases
        # 1. PDF blob — we can't extract its contents, but the parent
        #    directory may hold more same-protocol audits in an auditor
        #    publication repo (Zellic/publications, spearbit/portfolio, etc.).
        #    List the directory via the GitHub API and pull siblings.
        # 2. Markdown / text blob — fetch raw content and run extraction.
        if _is_pdf_url(github["path"]):
            return _expand_blob_to_directory(
                github["owner"], github["repo"], github["ref"],
                github["path"], company, debug=debug,
            )
        page_text = _fetch_github_raw(
            github["owner"], github["repo"], github["ref"],
            github["path"], debug=debug,
        )
        if not page_text:
            return None
        return extract_report_details(url, page_text, company, debug=debug)

    # Non-GitHub: fetch HTML and convert to text
    page_html = _fetch_html_page(url, debug=debug)
    if not page_html:
        return None

    page_text = _page_to_text(page_html)
    if not page_text:
        _debug_log(debug, f"Empty page after stripping HTML: {url}")
        return None

    return extract_report_details(url, page_text, company, debug=debug)


def _build_report_entry(
    report: dict[str, Any],
    source_url: str,
    confidence: float,
    now_iso: str,
) -> dict[str, Any]:
    """Build a final report dict from extracted LLM data."""
    return {
        "url": report.get("pdf_url") or report.get("report_url") or source_url,
        "pdf_url": report.get("pdf_url"),
        "auditor": report["auditor"],
        "title": report["title"],
        "date": report.get("date"),
        "source_url": source_url,
        "confidence": round(confidence, 4),
        "discovered_at": now_iso,
    }


_GENERIC_TITLE_TOKENS = frozenset({"audit", "report", "review", "security", "smart", "contract", "assessment"})


def _title_tokens(title: str) -> set[str]:
    """Lowercase word-tokens with the generic audit-report words removed.

    The result is what's left to compare for "do these two titles describe
    the same audit?" — auditor names, product names, version numbers, scope
    keywords. Punctuation is stripped. Single-character tokens are kept
    deliberately so that consecutive audits like ``V3.Prelude - 1`` and
    ``V3.Prelude - 2`` remain distinguishable.
    """
    if not title:
        return set()
    raw = re.findall(r"[A-Za-z0-9.]+", title.lower())
    return {w for w in raw if w and w not in _GENERIC_TITLE_TOKENS}


def _richness_score(report: dict[str, Any]) -> int:
    return sum(1 for key in ("pdf_url", "date") if report.get(key))


def _collapse_same_audit_mirrors(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse entries that look like mirrors of the same audit.

    Two passes, both designed to never merge two genuinely distinct audits:

      1. Drop reports whose auditor is "Unknown" when another report on the
         exact same date has a named auditor — those are almost always
         alternate hostings of that audit (gitbook PDF vs. github PDF).

      2. For each (auditor, date) group with >1 entry, collapse entries
         whose titles share their non-generic content (one title's
         meaningful tokens are a subset of another's). Distinct audits by
         the same auditor on the same day keep different titles, so they
         stay separate.
    """
    # Pass 1: dates that have a named-auditor entry
    named_dates: set[str] = set()
    for r in reports:
        auditor = (r.get("auditor") or "").strip().lower()
        date = (r.get("date") or "").strip()
        if date and auditor and auditor != "unknown":
            named_dates.add(date)

    pass1: list[dict[str, Any]] = []
    for r in reports:
        auditor = (r.get("auditor") or "").strip().lower()
        date = (r.get("date") or "").strip()
        if auditor in ("", "unknown") and date in named_dates:
            continue
        pass1.append(r)

    # Pass 2: collapse mirrors that share (auditor, date, title-tokens).
    # Distinct audits by the same auditor on the same day still stay
    # separate because they have different non-generic title tokens
    # (e.g. Certora's "EtherFi v2.49" vs "EtherFi Instant Withdrawal
    # Merge into v2.49"). The date component can be empty: that catches
    # mirror entries (like Nethermind NM-#### .md files) where neither
    # the filename nor the LLM yielded a date.
    drop: set[int] = set()
    groups: dict[tuple[str, str, frozenset[str]], list[int]] = {}
    for i, r in enumerate(pass1):
        auditor = (r.get("auditor") or "").strip().lower()
        if not auditor or auditor == "unknown":
            continue
        tokens = frozenset(_title_tokens(r.get("title") or ""))
        if not tokens:
            # No meaningful title content — too risky to collapse.
            continue
        date = (r.get("date") or "").strip()
        groups.setdefault((auditor, date, tokens), []).append(i)

    for indices in groups.values():
        if len(indices) < 2:
            continue
        # Keep the entry with the highest richness; tie → keep the first.
        best = max(indices, key=lambda i: (_richness_score(pass1[i]), -i))
        for i in indices:
            if i != best:
                drop.add(i)

    return [r for i, r in enumerate(pass1) if i not in drop]


def _build_fallback_entry(
    url: str,
    classification: dict[str, Any],
    company: str,
    all_results: list[dict[str, Any]],
    confidence: float,
    now_iso: str,
    pdf_url: str | None = None,
) -> dict[str, Any]:
    """Build a report entry from Stage 1 LLM metadata when extraction fails.

    ``classification`` is the dict returned by ``classify_search_results`` for
    this URL — it carries the LLM's parsed auditor, title and date in addition
    to the raw Tavily snippet, so the fallback never has to apply heuristics
    of its own.
    """
    tavily_match = next((r for r in all_results if r.get("url") == url), None)
    tavily_title = (tavily_match.get("title") or "").strip() if tavily_match else ""
    title = (
        str(classification.get("title") or "").strip()
        or tavily_title
        or f"{company} Audit Report"
    )

    return {
        "url": url,
        "pdf_url": pdf_url or (url if _is_pdf_url(url) else None),
        "auditor": str(classification.get("auditor") or "").strip() or "Unknown",
        "title": title,
        "date": classification.get("date") or None,
        "source_url": url,
        "confidence": round(confidence, 4),
        "discovered_at": now_iso,
    }


def search_audit_reports(
    company: str,
    official_domain: str | None = None,
    max_queries: int = 2,
    debug: bool = False,
) -> dict[str, Any]:
    """Search the web for third-party audit reports for a protocol.

    Returns a dict with ``reports`` (list of extracted audit metadata),
    ``queries_used``, ``errors``, and ``notes``.
    """
    clean_company = company.strip()
    if not clean_company:
        raise ValueError("company must not be empty")

    errors: list[dict[str, Any]] = []
    notes: list[str] = []
    queries_used = [0]

    _debug_log(debug, f"Starting audit report discovery for: {clean_company}")

    # --- Tavily searches ---

    # Query 1: broad search
    broad_results = _tavily_search(
        f'"{clean_company}" smart contract security audit report',
        max_results=10,
        queries_used=queries_used,
        max_queries=max_queries,
        errors=errors,
        debug=debug,
    )

    # Query 2: LLM-generated follow-up based on what Query 1 returned
    followup_query = generate_followup_query(broad_results, clean_company, debug=debug)
    followup_results: list[dict[str, Any]] = []
    if followup_query:
        followup_results = _tavily_search(
            followup_query,
            max_results=10,
            queries_used=queries_used,
            max_queries=max_queries,
            errors=errors,
            debug=debug,
        )

    all_results = _dedupe_results_by_url(broad_results + followup_results)
    notes.append(f"Search returned {len(all_results)} unique result(s)")

    if not all_results:
        notes.append("No search results found")
        return _empty_result(clean_company, official_domain, queries_used[0], errors, notes)

    # --- Stage 1: LLM classification ---

    classified = classify_search_results(all_results, clean_company, debug=debug)
    notes.append(f"LLM classified {len(classified)} result(s) as audit reports")

    if not classified:
        notes.append("No results classified as audit reports")
        return _empty_result(clean_company, official_domain, queries_used[0], errors, notes)

    # Sort: listing pages first (each one expands into many reports — high
    # value), then everything else by descending confidence. Without this,
    # an aggregator like ``docs.morpho.org/get-started/resources/audits/``
    # gets pushed below individual PDF hits and misses the Stage-2 cap.
    classified.sort(
        key=lambda x: (x.get("type") != "listing", -x.get("confidence", 0))
    )

    now_iso = datetime.now(timezone.utc).isoformat()
    reports: list[dict[str, Any]] = []
    processed_urls: set[str] = set()
    extraction_count = 0
    # URLs discovered by the LLM on fetched pages, to follow in Stage 3
    discovered_links: list[dict[str, Any]] = []

    # --- Stage 2: fetch confirmed pages + LLM extraction ---

    for item in classified[:_MAX_STAGE2_PAGES]:
        if extraction_count >= _MAX_TOTAL_EXTRACTIONS:
            break

        url = item["url"]
        url_key = _normalize_url(url)
        if url_key in processed_urls:
            continue
        processed_urls.add(url_key)

        stage1_confidence = item.get("confidence", 0.5)

        # For direct PDF links: record as fallback (we can't extract text from PDFs)
        # but still record what we know from the Tavily snippet + classification
        if _is_pdf_url(url):
            reports.append(_build_fallback_entry(
                url, item, clean_company, all_results,
                confidence=stage1_confidence, now_iso=now_iso, pdf_url=url,
            ))
            continue

        extracted = _fetch_and_extract(url, clean_company, all_results, debug=debug)
        extraction_count += 1

        if extracted is None:
            # Extraction failed — record from Stage 1 metadata
            reports.append(_build_fallback_entry(
                url, item, clean_company, all_results,
                confidence=stage1_confidence, now_iso=now_iso,
            ))
            continue

        # Process extracted reports (may be multiple from a listing page)
        for report in extracted.get("reports", []):
            reports.append(_build_report_entry(report, url, stage1_confidence, now_iso))

        # Collect links for Stage 3
        for linked_url in extracted.get("linked_urls", []):
            linked_key = _normalize_url(linked_url)
            if linked_key not in processed_urls:
                discovered_links.append({
                    "url": linked_url,
                    "source_url": url,
                    "parent_confidence": stage1_confidence,
                })

    stage2_count = len(reports)
    _debug_log(debug, f"Stage 2 complete: {stage2_count} report(s) from {extraction_count} page(s)")

    # --- Stage 3: follow discovered links ---

    links_followed = 0
    for link_item in discovered_links:
        if extraction_count >= _MAX_TOTAL_EXTRACTIONS:
            break
        if links_followed >= _MAX_LINK_FOLLOWS:
            break

        url = link_item["url"]
        url_key = _normalize_url(url)
        if url_key in processed_urls:
            continue
        processed_urls.add(url_key)

        parent_confidence = link_item.get("parent_confidence", 0.5)

        # PDF links from discovered pages: record directly with parent context
        if _is_pdf_url(url):
            reports.append(_build_fallback_entry(
                url, {}, clean_company, all_results,
                confidence=parent_confidence, now_iso=now_iso, pdf_url=url,
            ))
            links_followed += 1
            continue

        extracted = _fetch_and_extract(url, clean_company, all_results, debug=debug)
        extraction_count += 1
        links_followed += 1

        if extracted is None:
            continue

        for report in extracted.get("reports", []):
            reports.append(_build_report_entry(report, url, parent_confidence, now_iso))

        # Don't follow links from Stage 3 pages (one level of depth only)

    stage3_count = len(reports) - stage2_count
    if stage3_count:
        _debug_log(debug, f"Stage 3: found {stage3_count} additional report(s) from {links_followed} linked page(s)")
        notes.append(f"Link following: {stage3_count} additional report(s) from {links_followed} linked page(s)")

    # --- Deduplicate reports by URL ---

    seen_report_urls: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for report in reports:
        report_key = _normalize_url(report.get("pdf_url") or report["url"])
        if report_key not in seen_report_urls:
            seen_report_urls.add(report_key)
            deduped.append(report)
    reports = deduped

    # --- Collapse mirror entries (same audit, different host) -------------
    pre_mirror = len(reports)
    reports = _collapse_same_audit_mirrors(reports)
    if pre_mirror != len(reports):
        notes.append(f"Mirror dedup: collapsed {pre_mirror - len(reports)} entry(ies)")

    notes.append(f"Extracted {len(reports)} audit report(s)")
    notes.append(f"Tavily queries used: {queries_used[0]}/{max_queries}")

    _debug_log(
        debug,
        f"Audit report discovery complete: {len(reports)} report(s), "
        f"extractions={extraction_count}, queries={queries_used[0]}/{max_queries}",
    )

    return {
        "company": clean_company,
        "official_domain": official_domain,
        "reports": reports,
        "queries_used": queries_used[0],
        "errors": errors[:12],
        "notes": notes[:12],
    }


def _empty_result(
    company: str,
    domain: str | None,
    queries_used: int,
    errors: list,
    notes: list,
) -> dict[str, Any]:
    return {
        "company": company,
        "official_domain": domain,
        "reports": [],
        "queries_used": queries_used,
        "errors": errors[:12],
        "notes": notes[:12],
    }


# ---------------------------------------------------------------------------
# Append-only merge
# ---------------------------------------------------------------------------


def _richness(report: dict[str, Any]) -> int:
    """Count non-null detail fields as a richness score."""
    return sum(1 for key in ("pdf_url", "date") if report.get(key))


def merge_audit_reports(prev: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """Merge previous and new audit report results (append-only).

    - URL-keyed dedup: reports in both keep the richer entry (prefer new on tie).
    - Reports only in prev are kept unchanged — never removed.
    - Reports only in new are added.
    """
    prev_reports = {
        _normalize_url(r["url"]): r
        for r in prev.get("reports", [])
        if r.get("url")
    }
    new_reports = {
        _normalize_url(r["url"]): r
        for r in new.get("reports", [])
        if r.get("url")
    }

    merged: dict[str, dict[str, Any]] = {}

    for url_key, report in new_reports.items():
        if url_key in prev_reports:
            if _richness(prev_reports[url_key]) > _richness(report):
                merged[url_key] = prev_reports[url_key]
            else:
                merged[url_key] = report
        else:
            merged[url_key] = report

    for url_key, report in prev_reports.items():
        if url_key not in new_reports:
            merged[url_key] = report

    sorted_reports = sorted(
        merged.values(),
        key=lambda r: (r.get("date") or "", r.get("confidence") or 0),
        reverse=True,
    )

    return {
        "company": new.get("company", prev.get("company")),
        "official_domain": new.get("official_domain") or prev.get("official_domain"),
        "reports": sorted_reports,
        "queries_used": new.get("queries_used"),
        "errors": new.get("errors"),
        "notes": new.get("notes"),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover audit reports for a protocol")
    parser.add_argument("company", help="Company or protocol name")
    parser.add_argument("--domain", default=None, help="Official domain hint")
    parser.add_argument("--max-queries", type=int, default=2, help="Tavily query cap")
    parser.add_argument("--debug", action="store_true", help="Print debug logs to stderr")
    parser.add_argument("--no-save", action="store_true", help="Print to stdout only, do not save to file")
    args = parser.parse_args()

    try:
        result = search_audit_reports(
            args.company,
            official_domain=args.domain,
            max_queries=args.max_queries,
            debug=args.debug,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    output = json.dumps(result, indent=2)

    if args.no_save:
        print(output)
        return

    from pathlib import Path

    safe_name = args.company.replace("/", "_").replace(" ", "_")
    out_dir = Path("protocols") / safe_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "audit_reports.json"
    out_path.write_text(output + "\n")
    print(f"\nSaved to {out_path}")
    report_count = len(result.get("reports", []))
    print(f"Found {report_count} audit report(s)")


if __name__ == "__main__":
    main()
