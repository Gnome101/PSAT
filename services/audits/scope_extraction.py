"""Extract the list of in-scope contracts from an audit report's PDF text.

Runs *after* ``services.audits.text_extraction`` has put the parsed PDF body
into object storage. The worker in ``workers.audit_scope_extraction`` drives
this module; every function here is importable without a DB or S3.

Flow for one audit:
  1. ``locate_scope_section(text)`` — regex the page-annotated body for a
     "Scope" / "Files in scope" / "Smart Contracts in Scope" / "Code
     repository" / "Assessment scope" header, return 1-3 page slices.
  2. ``extract_scope_with_llm(sections, title, auditor)`` — send the slices
     to Gemini 2.0 Flash via OpenRouter, get back a JSON array of contract
     names.
  3. ``validate_contracts(names, raw_text)`` — drop any name that never
     appears in the raw body (hallucination guard).
  4. ``extract_date_from_pdf_text(text)`` — best-effort date pull from the
     title page so the worker can backfill ``AuditReport.date`` when it's
     null.

``process_audit_scope`` chains all four and returns a
``ScopeExtractionOutcome`` that the worker persists in one place.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from db.storage import StorageUnavailable, get_storage_client

logger = logging.getLogger(__name__)


# --- Constants ------------------------------------------------------------

PROMPT_VERSION: Final[str] = "scope-v1"

SCOPE_ARTIFACT_CONTENT_TYPE: Final[str] = "application/json"

# Token budget for the prompt payload. We only send the scope section(s),
# which are almost always under 10 KB. 40 KB is a hard cap to protect
# against a degenerate fixture that somehow slices in a huge block.
_MAX_SCOPE_TEXT_CHARS: Final[int] = 40_000

# Chunk-scan fallback: when header + content-pattern matching both find
# nothing, walk the PDF page-by-page in N-page windows and ask the LLM
# per chunk whether it contains a scope listing. Stops at the first hit.
# Sized to cover the first 20 pages, which catches ~95% of real reports
# without runaway cost (4 × ~$0.0003 = ~$0.0012 worst case).
_CHUNK_SCAN_PAGES_PER_CHUNK: Final[int] = 5
_CHUNK_SCAN_MAX_CHUNKS: Final[int] = 4
_CHUNK_SCAN_MAX_CHARS: Final[int] = 12_000

# Matches the ``\f\n--- page {n} ---\n\f`` markers emitted by
# ``extract_text_from_pdf``. Used to translate character offsets back to
# page numbers.
_PAGE_MARKER_RE: Final[re.Pattern[str]] = re.compile(r"\f\n--- page (\d+) ---\n\f")

# Scope-section headers. Ordered so longer phrases win when a body has
# multiple matches on the same line (e.g. "Files in scope" beats "Scope").
# Real-world samples this covers: Spearbit "Scope"/"Files in scope",
# Cantina "Files in scope", Certora "Project Scope", Halborn "5. Scope",
# Nethermind "2 Audited Files", Trail of Bits "Project Targets".
_SCOPE_HEADERS: Final[tuple[str, ...]] = (
    "smart contracts in scope",
    "contracts in scope",
    "files in scope",
    "items in scope",
    "audited files",
    "audited contracts",
    "in-scope contracts",
    "in scope contracts",
    "assessment scope",
    "audit scope",
    "project scope",
    "project targets",
    "code repository",
    "scope",
)

# Common unicode ligatures that pypdf leaves in the extracted text. We
# normalize these before the LLM call and before validation so contract
# names like "EthfiL2Token" match across both halves of the pipeline.
# Source: U+FB00-FB06 covers the ASCII ligatures in the PDF spec.
_LIGATURE_MAP: Final[dict[str, str]] = {
    "\ufb00": "ff",   # ﬀ
    "\ufb01": "fi",   # ﬁ
    "\ufb02": "fl",   # ﬂ
    "\ufb03": "ffi",  # ﬃ
    "\ufb04": "ffl",  # ﬄ
    "\ufb05": "ft",   # ﬅ
    "\ufb06": "st",   # ﬆ
}


def _normalize_ligatures(text: str) -> str:
    """Replace unicode ligatures (U+FB00..U+FB06) with their ASCII expansion."""
    for lig, ascii_pair in _LIGATURE_MAP.items():
        if lig in text:
            text = text.replace(lig, ascii_pair)
    return text


# Body-prose scope-introduction phrases. Used as a second pass when
# header matching finds nothing useful — catches multi-sub-audit reports
# (Certora) that list scope inline without a structural heading.
# Patterns are case-insensitive and tolerant of pypdf double-spacing.
# Each pattern is a multi-word phrase that introduces a scope listing;
# NOT a pattern that merely mentions "scope" in passing.
_SCOPE_CONTENT_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    # "The following contract list is included in the scope of this audit:"
    # (Certora)
    re.compile(
        r"the\s+following\s+(?:contract\s+list|file\s+list|list\s+of\s+\w+)\s+"
        r"(?:is|are|was|were)\s+(?:included|listed)\s+in\s+(?:the\s+)?scope",
        re.IGNORECASE,
    ),
    # "The following files/contracts [are] in scope" / "...reviewed" / "...audited"
    re.compile(
        r"the\s+following\s+(?:smart\s+)?(?:files?|contracts?)\s+"
        r"(?:are|were|is|was|are\s+in\s+scope|were\s+in\s+scope|"
        r"reviewed|audited|assessed|included)",
        re.IGNORECASE,
    ),
    # "Audited/Reviewed/Assessed the following (files|contracts)"
    re.compile(
        r"(?:audited|reviewed|assessed)\s+the\s+following\s+(?:files?|contracts?)",
        re.IGNORECASE,
    ),
    # Header-ish phrases ending with a colon anywhere on a line.
    re.compile(
        r"(?:files?|contracts?|targets?)\s+(?:reviewed|audited|assessed|in\s+scope)\s*:",
        re.IGNORECASE,
    ),
)


# Matches e.g. "MorphoBlue.sol" or "Pool.vy". Starts with a capital letter
# to skip lowercase identifiers like "foo.sol" that rarely appear as real
# contract names. Used only by the regex fallback.
_SOL_FILE_RE: Final[re.Pattern[str]] = re.compile(r"\b([A-Z][A-Za-z0-9_]+)\.(?:sol|vy)\b")

# Date patterns for ``extract_date_from_pdf_text``. Kept conservative —
# each pattern returns a canonicalisable match, and ambiguous bodies
# simply return None instead of guessing.
_MONTH_NAMES = (
    "january|february|march|april|may|june|july|august|september|october|november|december"
    "|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
)
# Size of the "title region" that ``extract_date_from_pdf_text`` scans
# for a date. PDFs from auditors like Nethermind and Zellic have ~2k of
# title-page boilerplate before the delivery date appears, so 2k wasn't
# always enough. 6k covers cover page + "Executive Summary" + "Contents"
# without picking up body-text dates from later sections.
_DATE_SEARCH_CHARS: Final[int] = 6_000

_ORDINAL = r"(?:st|nd|rd|th)"  # "19th", "1st", "2nd", "3rd" — audit prose often uses these.
_DATE_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),  # 2024-12-19
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"),  # 12/19/2024 (interpreted US-style)
    re.compile(
        rf"\b(\d{{1,2}}){_ORDINAL}?\s+({_MONTH_NAMES})\s+(\d{{4}})\b",
        re.IGNORECASE,
    ),  # 19 December 2024 / 19th December 2024
    re.compile(
        rf"\b({_MONTH_NAMES})\s+(\d{{1,2}}){_ORDINAL}?,?\s+(\d{{4}})\b",
        re.IGNORECASE,
    ),  # December 19, 2024 / December 19th, 2024
    re.compile(rf"\b({_MONTH_NAMES})\s+(\d{{4}})\b", re.IGNORECASE),  # December 2024
)
_MONTH_TO_NUM: Final[dict[str, int]] = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}


# --- Errors ---------------------------------------------------------------


class ScopeExtractionError(RuntimeError):
    """Base class for recoverable failures during scope extraction."""


class LLMUnavailableError(ScopeExtractionError):
    """LLM call failed or returned unparseable output."""


# --- Result types ---------------------------------------------------------


@dataclass(frozen=True)
class ScopeSection:
    """One scope-section slice located by a header match."""

    start_page: int
    end_page: int
    header: str
    text_slice: str


@dataclass(frozen=True)
class ScopeExtractionOutcome:
    """Structured result of ``process_audit_scope``.

    ``status`` mirrors the ``AuditReport.scope_extraction_status`` values:
    "success" / "failed" / "skipped". ``method`` tells the worker how the
    contracts came out — "llm", "regex_fallback", or "cache_copy".
    """

    status: str
    contracts: tuple[str, ...] = ()
    storage_key: str | None = None
    extracted_date: str | None = None
    error: str | None = None
    method: str = "llm"
    raw_response: str | None = field(default=None, repr=False)
    model: str | None = None


# --- Keys -----------------------------------------------------------------


def scope_artifact_key(audit_report_id: int) -> str:
    """Deterministic object-storage key for an audit's scope JSON blob."""
    return f"audits/scope/{int(audit_report_id)}.json"


# --- Locate ---------------------------------------------------------------


def _page_offsets(text: str) -> list[tuple[int, int]]:
    """Return ``[(page_number, start_offset), ...]`` sorted by offset.

    The final sentinel entry points at ``len(text)`` so lookups stay simple.
    ``extract_text_from_pdf`` calls ``.strip()`` on its output, which drops
    the leading ``\\f`` of the first page marker — so the page-1 marker is
    invisible to ``_PAGE_MARKER_RE``. We synthesize ``(1, 0)`` in that
    case so offsets before the first captured marker don't fall through.
    """
    pages: list[tuple[int, int]] = []
    for m in _PAGE_MARKER_RE.finditer(text):
        pages.append((int(m.group(1)), m.end()))
    if not pages or pages[0][0] > 1:
        pages.insert(0, (1, 0))
    pages.append((pages[-1][0] + 1, len(text)))
    return pages


def _page_of_offset(pages: list[tuple[int, int]], offset: int) -> int:
    """Look up the page number containing ``offset``."""
    for i in range(len(pages) - 1):
        if pages[i][1] <= offset < pages[i + 1][1]:
            return pages[i][0]
    return pages[-2][0] if len(pages) >= 2 else 1


def locate_scope_section(text: str) -> list[ScopeSection]:
    """Find the scope section(s) in an audit's PDF text.

    Matches any of the ``_SCOPE_HEADERS`` case-insensitively at the start
    of a line, then captures ~3 pages of following context. Overlapping
    slices are merged. Returns [] when no header is found — the worker
    translates that into ``status='skipped'``.

    Ligatures are normalized first so headers written in a ligature-rich
    font (pypdf preserves ``ﬁ`` as U+FB01 rather than "fi") don't slip
    through the regex.
    """
    text = _normalize_ligatures(text)
    pages = _page_offsets(text)
    lower = text.lower()

    # Optional numbered-section prefix that real audit reports use
    # ("5. Scope", "5.1 Files in scope", "5 Scope"). Kept permissive but
    # anchored to line start so body prose doesn't match.
    _NUMBERED_PREFIX = r"(?:\d+(?:\.\d+)*\.?[ \t]+)?"
    sections: list[ScopeSection] = []
    seen_offsets: set[int] = set()
    for header in _SCOPE_HEADERS:
        # Allow any amount of same-line whitespace between header words.
        # pypdf occasionally emits double-spacing on section titles
        # (Certora's "Project  Scope" is a real-world example), so a
        # rigid single-space match would miss legitimate headers.
        header_words = r"[ \t]+".join(re.escape(w) for w in header.split())
        # Keep the whitespace classes non-newline-spanning ([ \t] not \s)
        # so the match doesn't slurp the preceding \f\n page-marker line
        # and land on the wrong page.
        pattern = re.compile(
            rf"^[ \t]*{_NUMBERED_PREFIX}{header_words}\b[ \t:]*$",
            re.MULTILINE,
        )
        # Capture ALL matches per header, not just the first. Real reports
        # often have a TOC entry ("5. Scope") on an early page and the
        # actual section ("5. SCOPE") later — we want the LLM to see both
        # because the TOC entry by itself has no contract names.
        for m in pattern.finditer(lower):
            start = m.start()
            if start in seen_offsets:
                continue
            seen_offsets.add(start)

            start_page = _page_of_offset(pages, start)
            # Include the starting page + 2 following pages. This reliably
            # covers Spearbit / Cantina / Certora scope tables, which are
            # rarely longer than 2-3 pages.
            end_page = min(start_page + 2, pages[-2][0])
            end_offset = next(
                (off for (p, off) in pages if p > end_page),
                len(text),
            )
            sections.append(
                ScopeSection(
                    start_page=start_page,
                    end_page=end_page,
                    header=header,
                    text_slice=text[start:end_offset],
                )
            )

    # Second pass: scope-introduction content patterns. Catches body prose
    # like "The following contract list is included in the scope of this
    # audit:" (Certora multi-sub-audit format) that header matching misses.
    # We use the raw text here (not lower-cased) only for offset accuracy;
    # the patterns are already re.IGNORECASE.
    for pattern in _SCOPE_CONTENT_PATTERNS:
        for m in pattern.finditer(text):
            start = m.start()
            if start in seen_offsets:
                continue
            seen_offsets.add(start)

            start_page = _page_of_offset(pages, start)
            # Content matches usually land right BEFORE the contract list,
            # so a 2-page window is enough to capture a bulleted list.
            end_page = min(start_page + 1, pages[-2][0])
            end_offset = next(
                (off for (p, off) in pages if p > end_page),
                len(text),
            )
            sections.append(
                ScopeSection(
                    start_page=start_page,
                    end_page=end_page,
                    header=f"content:{m.group(0)[:40].strip()}",
                    text_slice=text[start:end_offset],
                )
            )

    # Merge overlapping slices (e.g. "Scope" on p.2 and "Files in scope"
    # on p.3 — their 3-page windows overlap, one covers the other).
    sections.sort(key=lambda s: s.start_page)
    merged: list[ScopeSection] = []
    for s in sections:
        if merged and s.start_page <= merged[-1].end_page:
            prev = merged[-1]
            merged[-1] = ScopeSection(
                start_page=prev.start_page,
                end_page=max(prev.end_page, s.end_page),
                header=prev.header,
                text_slice=prev.text_slice,
            )
        else:
            merged.append(s)
    return merged


# --- LLM call -------------------------------------------------------------


_SCOPE_PROMPT_TEMPLATE = """\
You are extracting the list of contracts that were in scope for a smart-contract security audit.

Audit title: {title}
Auditor: {auditor}

Below is the scope section(s) from the audit report PDF. Different auditors \
use different formats (markdown tables, bulleted URL lists, line-count \
tables, flat src/ trees, prose enumeration) — extract every contract \
regardless of format.

Rules:
- Return a JSON array of contract names ONLY. No prose, no file paths, no \
explanations.
- Contract names are the basenames of .sol / .vy files WITHOUT the extension, \
e.g. "MorphoBlue", "Pool", "BundlerV3".
- Deduplicate names. If the same contract appears in multiple repos or \
across audit phases, include it once.
- EXCLUDE test files (Test*, *Test, *.t.sol), mocks (Mock*, *Mock), \
deployment scripts (Deploy*, *.s.sol), and anything explicitly marked \
"out of scope" or "reference only".

Avoid these specific false positives that trip up extraction:
- Do NOT treat project names, product names, or section headers as \
contract names. "EtherFi RewardsManager" as a section title is not the \
same as a RewardsManager.sol file being audited.
- Do NOT include EXTERNAL dependencies described in a "System Overview" \
or architecture-background section — these are typically contracts the \
protocol INTEGRATES with (e.g. "Hyperliquid's CoreWriter at 0x3333..."), \
not audit targets.
- If a contract is mentioned ONLY in a single finding (e.g. \
"Issue L-01: BeaconFactory is vulnerable") without also appearing in a \
scope list or tree, it's probably not in scope — but if it IS in a scope \
list AND discussed in findings, include it normally.

Include these as in-scope when they appear:
- Contracts listed in an explicit scope section, audited-files table, \
"Program:" declaration, "Target:" field, flat src/ tree printout, or \
a bulleted contract list.
- Contracts the audit clearly reviewed even if the structural header is \
missing — a flat listing of .sol filenames IS a scope signal.

If no contracts can be identified from the text, return an empty array [].

Scope section text:
---
{scope_text}
---

Respond with the JSON array only."""


def _build_prompt(sections: list[ScopeSection], title: str, auditor: str) -> str:
    joined = "\n\n===\n\n".join(s.text_slice for s in sections)
    if len(joined) > _MAX_SCOPE_TEXT_CHARS:
        joined = joined[:_MAX_SCOPE_TEXT_CHARS]
    return _SCOPE_PROMPT_TEMPLATE.format(
        title=title or "(unknown)",
        auditor=auditor or "(unknown)",
        scope_text=joined,
    )


def _call_llm(prompt: str) -> tuple[str, str]:
    """Call the LLM, returning ``(response_text, model_identifier)``.

    If ``PSAT_LLM_STUB_DIR`` is set, route to fixture files keyed by the
    SHA-256 of the prompt. Lets integration tests run deterministically
    without touching OpenRouter. Falls back to ``_default.json`` when no
    specific digest file exists.
    """
    stub_dir = os.environ.get("PSAT_LLM_STUB_DIR")
    if stub_dir:
        digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        base = Path(stub_dir)
        specific = base / f"{digest}.json"
        if specific.exists():
            return specific.read_text(), f"stub:{digest[:12]}"
        default = base / "_default.json"
        if default.exists():
            return default.read_text(), "stub:_default"
        raise LLMUnavailableError(f"no LLM stub for prompt digest {digest} in {stub_dir}")

    try:
        from utils.llm import openrouter
    except Exception as exc:  # pragma: no cover - dep configured in pyproject
        raise LLMUnavailableError(f"openrouter client unavailable: {exc}") from exc

    model = os.environ.get("PSAT_SCOPE_LLM_MODEL", "google/gemini-2.0-flash-001")
    try:
        response = openrouter.chat(
            [{"role": "user", "content": prompt}],
            model=model,
            max_tokens=2048,
            temperature=0.0,
        )
    except Exception as exc:
        raise LLMUnavailableError(f"LLM call failed: {exc}") from exc
    return response, model


def extract_scope_with_llm(sections: list[ScopeSection], title: str, auditor: str) -> tuple[list[str], str, str]:
    """Call the LLM for the scope list. Returns ``(names, raw_response, model)``.

    Raises ``LLMUnavailableError`` if the call fails or the response is not
    a JSON array of strings. Validation against the raw text (hallucination
    guard) happens later in ``validate_contracts``.
    """
    from services.discovery.audit_reports_llm import _parse_json_array

    prompt = _build_prompt(sections, title, auditor)
    response, model = _call_llm(prompt)
    parsed = _parse_json_array(response)
    if parsed is None:
        raise LLMUnavailableError(f"LLM returned unparseable output: {response[:200]!r}")

    names: list[str] = []
    seen: set[str] = set()
    for item in parsed:
        if isinstance(item, str):
            candidate = item.strip()
        elif isinstance(item, dict):
            # Tolerate model drift where it returns [{name: ...}, ...] even
            # though the prompt asked for strings.
            raw = item.get("contract_name") or item.get("name") or item.get("file")
            candidate = str(raw).strip() if raw else ""
        else:
            continue
        if not candidate:
            continue
        # Drop the .sol/.vy extension if the model leaked it in.
        stem = re.sub(r"\.(?:sol|vy)$", "", candidate, flags=re.IGNORECASE)
        key = stem.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(stem)
    return names, response, model


# --- Chunked fallback scan -----------------------------------------------


def _split_text_into_chunks(text: str) -> list[ScopeSection]:
    """Chop the full document into ``_CHUNK_SCAN_PAGES_PER_CHUNK``-page
    windows for the LLM chunk-scan fallback.

    Returns at most ``_CHUNK_SCAN_MAX_CHUNKS`` chunks covering the start of
    the document. Real-world scope sections appear in the first 10-20
    pages of ~95% of audits, so leaving the tail unsearched bounds cost
    without losing meaningful coverage.
    """
    pages = _page_offsets(text)
    # pages ends with a sentinel entry at len(text); iterate in
    # _CHUNK_SCAN_PAGES_PER_CHUNK-page strides over the actual pages.
    real_pages = pages[:-1]
    chunks: list[ScopeSection] = []
    for i in range(
        0,
        len(real_pages),
        _CHUNK_SCAN_PAGES_PER_CHUNK,
    ):
        if len(chunks) >= _CHUNK_SCAN_MAX_CHUNKS:
            break
        start_page, start_off = real_pages[i]
        end_idx = min(i + _CHUNK_SCAN_PAGES_PER_CHUNK, len(real_pages))
        # end_off is the next page's start offset, or the sentinel end.
        if end_idx < len(real_pages):
            end_off = real_pages[end_idx][1]
            end_page = real_pages[end_idx][0] - 1
        else:
            end_off = len(text)
            end_page = real_pages[-1][0]
        slice_text = text[start_off:end_off][:_CHUNK_SCAN_MAX_CHARS]
        if not slice_text.strip():
            continue
        chunks.append(
            ScopeSection(
                start_page=start_page,
                end_page=end_page,
                header=f"chunk-scan:p{start_page}-{end_page}",
                text_slice=slice_text,
            )
        )
    return chunks


def extract_scope_via_chunk_scan(
    text: str, title: str, auditor: str
) -> tuple[list[str], str, str, int, ScopeSection | None]:
    """Brute-force fallback: walk the first ~20 pages chunk-by-chunk and
    ask the LLM if each chunk contains a scope listing. Stop at the first
    chunk that returns a non-empty list.

    Returns ``(names, raw_response, model, chunks_consumed, winning_chunk)``
    where ``winning_chunk`` is the ``ScopeSection`` whose LLM call produced
    ``names`` (or None when no chunk matched). Raises ``LLMUnavailableError``
    only if *every* chunk call fails.

    The per-chunk prompt is the same as the structured extraction prompt,
    and the parser is the same too — the LLM is already good at returning
    [] when no scope content is present, so we don't need special phrasing.
    """
    chunks = _split_text_into_chunks(text)
    if not chunks:
        raise LLMUnavailableError("chunk-scan: text is empty")

    last_error: Exception | None = None
    for idx, chunk in enumerate(chunks, start=1):
        try:
            names, response, model = extract_scope_with_llm(
                [chunk], title, auditor
            )
        except LLMUnavailableError as exc:
            last_error = exc
            continue
        if names:
            logger.info(
                "scope: chunk-scan found %d name(s) in chunk %d/%d (pages %d-%d)",
                len(names),
                idx,
                len(chunks),
                chunk.start_page,
                chunk.end_page,
            )
            return names, response, model, idx, chunk

    # No chunk yielded names. If every call failed, propagate; otherwise
    # report an empty successful scan so the caller can mark 'skipped'.
    if last_error is not None and all(
        isinstance(e, LLMUnavailableError)
        for e in [last_error]
    ):
        raise LLMUnavailableError(
            f"chunk-scan: all {len(chunks)} chunks failed; last: {last_error}"
        )
    return [], "", "", len(chunks), None


# --- Validate -------------------------------------------------------------


def validate_contracts(names: list[str], raw_text: str) -> list[str]:
    """Drop names that never appear in ``raw_text``.

    Hallucination guard — the LLM occasionally invents plausible contract
    names (e.g. an audit of MorphoBlue mentions "IPool" and the model
    extrapolates to "Pool"). A verbatim case-insensitive substring check
    against the raw PDF text catches these.
    """
    if not names:
        return []
    haystack = raw_text.lower()
    kept: list[str] = []
    for name in names:
        if not name:
            continue
        if name.lower() in haystack:
            kept.append(name)
        else:
            logger.info("scope: dropped hallucinated contract name %r", name)
    return kept


# --- Regex fallback -------------------------------------------------------


def extract_contracts_regex_fallback(sections_text: str) -> list[str]:
    """Scrape contract names from scope text using only regex.

    Used when the LLM is unavailable or returns unparseable output. Picks
    up any capitalized ``Word.sol`` / ``Word.vy`` filename, strips the
    extension, dedupes. Tolerates scraping junk — the caller still runs
    ``validate_contracts`` so bogus matches get dropped.
    """
    seen: set[str] = set()
    names: list[str] = []
    for m in _SOL_FILE_RE.finditer(sections_text):
        stem = m.group(1)
        key = stem.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(stem)
    return names


# --- Date extraction ------------------------------------------------------


def extract_date_from_pdf_text(text: str) -> str | None:
    """Best-effort extraction of the audit date from the title page.

    Returns an ISO-8601 ``YYYY-MM-DD`` (or ``YYYY-MM-00`` for month-only
    matches) when a date pattern hits in the title region (first
    ``_DATE_SEARCH_CHARS`` characters), else None. The worker uses this
    only to backfill ``AuditReport.date`` when the discovery-time value
    is null or incomplete — ``_maybe_backfill_date`` prevents us from
    clobbering a correct pre-existing date.

    Supports five common formats:
      - ISO ``YYYY-MM-DD``
      - US-style ``MM/DD/YYYY`` (slashes)
      - ``DD Month YYYY`` with optional ordinal (e.g. ``19th December 2024``)
      - ``Month DD, YYYY`` with optional ordinal (e.g. ``December 19th, 2024``)
      - ``Month YYYY`` (month-only fallback, returns ``YYYY-MM-00``)
    """
    head = text[:_DATE_SEARCH_CHARS]
    for pat in _DATE_PATTERNS:
        m = pat.search(head)
        if not m:
            continue
        try:
            groups = m.groups()
            if len(groups) == 3 and groups[0].isdigit() and len(groups[0]) == 4:
                # YYYY-MM-DD
                year = int(groups[0])
                month = int(groups[1])
                day = int(groups[2])
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            if len(groups) == 3 and groups[0].isdigit() and groups[1].isdigit():
                # Slash-separated date: one of DD/MM/YYYY or MM/DD/YYYY.
                # We disambiguate using the values themselves:
                #   - first > 12  → must be DD/MM/YYYY (flip)
                #   - second > 12 → must be MM/DD/YYYY (keep)
                #   - both ≤ 12   → AMBIGUOUS. Skip to next pattern rather
                #                   than silently assuming US order; some
                #                   firms (Certora) use DD/MM, and guessing
                #                   wrong produces plausible-but-wrong dates.
                a = int(groups[0])
                b = int(groups[1])
                year = int(groups[2])
                if a > 12 and b <= 12:
                    month, day = b, a
                elif b > 12 and a <= 12:
                    month, day = a, b
                else:
                    # Both ≤12 (or both >12, impossible for valid date).
                    # Defer to a less ambiguous pattern below.
                    continue
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            if len(groups) == 3 and groups[0].isdigit():
                # DD Month YYYY
                day = int(groups[0])
                month = _MONTH_TO_NUM.get(groups[1].lower())
                year = int(groups[2])
                if month is None:
                    continue
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            if len(groups) == 3:
                # Month DD, YYYY
                month = _MONTH_TO_NUM.get(groups[0].lower())
                if month is None:
                    continue
                day = int(groups[1])
                year = int(groups[2])
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            if len(groups) == 2:
                # Month YYYY — no day
                month = _MONTH_TO_NUM.get(groups[0].lower())
                if month is None:
                    continue
                year = int(groups[1])
                return f"{year:04d}-{month:02d}-00"
        except (ValueError, KeyError):
            continue
    return None


# --- Artifact payload -----------------------------------------------------


def build_artifact_payload(
    contracts: list[str],
    *,
    method: str,
    model: str | None,
    extracted_date: str | None,
    raw_response: str | None,
    scope_section_text: str | None = None,
) -> dict[str, object]:
    """Return the JSON body that gets stored at ``scope_artifact_key``.

    ``raw_response`` is included for method='llm' so the extraction is
    replayable and debuggable. For method='regex_fallback' we store the
    generating regex names so there's no confusion about origin.
    ``scope_section_text`` is the text slice the LLM actually saw — for
    header/content-pattern matches that's the merged section slice, for
    chunk-scan that's the winning chunk. Capped at 20k chars so the
    artifact stays manageable.
    """
    sliced = scope_section_text[:20_000] if scope_section_text else None
    return {
        "contracts": list(contracts),
        "extracted_date": extracted_date,
        "method": method,
        "model": model,
        "prompt_version": PROMPT_VERSION,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "raw_llm_response": raw_response,
        "scope_section_text": sliced,
    }


def _store_artifact(
    audit_id: int,
    payload: dict[str, object],
) -> str | None:
    """Write the payload to object storage, returning the key.

    Returns None (without raising) if storage isn't configured — the
    contracts still get persisted to ``scope_contracts``, the blob is
    a debug nice-to-have, not load-bearing.
    """
    client = get_storage_client()
    if client is None:
        logger.warning(
            "scope: storage client unavailable; skipping artifact write for audit %s",
            audit_id,
        )
        return None
    key = scope_artifact_key(audit_id)
    body = json.dumps(payload, sort_keys=False).encode("utf-8")
    try:
        client.put(
            key,
            body,
            SCOPE_ARTIFACT_CONTENT_TYPE,
            metadata={
                "audit_report_id": str(audit_id),
                "method": str(payload.get("method") or ""),
            },
        )
    except StorageUnavailable as exc:
        logger.warning("scope: storage put failed for %s: %s", audit_id, exc)
        return None
    return key


# --- Orchestration --------------------------------------------------------


def process_audit_scope(
    audit_report_id: int,
    text_storage_key: str,
    text_sha256: str | None,
    audit_title: str,
    auditor: str,
) -> ScopeExtractionOutcome:
    """Full scope-extraction pipeline for one audit.

    Fetches the previously-extracted PDF text from object storage, finds
    the scope section, calls the LLM (or falls back to regex), validates
    the results against the raw text, and writes a JSON artifact.

    Never raises: any failure becomes ``status="failed"`` with ``error``
    populated. No-header bodies become ``status="skipped"``.
    """
    client = get_storage_client()
    if client is None:
        return ScopeExtractionOutcome(
            status="failed",
            error="object storage not configured (ARTIFACT_STORAGE_* env vars unset)",
        )

    try:
        raw_bytes = client.get(text_storage_key)
    except StorageUnavailable as exc:
        return ScopeExtractionOutcome(status="failed", error=f"storage get failed: {exc}")
    except Exception as exc:
        logger.exception("scope: unexpected storage error for audit %s", audit_report_id)
        return ScopeExtractionOutcome(status="failed", error=f"storage: {exc!r}")

    try:
        raw_text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        return ScopeExtractionOutcome(status="failed", error=f"text decode: {exc}")

    # Normalize ligatures once, up front. Downstream helpers can do it
    # again defensively but work is idempotent.
    raw_text = _normalize_ligatures(raw_text)
    extracted_date = extract_date_from_pdf_text(raw_text)

    sections = locate_scope_section(raw_text)

    method = "llm"
    raw_response: str | None = None
    model: str | None = None
    names: list[str] = []
    # Track the text the LLM actually saw — persisted as
    # scope_section_text on the artifact so future debugging can answer
    # "why did the model extract what it extracted?".
    llm_input_text: str | None = None

    if sections:
        llm_input_text = "\n\n===\n\n".join(s.text_slice for s in sections)
        try:
            names, raw_response, model = extract_scope_with_llm(
                sections, audit_title, auditor
            )
        except LLMUnavailableError as exc:
            logger.warning(
                "scope: LLM unavailable for audit %s (%s); falling back to regex",
                audit_report_id,
                exc,
            )
            combined = "\n".join(s.text_slice for s in sections)
            names = extract_contracts_regex_fallback(combined)
            method = "regex_fallback"
            raw_response = json.dumps(
                {"_fallback": "regex", "error": str(exc)},
                sort_keys=False,
            )

    validated = validate_contracts(names, raw_text)

    # Fallback: neither header nor content-pattern matching yielded a
    # scope section, OR the located sections contained no real contract
    # names. Walk the first ~20 pages chunk-by-chunk asking the LLM to
    # find scope. Cheap, bounded, and catches reports with no structural
    # scope header (Solidified) or with scope scattered across body prose
    # past our fixed page window (Certora multi-audit format).
    if not validated:
        try:
            (
                cs_names,
                cs_response,
                cs_model,
                chunks_used,
                winning_chunk,
            ) = extract_scope_via_chunk_scan(raw_text, audit_title, auditor)
        except LLMUnavailableError as exc:
            logger.warning(
                "scope: chunk-scan unavailable for audit %s: %s",
                audit_report_id,
                exc,
            )
            cs_names, cs_response, cs_model, chunks_used, winning_chunk = (
                [],
                "",
                None,
                0,
                None,
            )
        if cs_names:
            validated = validate_contracts(cs_names, raw_text)
            if validated:
                method = "llm_chunk_scan"
                raw_response = cs_response
                model = cs_model
                # Swap to the winning chunk's text — what the LLM actually
                # saw that produced these names. Without this, chunk-scan
                # outcomes are un-debuggable (no provenance for the
                # contract names).
                if winning_chunk is not None:
                    llm_input_text = winning_chunk.text_slice
                logger.info(
                    "scope: audit %s recovered via chunk-scan (%d chunks, %d names)",
                    audit_report_id,
                    chunks_used,
                    len(validated),
                )

    if not validated:
        return ScopeExtractionOutcome(
            status="skipped",
            error=(
                "no scope section found: header + content-pattern + chunk-scan all empty"
                if not sections
                else "scope section found but extraction + chunk-scan yielded no valid contracts"
            ),
            method=method,
            raw_response=raw_response,
            model=model,
            extracted_date=extracted_date,
        )

    payload = build_artifact_payload(
        validated,
        method=method,
        model=model,
        extracted_date=extracted_date,
        raw_response=raw_response,
        scope_section_text=llm_input_text,
    )
    storage_key = _store_artifact(audit_report_id, payload)

    return ScopeExtractionOutcome(
        status="success",
        contracts=tuple(validated),
        storage_key=storage_key,
        extracted_date=extracted_date,
        method=method,
        raw_response=raw_response,
        model=model,
    )
