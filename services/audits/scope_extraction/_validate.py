"""Hallucination guard, regex fallback, and date extraction."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Final

logger = logging.getLogger(__name__)

# Capital-start to skip lowercase identifiers ("foo.sol") that rarely
# represent real contracts. Used only by the regex fallback.
_SOL_FILE_RE: Final[re.Pattern[str]] = re.compile(r"\b([A-Z][A-Za-z0-9_]+)\.(?:sol|vy)\b")

_MONTH_NAMES = (
    "january|february|march|april|may|june|july|august|september|october|november|december"
    "|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
)
# Title-region scan size. Some auditors (Nethermind, Zellic) push the
# delivery date past the first ~2k chars of title-page boilerplate.
_DATE_SEARCH_CHARS: Final[int] = 6_000

_ORDINAL = r"(?:st|nd|rd|th)"
_DATE_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),  # 2024-12-19
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"),  # 12/19/2024 or 19/12/2024
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


def validate_contracts(names: list[str], raw_text: str) -> list[str]:
    """Drop names that don't appear verbatim in ``raw_text``.

    An earlier iteration also collapsed ``IFoo``→``Foo`` duplicates to match
    Nethermind's LoC-table convention, but that produced 23 wrong drops vs
    11 right drops — Certora lists ``src/interfaces/IFoo.sol`` as
    first-class scope. The LLM's per-audit judgment handles this better.
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


def extract_contracts_regex_fallback(sections_text: str) -> list[str]:
    """Scrape ``Word.sol`` / ``Word.vy`` names when the LLM is unavailable.

    Tolerates noise — the caller still runs ``validate_contracts``.
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


def extract_date_from_pdf_text(text: str) -> str | None:
    """Best-effort ISO date from the title region (first 6000 chars).

    Returns ``YYYY-MM-DD`` for full dates, ``YYYY-MM-00`` for month-only
    matches, ``None`` on no hit or ambiguous slash formats. The worker
    only uses this to backfill a null ``AuditReport.date``.
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
                year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            if len(groups) == 3 and groups[0].isdigit() and groups[1].isdigit():
                # Slash date — disambiguate by values:
                #   first > 12  → DD/MM/YYYY
                #   second > 12 → MM/DD/YYYY
                #   both ≤ 12   → ambiguous, defer to next pattern
                a, b, year = int(groups[0]), int(groups[1]), int(groups[2])
                if a > 12 and b <= 12:
                    month, day = b, a
                elif b > 12 and a <= 12:
                    month, day = a, b
                else:
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
                day, year = int(groups[1]), int(groups[2])
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            if len(groups) == 2:
                # Month YYYY
                month = _MONTH_TO_NUM.get(groups[0].lower())
                if month is None:
                    continue
                year = int(groups[1])
                return f"{year:04d}-{month:02d}-00"
        except (ValueError, KeyError):
            continue
    return None
