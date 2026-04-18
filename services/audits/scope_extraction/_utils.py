"""Page-offset + ligature helpers shared across scope extraction submodules."""

from __future__ import annotations

import re
from typing import Final

# Matches the ``\f\n--- page {n} ---\n\f`` markers emitted by
# ``services.audits.text_extraction.extract_text_from_pdf``.
_PAGE_MARKER_RE: Final[re.Pattern[str]] = re.compile(r"\f\n--- page (\d+) ---\n\f")

# Unicode ligatures pypdf leaves in extracted text. Normalized so contract
# names like "EthfiL2Token" match across both halves of the pipeline.
_LIGATURE_MAP: Final[dict[str, str]] = {
    "\ufb00": "ff",
    "\ufb01": "fi",
    "\ufb02": "fl",
    "\ufb03": "ffi",
    "\ufb04": "ffl",
    "\ufb05": "ft",
    "\ufb06": "st",
}


def _normalize_ligatures(text: str) -> str:
    """Replace unicode ligatures (U+FB00..U+FB06) with their ASCII expansion."""
    for lig, ascii_pair in _LIGATURE_MAP.items():
        if lig in text:
            text = text.replace(lig, ascii_pair)
    return text


def _page_offsets(text: str) -> list[tuple[int, int]]:
    """Return ``[(page_number, start_offset), ...]`` plus a sentinel at ``len(text)``.

    ``extract_text_from_pdf`` strips its output, so the leading ``\\f`` of
    the first page marker is missing — we synthesize ``(1, 0)`` when no
    page-1 marker is captured so pre-first-marker offsets still resolve.
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


def scope_artifact_key(audit_report_id: int) -> str:
    """Deterministic object-storage key for an audit's scope JSON blob."""
    return f"audits/scope/{int(audit_report_id)}.json"
