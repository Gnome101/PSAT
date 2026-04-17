"""Audit-report post-processing services.

This package holds the pieces that run *after* discovery:
  - ``text_extraction`` — download audit PDFs, extract text, store the text in
    object storage, and update the ``AuditReport`` row's extraction state.
"""

from .text_extraction import (
    AUDIT_TEXT_CONTENT_TYPE,
    ExtractionOutcome,
    audit_text_key,
    download_pdf,
    extract_text_from_pdf,
    process_audit_report,
    store_audit_text,
)

__all__ = [
    "AUDIT_TEXT_CONTENT_TYPE",
    "ExtractionOutcome",
    "audit_text_key",
    "download_pdf",
    "extract_text_from_pdf",
    "process_audit_report",
    "store_audit_text",
]
