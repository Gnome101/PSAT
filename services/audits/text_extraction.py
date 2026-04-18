"""Download audit PDFs, extract text, store it in object storage.

I/O-only at this layer — orchestration (claiming rows, DB state, rate
limiting) lives in ``workers.audit_text_extraction``. Importable and
testable without DB or S3.

``process_audit_report`` chains ``download_pdf`` → ``extract_text_from_pdf``
→ ``store_audit_text`` and returns an ``ExtractionOutcome`` the worker
persists.
"""

from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Final

import requests

from db.storage import StorageUnavailable, get_storage_client

logger = logging.getLogger(__name__)

# Size cap: >50MB is almost always a scanned-image dump anyway, plus OOM
# protection against hostile hosts. Under-500-char extracts get marked
# ``skipped`` as image-only PDFs (OCR is out of scope).
_MAX_PDF_BYTES: Final[int] = 50 * 1024 * 1024
_CONNECT_TIMEOUT: Final[float] = 10.0
_READ_TIMEOUT: Final[float] = 60.0
_MIN_USEFUL_TEXT_LENGTH: Final[int] = 500

# CDNs often serve PDFs as ``application/octet-stream`` — accept those too.
# Non-matching content-types short-circuit so we don't parse HTML error
# pages as PDFs.
_ACCEPTED_CONTENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "application/pdf",
        "application/octet-stream",
        "binary/octet-stream",
        "application/x-pdf",
    }
)

AUDIT_TEXT_CONTENT_TYPE: Final[str] = "text/plain; charset=utf-8"


# --- Errors ---------------------------------------------------------------


class TextExtractionError(RuntimeError):
    """Base class for failures during the download/extract/store flow."""


class PdfDownloadError(TextExtractionError):
    """HTTP or transport failure fetching the PDF body."""


class PdfTooLargeError(TextExtractionError):
    """Server-reported or streamed content exceeded ``_MAX_PDF_BYTES``."""


class PdfParseError(TextExtractionError):
    """pypdf could not parse the body (encrypted, corrupted, not a PDF)."""


class StorageWriteError(TextExtractionError):
    """Object storage write failed or storage is not configured."""


# --- Result type ----------------------------------------------------------


@dataclass(frozen=True)
class ExtractionOutcome:
    """Structured result of ``process_audit_report``.

    Exactly one of ``storage_key`` / ``error`` is non-None for a given status.
    ``status`` mirrors the ``AuditReport.text_extraction_status`` enum strings.
    """

    status: str  # "success" | "failed" | "skipped"
    storage_key: str | None = None
    text_size_bytes: int | None = None
    text_sha256: str | None = None
    error: str | None = None


# --- Storage key ---------------------------------------------------------


def audit_text_key(audit_report_id: int) -> str:
    """Deterministic object-storage key for an audit's extracted text body."""
    return f"audits/text/{int(audit_report_id)}.txt"


# --- Download ------------------------------------------------------------


def download_pdf(url: str, session: requests.Session | None = None) -> bytes:
    """Fetch a PDF by URL. Streams to memory with a hard size cap.

    Raises ``PdfDownloadError`` for network / HTTP failures and
    ``PdfTooLargeError`` when the body exceeds ``_MAX_PDF_BYTES``. Rejects
    responses whose Content-Type is clearly not a PDF.
    """
    sess = session or requests
    try:
        resp = sess.get(
            url,
            timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT),
            stream=True,
            headers={"User-Agent": "PSAT-audit-text-extractor/0.1"},
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise PdfDownloadError(f"fetch error: {exc}") from exc

    try:
        if resp.status_code != 200:
            raise PdfDownloadError(f"HTTP {resp.status_code}")

        content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
        # GitHub serves raw PDFs with content-type application/pdf; gitbook
        # CDNs often use octet-stream. Reject text/html / application/json
        # etc. — we've been redirected to an error page.
        if content_type and content_type not in _ACCEPTED_CONTENT_TYPES:
            raise PdfDownloadError(f"unexpected content-type {content_type!r}")

        # Server-reported size check — saves us the round trip if we can
        # tell upfront the body is too big.
        content_length = resp.headers.get("content-length")
        if content_length and content_length.isdigit() and int(content_length) > _MAX_PDF_BYTES:
            raise PdfTooLargeError(f"Content-Length {content_length} exceeds cap {_MAX_PDF_BYTES}")

        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_content(chunk_size=131_072):
            if not chunk:
                continue
            total += len(chunk)
            if total > _MAX_PDF_BYTES:
                raise PdfTooLargeError(f"streamed body exceeded cap {_MAX_PDF_BYTES}")
            chunks.append(chunk)

        return b"".join(chunks)
    finally:
        resp.close()


# --- Extract -------------------------------------------------------------


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Parse a PDF page-by-page with ``\\f\\n--- page {n} ---\\n\\f`` separators.

    Scope extraction uses the markers to recover page boundaries without
    re-parsing. Empty/short output is the caller's call to skip. Raises
    ``PdfParseError`` when pypdf rejects the body outright.
    """
    try:
        from pypdf import PdfReader
        from pypdf.errors import PdfReadError
    except ImportError as exc:  # pragma: no cover - dep configured in pyproject
        raise PdfParseError(f"pypdf import failed: {exc}") from exc

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except PdfReadError as exc:
        raise PdfParseError(f"not a valid PDF: {exc}") from exc
    except Exception as exc:  # pypdf sometimes raises plain ValueError
        raise PdfParseError(f"pypdf failed: {exc}") from exc

    if getattr(reader, "is_encrypted", False):
        # Empty-password decrypt covers legacy print-protection PDFs.
        try:
            if not reader.decrypt(""):
                raise PdfParseError("encrypted PDF; no password available")
        except Exception as exc:
            raise PdfParseError(f"encrypted PDF decrypt failed: {exc}") from exc

    parts: list[str] = []
    for idx, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception as exc:
            logger.warning("pypdf page %d extract_text raised: %s", idx, exc)
            text = ""
        parts.append(f"\f\n--- page {idx} ---\n\f\n{text}")

    return "".join(parts).strip()


# --- Store ---------------------------------------------------------------


def store_audit_text(
    audit_report_id: int,
    text: str,
) -> tuple[str, int, str]:
    """Upload an audit's extracted text to object storage.

    Returns ``(storage_key, size_bytes, sha256_hex)``. Raises
    ``StorageWriteError`` if storage isn't configured or the put fails.
    """
    client = get_storage_client()
    if client is None:
        raise StorageWriteError("object storage not configured (ARTIFACT_STORAGE_* env vars unset)")

    body = text.encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()
    key = audit_text_key(audit_report_id)

    try:
        client.put(
            key,
            body,
            AUDIT_TEXT_CONTENT_TYPE,
            metadata={
                "audit_report_id": str(audit_report_id),
                "sha256": digest,
            },
        )
    except StorageUnavailable as exc:
        raise StorageWriteError(f"storage put failed: {exc}") from exc

    return key, len(body), digest


# --- Orchestration ---------------------------------------------------------


def process_audit_report(
    audit_report_id: int,
    url: str,
    session: requests.Session | None = None,
) -> ExtractionOutcome:
    """Run download → parse → store for one audit. Never raises.

    Typed errors from each stage become ``ExtractionOutcome(status=...)``
    the worker can persist directly; unexpected exceptions still surface
    as ``status="failed"`` with the error message captured.
    """
    if not url:
        return ExtractionOutcome(status="failed", error="no URL on audit row")

    try:
        body = download_pdf(url, session=session)
    except PdfTooLargeError as exc:
        return ExtractionOutcome(status="skipped", error=f"pdf too large: {exc}")
    except PdfDownloadError as exc:
        return ExtractionOutcome(status="failed", error=f"download: {exc}")
    except Exception as exc:
        # pypdf can raise unbounded types on malformed input; don't let one
        # broken PDF kill the worker loop.
        logger.exception("unexpected download error for %s", url)
        return ExtractionOutcome(status="failed", error=f"download: {exc!r}")

    try:
        text = extract_text_from_pdf(body)
    except PdfParseError as exc:
        return ExtractionOutcome(status="failed", error=f"parse: {exc}")
    except Exception as exc:
        logger.exception("unexpected parse error for %s", url)
        return ExtractionOutcome(status="failed", error=f"parse: {exc!r}")

    if len(text) < _MIN_USEFUL_TEXT_LENGTH:
        return ExtractionOutcome(
            status="skipped",
            error=(
                f"extracted text is {len(text)} chars (< {_MIN_USEFUL_TEXT_LENGTH}) — "
                f"likely image-only PDF, OCR required"
            ),
        )

    try:
        key, size, digest = store_audit_text(audit_report_id, text)
    except StorageWriteError as exc:
        return ExtractionOutcome(status="failed", error=f"store: {exc}")
    except Exception as exc:
        logger.exception("unexpected store error for audit %s", audit_report_id)
        return ExtractionOutcome(status="failed", error=f"store: {exc!r}")

    return ExtractionOutcome(
        status="success",
        storage_key=key,
        text_size_bytes=size,
        text_sha256=digest,
    )


# --- CLI ------------------------------------------------------------------


def _cli() -> None:  # pragma: no cover - dev-only entrypoint
    """Dry-run the download+extract pipeline against a URL.

    Usage: ``python -m services.audits.text_extraction <url> [--save PATH]``

    Skips the object-storage write — useful for confirming the network and
    pypdf paths work against real audit PDFs without needing DB / S3 set up.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Download an audit PDF and print the extracted text.",
    )
    parser.add_argument("url", help="Full URL of the PDF (gh raw, CDN, etc.)")
    parser.add_argument(
        "--save",
        metavar="PATH",
        help="Also write the extracted text to this local path.",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=800,
        help="Print only the first N chars (default: 800).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    print(f"→ downloading {args.url}", file=sys.stderr)
    try:
        body = download_pdf(args.url)
    except (PdfDownloadError, PdfTooLargeError) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print(f"  {len(body):,} bytes", file=sys.stderr)

    print("→ extracting text", file=sys.stderr)
    try:
        text = extract_text_from_pdf(body)
    except PdfParseError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print(f"  {len(text):,} chars", file=sys.stderr)
    if len(text) < _MIN_USEFUL_TEXT_LENGTH:
        print(
            f"  ⚠ under {_MIN_USEFUL_TEXT_LENGTH} chars — worker would SKIP this as image-only PDF",
            file=sys.stderr,
        )

    if args.save:
        from pathlib import Path

        Path(args.save).write_text(text)
        print(f"→ saved to {args.save}", file=sys.stderr)

    print("-" * 60)
    print(text[: args.head])
    if len(text) > args.head:
        print(f"... ({len(text) - args.head:,} more chars)")


if __name__ == "__main__":
    _cli()
