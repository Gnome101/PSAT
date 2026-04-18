"""Pure-logic + contract-boundary tests for the audit-PDF text extractor.

The full worker loop (claim → download → extract → store → persist) is
covered end-to-end by ``test_audit_text_extraction_integration.py`` which
runs against real Postgres + real MinIO. What lives here is:

    - ``extract_text_from_pdf`` round-trip through real ``pypdf``
    - ``download_pdf`` HTTP contract boundaries that the integration test
      doesn't exercise because it stubs ``download_pdf`` wholesale
      (size caps, content-type rejection, transport errors)
    - ``audit_text_key`` deterministic key format

Anything that would re-mock the worker / storage / LLM paths belongs in
the integration test, not here.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.audits.text_extraction import (
    _ACCEPTED_CONTENT_TYPES,
    PdfDownloadError,
    PdfParseError,
    PdfTooLargeError,
    audit_text_key,
    download_pdf,
    extract_text_from_pdf,
)

# ---------------------------------------------------------------------------
# Minimal valid PDF fixture — kept pure-Python so the test doesn't pull in
# reportlab / fpdf2 as dev deps just for a roundtrip.
# ---------------------------------------------------------------------------


def _minimal_pdf_with_text(text: str) -> bytes:
    escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    content_stream = f"BT\n/F1 12 Tf\n50 750 Td\n({escaped}) Tj\nET\n".encode("ascii")
    objects: list[bytes] = [
        b"<</Type/Catalog/Pages 2 0 R>>",
        b"<</Type/Pages/Count 1/Kids[3 0 R]>>",
        b"<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>",
        (f"<</Length {len(content_stream)}>>\nstream\n".encode("ascii") + content_stream + b"endstream"),
        b"<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>",
    ]
    buf = bytearray(b"%PDF-1.4\n")
    xref_offsets: list[int] = []
    for idx, obj in enumerate(objects, start=1):
        xref_offsets.append(len(buf))
        buf += f"{idx} 0 obj\n".encode("ascii") + obj + b"\nendobj\n"
    xref_start = len(buf)
    buf += b"xref\n0 " + str(len(objects) + 1).encode("ascii") + b"\n"
    buf += b"0000000000 65535 f \n"
    for offset in xref_offsets:
        buf += f"{offset:010d} 00000 n \n".encode("ascii")
    buf += b"trailer\n<</Size " + str(len(objects) + 1).encode("ascii") + b"/Root 1 0 R>>\n"
    buf += b"startxref\n" + str(xref_start).encode("ascii") + b"\n%%EOF\n"
    return bytes(buf)


# ---------------------------------------------------------------------------
# extract_text_from_pdf — real pypdf roundtrip, no mocks
# ---------------------------------------------------------------------------


class TestExtractTextFromPdf:
    def test_roundtrips_simple_ascii(self):
        body = _minimal_pdf_with_text("Audit scope covers Pool.sol and Vault.sol.")
        text = extract_text_from_pdf(body)
        assert "Pool.sol" in text
        assert "Vault.sol" in text
        # Page markers must be preserved so the scope extractor can recover
        # page boundaries.
        assert "--- page 1 ---" in text

    def test_garbage_body_raises_parse_error(self):
        with pytest.raises(PdfParseError):
            extract_text_from_pdf(b"not a pdf at all")

    def test_empty_body_raises_parse_error(self):
        with pytest.raises(PdfParseError):
            extract_text_from_pdf(b"")


# ---------------------------------------------------------------------------
# download_pdf — boundary conditions around HTTP behaviour.
#
# These are *not* covered by the integration test: the integration suite
# stubs ``download_pdf`` wholesale so it can drop fixture PDFs in without
# a real HTTP server. The contract boundaries below (HTTP error, wrong
# content-type, oversize body) are the thing that would actually break in
# prod when a publisher changes their CDN behaviour.
# ---------------------------------------------------------------------------


def _mock_response(
    *,
    status_code: int = 200,
    content_type: str = "application/pdf",
    content_length: str | None = None,
    body: bytes = b"",
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {"content-type": content_type}
    if content_length is not None:
        resp.headers["content-length"] = content_length
    resp.iter_content.return_value = iter([body]) if body else iter([])
    resp.close = MagicMock()
    return resp


class TestDownloadPdfBoundaries:
    def test_happy_path_returns_bytes(self):
        session = MagicMock()
        session.get.return_value = _mock_response(body=b"%PDF-1.4\n...")
        assert download_pdf("https://example.com/a.pdf", session=session) == b"%PDF-1.4\n..."

    def test_non_200_raises_download_error(self):
        session = MagicMock()
        session.get.return_value = _mock_response(status_code=404)
        with pytest.raises(PdfDownloadError, match="HTTP 404"):
            download_pdf("https://example.com/missing.pdf", session=session)

    def test_rejects_html_content_type(self):
        """Etherscan / publisher soft-redirects (login walls, 200 HTML) are
        the main cause of misclassified bodies — guard against them."""
        session = MagicMock()
        session.get.return_value = _mock_response(content_type="text/html")
        with pytest.raises(PdfDownloadError, match="content-type"):
            download_pdf("https://example.com/a.pdf", session=session)

    def test_accepts_octet_stream(self):
        session = MagicMock()
        session.get.return_value = _mock_response(content_type="application/octet-stream", body=b"PDF body")
        assert download_pdf("https://example.com/a.pdf", session=session) == b"PDF body"

    def test_content_length_over_cap_raises(self):
        session = MagicMock()
        session.get.return_value = _mock_response(
            content_length=str(100 * 1024 * 1024),  # 100MB, over the 50MB cap
        )
        with pytest.raises(PdfTooLargeError, match="Content-Length"):
            download_pdf("https://example.com/huge.pdf", session=session)

    def test_streamed_body_over_cap_raises(self):
        """Content-Length header is optional — the size cap must still trip
        when the stream itself exceeds the limit."""
        session = MagicMock()
        resp = _mock_response()
        resp.iter_content.return_value = iter([b"x" * (60 * 1024 * 1024)])
        session.get.return_value = resp
        with pytest.raises(PdfTooLargeError, match="streamed body"):
            download_pdf("https://example.com/huge.pdf", session=session)

    def test_request_exception_raises_download_error(self):
        session = MagicMock()
        session.get.side_effect = requests.ConnectionError("connection refused")
        with pytest.raises(PdfDownloadError, match="fetch error"):
            download_pdf("https://example.com/a.pdf", session=session)

    def test_all_accepted_content_types_include_pdf(self):
        assert "application/pdf" in _ACCEPTED_CONTENT_TYPES


# ---------------------------------------------------------------------------
# audit_text_key — deterministic key format that downstream routes depend on
# ---------------------------------------------------------------------------


def test_audit_text_key_is_deterministic():
    assert audit_text_key(1) == "audits/text/1.txt"
    assert audit_text_key(999999) == "audits/text/999999.txt"
