"""Tests for the audit-PDF text extraction service + worker.

Covers:
    - download_pdf: size caps, content-type rejection, HTTP errors
    - extract_text_from_pdf: real pypdf parse, empty/malformed inputs
    - process_audit_report: happy path, skipped (short text), failure paths
    - AuditTextExtractionWorker: claim + persist outcome + stale recovery
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.audits.text_extraction import (
    ExtractionOutcome,
    PdfDownloadError,
    PdfParseError,
    PdfTooLargeError,
    StorageWriteError,
    _ACCEPTED_CONTENT_TYPES,
    audit_text_key,
    download_pdf,
    extract_text_from_pdf,
    process_audit_report,
    store_audit_text,
)


# ---------------------------------------------------------------------------
# Minimal PDF fixture generator
# ---------------------------------------------------------------------------


def _minimal_pdf_with_text(text: str) -> bytes:
    """Build a tiny valid PDF containing a single page with the given text.

    Just enough structure for pypdf to round-trip the text through
    ``extract_text`` — one Catalog, one Pages, one Page, one content-stream,
    one Type1 Helvetica font. Keeps test fixtures pure-Python without
    pulling in reportlab / fpdf2 as dev deps.
    """
    # Escape PDF-special chars in the literal string.
    escaped = (
        text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    )
    content_stream = (
        f"BT\n/F1 12 Tf\n50 750 Td\n({escaped}) Tj\nET\n".encode("ascii")
    )

    objects: list[bytes] = [
        b"<</Type/Catalog/Pages 2 0 R>>",
        b"<</Type/Pages/Count 1/Kids[3 0 R]>>",
        b"<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R"
        b"/Resources<</Font<</F1 5 0 R>>>>>>",
        (
            f"<</Length {len(content_stream)}>>\nstream\n".encode("ascii")
            + content_stream
            + b"endstream"
        ),
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
    buf += (
        b"trailer\n<</Size "
        + str(len(objects) + 1).encode("ascii")
        + b"/Root 1 0 R>>\n"
    )
    buf += b"startxref\n" + str(xref_start).encode("ascii") + b"\n%%EOF\n"
    return bytes(buf)


# ---------------------------------------------------------------------------
# extract_text_from_pdf
# ---------------------------------------------------------------------------


class TestExtractTextFromPdf:
    def test_roundtrips_simple_ascii(self):
        body = _minimal_pdf_with_text(
            "Audit scope covers Pool.sol and Vault.sol."
        )
        text = extract_text_from_pdf(body)
        assert "Pool.sol" in text
        assert "Vault.sol" in text
        # Page markers should be present so downstream scope extraction
        # can recover page boundaries.
        assert "--- page 1 ---" in text

    def test_garbage_body_raises_parse_error(self):
        with pytest.raises(PdfParseError):
            extract_text_from_pdf(b"not a pdf at all")

    def test_empty_body_raises_parse_error(self):
        with pytest.raises(PdfParseError):
            extract_text_from_pdf(b"")


# ---------------------------------------------------------------------------
# download_pdf
# ---------------------------------------------------------------------------


def _mock_response(
    *,
    status_code: int = 200,
    content_type: str = "application/pdf",
    content_length: str | None = None,
    body: bytes = b"",
) -> MagicMock:
    """Build a mock response compatible with download_pdf's streaming path."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {"content-type": content_type}
    if content_length is not None:
        resp.headers["content-length"] = content_length
    # iter_content yields the body in one chunk for the happy path
    resp.iter_content.return_value = iter([body]) if body else iter([])
    resp.close = MagicMock()
    return resp


class TestDownloadPdf:
    def test_happy_path_returns_bytes(self):
        fake_session = MagicMock()
        fake_session.get.return_value = _mock_response(
            body=b"%PDF-1.4\n...",
        )
        result = download_pdf("https://example.com/a.pdf", session=fake_session)
        assert result == b"%PDF-1.4\n..."

    def test_non_200_raises_download_error(self):
        fake_session = MagicMock()
        fake_session.get.return_value = _mock_response(status_code=404)
        with pytest.raises(PdfDownloadError, match="HTTP 404"):
            download_pdf("https://example.com/missing.pdf", session=fake_session)

    def test_rejects_html_content_type(self):
        fake_session = MagicMock()
        fake_session.get.return_value = _mock_response(
            content_type="text/html",
        )
        with pytest.raises(PdfDownloadError, match="content-type"):
            download_pdf("https://example.com/a.pdf", session=fake_session)

    def test_accepts_octet_stream_content_type(self):
        fake_session = MagicMock()
        fake_session.get.return_value = _mock_response(
            content_type="application/octet-stream",
            body=b"PDF body",
        )
        result = download_pdf("https://example.com/a.pdf", session=fake_session)
        assert result == b"PDF body"

    def test_content_length_over_cap_raises(self):
        fake_session = MagicMock()
        fake_session.get.return_value = _mock_response(
            content_length=str(100 * 1024 * 1024),  # 100MB, over the 50MB cap
        )
        with pytest.raises(PdfTooLargeError, match="Content-Length"):
            download_pdf("https://example.com/huge.pdf", session=fake_session)

    def test_streamed_body_over_cap_raises(self):
        fake_session = MagicMock()
        # Body streams in chunks that exceed the cap cumulatively.
        big_chunk = b"x" * (60 * 1024 * 1024)
        resp = _mock_response()
        resp.iter_content.return_value = iter([big_chunk])
        fake_session.get.return_value = resp
        with pytest.raises(PdfTooLargeError, match="streamed body"):
            download_pdf("https://example.com/huge.pdf", session=fake_session)

    def test_request_exception_raises_download_error(self):
        fake_session = MagicMock()
        fake_session.get.side_effect = requests.ConnectionError("connection refused")
        with pytest.raises(PdfDownloadError, match="fetch error"):
            download_pdf("https://example.com/a.pdf", session=fake_session)

    def test_all_accepted_content_types_include_pdf(self):
        # Sanity: the PDF content type must be accepted.
        assert "application/pdf" in _ACCEPTED_CONTENT_TYPES


# ---------------------------------------------------------------------------
# store_audit_text
# ---------------------------------------------------------------------------


class TestStoreAuditText:
    def test_happy_path_returns_key_size_sha(self, monkeypatch):
        put_calls = []

        def fake_put(key, body, content_type, metadata=None):
            put_calls.append(
                {"key": key, "body": body, "content_type": content_type, "metadata": metadata}
            )

        fake_client = SimpleNamespace(put=fake_put)
        monkeypatch.setattr(
            "services.audits.text_extraction.get_storage_client",
            lambda: fake_client,
        )

        key, size, digest = store_audit_text(42, "hello world")

        assert key == "audits/text/42.txt"
        assert size == len(b"hello world")
        # SHA256("hello world")
        assert digest == (
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
        assert len(put_calls) == 1
        call = put_calls[0]
        assert call["key"] == "audits/text/42.txt"
        assert call["body"] == b"hello world"
        assert call["metadata"]["audit_report_id"] == "42"
        assert call["metadata"]["sha256"] == digest

    def test_no_storage_raises(self, monkeypatch):
        monkeypatch.setattr(
            "services.audits.text_extraction.get_storage_client",
            lambda: None,
        )
        with pytest.raises(StorageWriteError, match="not configured"):
            store_audit_text(1, "some text")

    def test_storage_failure_propagates(self, monkeypatch):
        from db.storage import StorageUnavailable

        def fake_put(*_a, **_kw):
            raise StorageUnavailable("tigris down")

        monkeypatch.setattr(
            "services.audits.text_extraction.get_storage_client",
            lambda: SimpleNamespace(put=fake_put),
        )
        with pytest.raises(StorageWriteError, match="tigris down"):
            store_audit_text(1, "some text")


# ---------------------------------------------------------------------------
# audit_text_key
# ---------------------------------------------------------------------------


def test_audit_text_key_is_deterministic():
    assert audit_text_key(1) == "audits/text/1.txt"
    assert audit_text_key(999999) == "audits/text/999999.txt"


# ---------------------------------------------------------------------------
# process_audit_report
# ---------------------------------------------------------------------------


class TestProcessAuditReport:
    def test_happy_path(self, monkeypatch):
        # Keep well above the 500-char min-useful-text threshold so
        # ``process_audit_report`` lands on the success branch rather than
        # the image-only skip path. Repeat simple ASCII to sidestep any
        # pypdf ligature / encoding quirks.
        pdf = _minimal_pdf_with_text(
            "Audits covering Pool.sol Vault.sol Strategy.sol Registry.sol. " * 15
        )
        monkeypatch.setattr(
            "services.audits.text_extraction.download_pdf",
            lambda url, session=None: pdf,
        )
        monkeypatch.setattr(
            "services.audits.text_extraction.store_audit_text",
            lambda aid, text: (audit_text_key(aid), len(text), "a" * 64),
        )

        outcome = process_audit_report(
            audit_report_id=7, url="https://example.com/a.pdf"
        )
        assert outcome.status == "success"
        assert outcome.storage_key == "audits/text/7.txt"
        assert outcome.text_size_bytes is not None and outcome.text_size_bytes > 0
        assert outcome.text_sha256 == "a" * 64

    def test_too_short_text_is_skipped(self, monkeypatch):
        pdf = _minimal_pdf_with_text("tiny")
        monkeypatch.setattr(
            "services.audits.text_extraction.download_pdf",
            lambda url, session=None: pdf,
        )
        # store_audit_text should NOT be called when we skip
        store_called = {"n": 0}

        def spy_store(aid, text):
            store_called["n"] += 1
            return "x", 0, ""

        monkeypatch.setattr(
            "services.audits.text_extraction.store_audit_text", spy_store
        )

        outcome = process_audit_report(audit_report_id=1, url="https://x/a.pdf")
        assert outcome.status == "skipped"
        assert outcome.error and "image-only" in outcome.error
        assert store_called["n"] == 0

    def test_download_error_is_failed(self, monkeypatch):
        monkeypatch.setattr(
            "services.audits.text_extraction.download_pdf",
            lambda url, session=None: (_ for _ in ()).throw(
                PdfDownloadError("HTTP 503")
            ),
        )
        outcome = process_audit_report(audit_report_id=1, url="https://x/a.pdf")
        assert outcome.status == "failed"
        assert "HTTP 503" in (outcome.error or "")

    def test_too_large_is_skipped(self, monkeypatch):
        monkeypatch.setattr(
            "services.audits.text_extraction.download_pdf",
            lambda url, session=None: (_ for _ in ()).throw(
                PdfTooLargeError("streamed body exceeded cap")
            ),
        )
        outcome = process_audit_report(audit_report_id=1, url="https://x/a.pdf")
        assert outcome.status == "skipped"
        assert "too large" in (outcome.error or "")

    def test_parse_error_is_failed(self, monkeypatch):
        monkeypatch.setattr(
            "services.audits.text_extraction.download_pdf",
            lambda url, session=None: b"not a pdf",
        )
        outcome = process_audit_report(audit_report_id=1, url="https://x/a.pdf")
        assert outcome.status == "failed"
        assert "parse" in (outcome.error or "")

    def test_empty_url_is_failed(self):
        outcome = process_audit_report(audit_report_id=1, url="")
        assert outcome.status == "failed"
        assert "no URL" in (outcome.error or "")


# ---------------------------------------------------------------------------
# AuditTextExtractionWorker
# ---------------------------------------------------------------------------


class TestAuditTextExtractionWorker:
    def _make_worker(self):
        # Avoid signal handler registration during tests by constructing
        # outside the normal __init__ path.
        with patch("signal.signal"):
            from workers.audit_text_extraction import AuditTextExtractionWorker
            return AuditTextExtractionWorker()

    def test_host_semaphore_is_per_host(self):
        worker = self._make_worker()
        a = worker._host_semaphore("https://cdn.example.com/x.pdf")
        b = worker._host_semaphore("https://cdn.example.com/y.pdf")
        c = worker._host_semaphore("https://raw.githubusercontent.com/a/b.pdf")
        assert a is b  # same host → same semaphore
        assert a is not c  # different host → different semaphore

    def test_persist_outcome_success_populates_all_fields(self, monkeypatch):
        worker = self._make_worker()

        fake_audit = SimpleNamespace(
            id=42,
            text_extraction_status="processing",
            text_extraction_error=None,
            text_extraction_worker="old-worker",
            text_storage_key=None,
            text_size_bytes=None,
            text_sha256=None,
            text_extracted_at=None,
        )
        session = MagicMock()
        session.get.return_value = fake_audit
        monkeypatch.setattr(
            "workers.audit_text_extraction.SessionLocal", lambda: session
        )

        outcome = ExtractionOutcome(
            status="success",
            storage_key="audits/text/42.txt",
            text_size_bytes=1234,
            text_sha256="abc" * 10 + "a" * 34,
        )
        worker._persist_outcome(42, outcome)

        assert fake_audit.text_extraction_status == "success"
        assert fake_audit.text_storage_key == "audits/text/42.txt"
        assert fake_audit.text_size_bytes == 1234
        assert fake_audit.text_extracted_at is not None
        assert fake_audit.text_extraction_worker is None
        session.commit.assert_called_once()

    def test_persist_outcome_failed_skips_success_fields(self, monkeypatch):
        worker = self._make_worker()

        fake_audit = SimpleNamespace(
            id=42,
            text_extraction_status="processing",
            text_extraction_error=None,
            text_extraction_worker="old-worker",
            text_storage_key=None,
            text_size_bytes=None,
            text_sha256=None,
            text_extracted_at=None,
        )
        session = MagicMock()
        session.get.return_value = fake_audit
        monkeypatch.setattr(
            "workers.audit_text_extraction.SessionLocal", lambda: session
        )

        outcome = ExtractionOutcome(
            status="failed", error="HTTP 404",
        )
        worker._persist_outcome(42, outcome)

        assert fake_audit.text_extraction_status == "failed"
        assert fake_audit.text_extraction_error == "HTTP 404"
        # Success fields must stay unset.
        assert fake_audit.text_storage_key is None
        assert fake_audit.text_extracted_at is None

    def test_persist_outcome_for_missing_row_logs_and_returns(self, monkeypatch):
        worker = self._make_worker()
        session = MagicMock()
        session.get.return_value = None  # Row was deleted
        monkeypatch.setattr(
            "workers.audit_text_extraction.SessionLocal", lambda: session
        )
        # Should not raise
        worker._persist_outcome(999, ExtractionOutcome(status="success", storage_key="x"))
        session.commit.assert_not_called()

    def test_process_row_uses_pdf_url_preferring_over_url(self, monkeypatch):
        worker = self._make_worker()

        captured = {}

        def fake_process(audit_report_id, url, session=None):
            captured["audit_id"] = audit_report_id
            captured["url"] = url
            return ExtractionOutcome(status="success", storage_key="x")

        monkeypatch.setattr(
            "workers.audit_text_extraction.process_audit_report",
            fake_process,
        )

        fake_audit = SimpleNamespace(
            id=7,
            pdf_url="https://cdn.example.com/real.pdf",
            url="https://github.com/org/repo/blob/master/audits/x.pdf",
        )
        audit_id, outcome = worker._process_row(fake_audit)

        assert audit_id == 7
        assert outcome.status == "success"
        assert captured["audit_id"] == 7
        assert captured["url"] == "https://cdn.example.com/real.pdf"

    def test_process_row_falls_back_to_url_when_no_pdf_url(self, monkeypatch):
        worker = self._make_worker()

        captured = {}

        def fake_process(audit_report_id, url, session=None):
            captured["url"] = url
            return ExtractionOutcome(status="success", storage_key="x")

        monkeypatch.setattr(
            "workers.audit_text_extraction.process_audit_report",
            fake_process,
        )
        fake_audit = SimpleNamespace(
            id=7, pdf_url=None, url="https://example.com/a.pdf"
        )
        worker._process_row(fake_audit)
        assert captured["url"] == "https://example.com/a.pdf"

    def test_process_row_missing_both_urls_returns_failed(self):
        worker = self._make_worker()
        fake_audit = SimpleNamespace(id=7, pdf_url=None, url=None)
        audit_id, outcome = worker._process_row(fake_audit)
        assert audit_id == 7
        assert outcome.status == "failed"
        assert "no URL" in (outcome.error or "")
