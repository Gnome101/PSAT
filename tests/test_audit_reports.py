"""Tests for audit report discovery pipeline.

Covers:
- merge_audit_reports: append-only URL-keyed dedup, richness selection
- classify_search_results: LLM classification parsing, filtering
- extract_report_details: LLM extraction parsing, field normalization
- search_audit_reports: full orchestrator with mocked Tavily + LLM
- _sync_audit_reports_to_db: relational table upsert via discovery worker
- JSON parsing helpers: markdown fences, extra text, garbage input
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery.audit_reports import (
    merge_audit_reports,
    search_audit_reports,
)
from services.discovery.audit_reports_llm import (
    _parse_json_array,
    _parse_json_object,
    classify_search_results,
    extract_report_details,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _report(
    url: str = "https://example.com/audit",
    auditor: str = "OpenZeppelin",
    title: str = "Protocol Audit",
    date: str | None = "2023-06-15",
    pdf_url: str | None = None,
    confidence: float = 0.9,
) -> dict:
    return {
        "url": url,
        "pdf_url": pdf_url,
        "auditor": auditor,
        "title": title,
        "date": date,
        "source_url": url,
        "confidence": confidence,
        "discovered_at": "2026-01-01T00:00:00+00:00",
    }


def _tavily_result(url: str, title: str = "Audit Result", snippet: str = "some content") -> dict:
    return {"url": url, "title": title, "content": snippet}


# ============================================================================
# 1. JSON parsing helpers
# ============================================================================


class TestJsonParsing:
    def test_parse_array_clean(self):
        assert _parse_json_array('[{"a": 1}]') == [{"a": 1}]

    def test_parse_array_with_markdown_fences(self):
        text = '```json\n[{"a": 1}]\n```'
        assert _parse_json_array(text) == [{"a": 1}]

    def test_parse_array_with_surrounding_text(self):
        text = 'Here is the result: [{"a": 1}] Done.'
        assert _parse_json_array(text) == [{"a": 1}]

    def test_parse_array_garbage_returns_none(self):
        assert _parse_json_array("not json at all") is None

    def test_parse_object_clean(self):
        assert _parse_json_object('{"a": 1}') == {"a": 1}

    def test_parse_object_with_markdown_fences(self):
        text = '```json\n{"a": 1}\n```'
        assert _parse_json_object(text) == {"a": 1}

    def test_parse_object_with_surrounding_text(self):
        text = 'Here is the result: {"a": 1} Done.'
        assert _parse_json_object(text) == {"a": 1}

    def test_parse_object_garbage_returns_none(self):
        assert _parse_json_object("not json") is None

    def test_parse_array_empty(self):
        assert _parse_json_array("[]") == []

    def test_parse_object_empty(self):
        assert _parse_json_object("{}") == {}


# ============================================================================
# 2. merge_audit_reports
# ============================================================================


class TestMergeAuditReports:
    def test_append_only_keeps_prev_only_reports(self):
        """Reports only in prev must survive the merge."""
        prev = {"company": "Aave", "reports": [_report(url="https://a.com/old")]}
        new = {"company": "Aave", "reports": [_report(url="https://b.com/new")]}
        merged = merge_audit_reports(prev, new)
        urls = {r["url"] for r in merged["reports"]}
        assert "https://a.com/old" in urls
        assert "https://b.com/new" in urls
        assert len(merged["reports"]) == 2

    def test_richer_entry_wins_on_overlap(self):
        """When both have the same URL, keep the entry with more non-null fields."""
        sparse = _report(url="https://a.com/report", pdf_url=None, date=None)
        rich = _report(
            url="https://a.com/report",
            pdf_url="https://a.com/report.pdf",
            date="2023-06-15",
        )
        # New is richer
        merged = merge_audit_reports(
            {"company": "X", "reports": [sparse]},
            {"company": "X", "reports": [rich]},
        )
        assert len(merged["reports"]) == 1
        assert merged["reports"][0]["pdf_url"] == "https://a.com/report.pdf"

    def test_prev_richer_beats_new(self):
        """When prev is richer than new, keep prev."""
        rich = _report(
            url="https://a.com/report",
            pdf_url="https://a.com/report.pdf",
            date="2023-01-01",
        )
        sparse = _report(url="https://a.com/report", pdf_url=None, date=None)
        merged = merge_audit_reports(
            {"company": "X", "reports": [rich]},
            {"company": "X", "reports": [sparse]},
        )
        assert merged["reports"][0]["pdf_url"] == "https://a.com/report.pdf"
        assert merged["reports"][0]["date"] == "2023-01-01"

    def test_empty_prev_returns_new(self):
        new = {"company": "X", "reports": [_report()]}
        merged = merge_audit_reports({"company": "X", "reports": []}, new)
        assert len(merged["reports"]) == 1

    def test_empty_new_keeps_prev(self):
        prev = {"company": "X", "reports": [_report()]}
        merged = merge_audit_reports(prev, {"company": "X", "reports": []})
        assert len(merged["reports"]) == 1

    def test_both_empty(self):
        merged = merge_audit_reports(
            {"company": "X", "reports": []},
            {"company": "X", "reports": []},
        )
        assert merged["reports"] == []

    def test_url_normalization_dedup(self):
        """Trailing slashes and case differences should be treated as the same URL."""
        r1 = _report(url="https://Example.com/Audit/")
        r2 = _report(url="https://example.com/Audit", title="Updated Title")
        merged = merge_audit_reports(
            {"company": "X", "reports": [r1]},
            {"company": "X", "reports": [r2]},
        )
        assert len(merged["reports"]) == 1

    def test_sorted_by_date_descending(self):
        old = _report(url="https://a.com/old", date="2022-01-01")
        new = _report(url="https://b.com/new", date="2024-06-01")
        merged = merge_audit_reports(
            {"company": "X", "reports": []},
            {"company": "X", "reports": [old, new]},
        )
        assert merged["reports"][0]["date"] == "2024-06-01"


# ============================================================================
# 3. classify_search_results (LLM Stage 1)
# ============================================================================


class TestClassifySearchResults:
    def test_classifies_confirmed_audits(self, monkeypatch):
        """LLM returns JSON classifying results; confirmed audits are returned."""
        results = [
            _tavily_result("https://a.com/audit", "Aave Audit Report"),
            _tavily_result("https://b.com/blog", "Blog about security"),
        ]
        llm_response = json.dumps([
            {"url": "https://a.com/audit", "is_audit": True, "auditor": "Trail of Bits", "confidence": 0.95},
            {"url": "https://b.com/blog", "is_audit": False, "auditor": None, "confidence": 0.1},
        ])
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        confirmed = classify_search_results(results, "Aave")
        assert len(confirmed) == 1
        assert confirmed[0]["url"] == "https://a.com/audit"
        assert confirmed[0]["auditor"] == "Trail of Bits"

    def test_low_confidence_filtered(self, monkeypatch):
        """Results with confidence < 0.5 are filtered out."""
        llm_response = json.dumps([
            {"url": "https://a.com/maybe", "is_audit": True, "auditor": "Unknown", "confidence": 0.3},
        ])
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        confirmed = classify_search_results([_tavily_result("https://a.com/maybe")], "Test")
        assert len(confirmed) == 0

    def test_llm_failure_returns_empty(self, monkeypatch):
        """LLM call failure returns empty list instead of crashing."""
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("LLM down")),
        )
        confirmed = classify_search_results([_tavily_result("https://a.com")], "Test")
        assert confirmed == []

    def test_malformed_llm_response(self, monkeypatch):
        """Unparseable LLM response returns empty list."""
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: "not json at all")
        confirmed = classify_search_results([_tavily_result("https://a.com")], "Test")
        assert confirmed == []

    def test_empty_results(self):
        confirmed = classify_search_results([], "Test")
        assert confirmed == []


# ============================================================================
# 4. extract_report_details (LLM Stage 2)
# ============================================================================


class TestExtractReportDetails:
    def test_successful_extraction(self, monkeypatch):
        """LLM returns reports array with identifying metadata + linked URLs."""
        llm_response = json.dumps({
            "reports": [{
                "auditor": "OpenZeppelin",
                "title": "Aave V3 Security Review",
                "date": "2023-06-15",
                "pdf_url": "https://a.com/report.pdf",
            }],
            "linked_urls": ["https://other.com/another-audit"],
        })
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        result = extract_report_details("https://a.com/audit", "Page text here", "Aave")
        assert result is not None
        assert len(result["reports"]) == 1
        report = result["reports"][0]
        assert report["auditor"] == "OpenZeppelin"
        assert report["title"] == "Aave V3 Security Review"
        assert report["date"] == "2023-06-15"
        assert report["pdf_url"] == "https://a.com/report.pdf"
        assert result["linked_urls"] == ["https://other.com/another-audit"]

    def test_multiple_reports_from_listing_page(self, monkeypatch):
        """A listing page can return multiple audit reports."""
        llm_response = json.dumps({
            "reports": [
                {"auditor": "OZ", "title": "Audit 1", "date": "2023-01-01"},
                {"auditor": "ToB", "title": "Audit 2", "date": "2022-06-01"},
            ],
            "linked_urls": ["https://example.com/audit1.pdf", "https://example.com/audit2.pdf"],
        })
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        result = extract_report_details("https://docs.example.com/security", "Page text", "Test")
        assert result is not None
        assert len(result["reports"]) == 2
        assert len(result["linked_urls"]) == 2

    def test_backwards_compat_flat_fields(self, monkeypatch):
        """If LLM returns flat fields (no reports array), wrap as single report."""
        llm_response = json.dumps({
            "auditor": "OZ",
            "title": "Single Audit",
            "date": "2023-01-01",
            "pdf_url": None,
        })
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        result = extract_report_details("https://a.com", "text", "Test")
        assert result is not None
        assert len(result["reports"]) == 1
        assert result["reports"][0]["auditor"] == "OZ"

    def test_reports_with_missing_required_fields_filtered(self, monkeypatch):
        """Reports missing auditor or title are dropped."""
        llm_response = json.dumps({
            "reports": [
                {"auditor": "", "title": "No Auditor"},
                {"auditor": "OZ", "title": ""},
                {"auditor": "OZ", "title": "Valid Report"},
            ],
            "linked_urls": [],
        })
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        result = extract_report_details("https://a.com", "text", "Test")
        assert result is not None
        assert len(result["reports"]) == 1
        assert result["reports"][0]["title"] == "Valid Report"

    def test_llm_failure_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("down")),
        )
        result = extract_report_details("https://a.com", "text", "Test")
        assert result is None

    def test_linked_urls_deduped(self, monkeypatch):
        """Duplicate linked URLs are collapsed."""
        llm_response = json.dumps({
            "reports": [{"auditor": "OZ", "title": "Audit"}],
            "linked_urls": ["https://a.com/report.pdf", "https://a.com/report.pdf", "https://b.com/other"],
        })
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", lambda *a, **kw: llm_response)

        result = extract_report_details("https://a.com", "text", "Test")
        assert result is not None
        assert len(result["linked_urls"]) == 2


# ============================================================================
# 5. search_audit_reports (full orchestrator)
# ============================================================================


@pytest.fixture(autouse=True)
def _mock_external_seeds(monkeypatch):
    """The orchestrator's Stage 0 (Solodit), Stage 3.5 (portfolio crawl)
    and Stage 3.6 (allied-infra crawl) all make real network calls. Tests
    should not hit the network — stub them to return empty by default;
    tests that need to verify those paths override the mocks locally."""
    monkeypatch.setattr(
        "services.discovery.audit_reports._solodit.search",
        lambda *a, **kw: [],
    )
    monkeypatch.setattr(
        "services.discovery.audit_reports._list_repo_root_for_company",
        lambda *a, **kw: None,
    )


class TestSearchAuditReports:
    def test_full_pipeline_with_mocked_externals(self, monkeypatch):
        """End-to-end orchestrator test with mocked Tavily and LLM."""
        tavily_calls = []

        def fake_tavily_search(query, max_results, queries_used, max_queries, errors, debug=False):
            tavily_calls.append(query)
            if len(tavily_calls) == 1:
                return [
                    _tavily_result("https://blog.openzeppelin.com/aave-audit", "Aave Audit - OZ"),
                    _tavily_result("https://news.com/aave", "Aave news article"),
                ]
            return [
                _tavily_result("https://github.com/aave/audits/tob.pdf", "Trail of Bits Aave Audit"),
            ]

        monkeypatch.setattr("services.discovery.audit_reports._tavily_search", fake_tavily_search)
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: '"Aave" audit Trail of Bits OpenZeppelin',
        )

        # Mock LLM: call 1 = classification, call 2 = extraction
        classification_response = json.dumps([
            {"url": "https://blog.openzeppelin.com/aave-audit", "is_audit": True, "type": "report", "auditor": "OpenZeppelin", "confidence": 0.95},
            {"url": "https://news.com/aave", "is_audit": False, "type": None, "auditor": None, "confidence": 0.1},
            {"url": "https://github.com/aave/audits/tob.pdf", "is_audit": True, "type": "pdf", "auditor": "Trail of Bits", "confidence": 0.9},
        ])

        extraction_response = json.dumps({
            "reports": [{
                "auditor": "OpenZeppelin",
                "title": "Aave V3 Security Audit",
                "date": "2023-06-15",
                "pdf_url": "https://blog.openzeppelin.com/aave-audit.pdf",
            }],
            "linked_urls": [],
        })

        call_count = {"n": 0}

        def fake_llm_chat(messages, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return classification_response
            return extraction_response

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)

        monkeypatch.setattr(
            "services.discovery.audit_reports._fetch_html_page",
            lambda url, debug=False: (
                "<html><body>"
                "OpenZeppelin conducted a comprehensive security audit of the Aave V3 protocol. "
                "The audit covered core lending pool contracts including AavePool and PoolConfigurator. "
                "Two high severity and five medium severity findings were identified during the review."
                "</body></html>"
            ),
        )

        result = search_audit_reports("Aave")

        assert result["company"] == "Aave"
        assert len(result["reports"]) == 2  # OZ extracted + ToB PDF fallback
        assert len(tavily_calls) == 2

        oz = next(r for r in result["reports"] if r["auditor"] == "OpenZeppelin")
        assert oz["title"] == "Aave V3 Security Audit"
        assert oz["date"] == "2023-06-15"

        tob = next(r for r in result["reports"] if r["auditor"] == "Trail of Bits")
        assert tob["pdf_url"] == "https://github.com/aave/audits/tob.pdf"

    def test_link_following_discovers_additional_reports(self, monkeypatch):
        """When Stage 2 extraction returns linked_urls, Stage 3 follows them."""
        tavily_calls = []

        def fake_tavily_search(query, max_results, queries_used, max_queries, errors, debug=False):
            tavily_calls.append(query)
            return [_tavily_result("https://docs.example.com/security", "Security Audits Page")]

        monkeypatch.setattr("services.discovery.audit_reports._tavily_search", fake_tavily_search)
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: "TestProtocol audit security review",
        )

        classification_response = json.dumps([
            {"url": "https://docs.example.com/security", "is_audit": True, "type": "listing", "auditor": None, "confidence": 0.9},
        ])

        # Stage 2: listing page returns 1 report + a link to another
        listing_extraction = json.dumps({
            "reports": [{
                "auditor": "Firm A",
                "title": "V1 Audit",
                "date": "2022-01-01",
            }],
            "linked_urls": ["https://firma.com/v2-audit"],
        })

        # Stage 3: followed link returns another report
        linked_extraction = json.dumps({
            "reports": [{
                "auditor": "Firm A",
                "title": "V2 Audit",
                "date": "2023-06-01",
            }],
            "linked_urls": [],
        })

        call_count = {"n": 0}

        def fake_llm_chat(messages, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return classification_response
            if call_count["n"] == 2:
                return listing_extraction
            return linked_extraction

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr(
            "services.discovery.audit_reports._fetch_html_page",
            lambda url, debug=False: "<html><body>" + "Audit content. " * 20 + "</body></html>",
        )

        result = search_audit_reports("TestProtocol")

        assert len(result["reports"]) == 2
        titles = {r["title"] for r in result["reports"]}
        assert "V1 Audit" in titles
        assert "V2 Audit" in titles
        # The V2 audit was found via link following
        v2 = next(r for r in result["reports"] if r["title"] == "V2 Audit")
        assert v2["date"] == "2023-06-01"

    def test_no_results_returns_empty(self, monkeypatch):
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: "fallback query",
        )

        result = search_audit_reports("Unknown Protocol")
        assert result["reports"] == []
        assert "No search results found" in result["notes"]

    def test_no_audits_classified(self, monkeypatch):
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result("https://blog.com/post", "Some blog post")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: "fallback query",
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: json.dumps([
                {"url": "https://blog.com/post", "is_audit": False, "auditor": None, "confidence": 0.1},
            ]),
        )

        result = search_audit_reports("Test")
        assert result["reports"] == []
        assert "No results classified as audit reports" in result["notes"]

    def test_page_fetch_failure_records_fallback(self, monkeypatch):
        """When page fetch fails, a fallback report is created from Stage 1 metadata."""
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result("https://a.com/audit", "Aave Audit by OZ")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: "fallback query",
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: json.dumps([
                {"url": "https://a.com/audit", "is_audit": True, "type": "report", "auditor": "OZ", "confidence": 0.8},
            ]),
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports._fetch_html_page",
            lambda url, debug=False: None,  # Fetch fails
        )

        result = search_audit_reports("Aave")
        # Fallback entry created from classification metadata
        assert len(result["reports"]) == 1
        assert result["reports"][0]["auditor"] == "OZ"
        assert result["reports"][0]["title"] == "Aave Audit by OZ"  # From Tavily title

    def test_empty_company_raises(self):
        with pytest.raises(ValueError, match="company must not be empty"):
            search_audit_reports("")

    def test_github_tree_per_file_url_and_llm_metadata(self, monkeypatch):
        """End-to-end: a GitHub /tree/ URL gets resolved through the GitHub
        contents API, each file goes through the batched filename-LLM call,
        and the resulting reports keep distinct per-file URLs.

        Reproduces the bug observed in protocols/etherfi/audit_reports.json
        where (a) the single-shot LLM call truncated and dropped all
        per-file metadata and (b) multiple .md files in the same directory
        would have collided onto the tree URL during dedup.
        """
        tree_url = "https://github.com/etherfi-protocol/smart-contracts/tree/master/audits"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(tree_url, "EtherFi Audits")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        api_payload = [
            {"name": "2023.05.16 - Omniscia.pdf", "type": "file",
             "download_url": "https://raw.githubusercontent.com/etherfi-protocol/smart-contracts/master/audits/2023.05.16%20-%20Omniscia.pdf",
             "html_url": "https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2023.05.16%20-%20Omniscia.pdf"},
            {"name": "2023.12.20 - Hats Finance.md", "type": "file",
             "download_url": "https://raw.githubusercontent.com/etherfi-protocol/smart-contracts/master/audits/2023.12.20%20-%20Hats%20Finance.md",
             "html_url": "https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2023.12.20%20-%20Hats%20Finance.md"},
            {"name": "2024.06.25 - Halborn - EtherFi_L2_Governance.md", "type": "file",
             "download_url": "https://raw.githubusercontent.com/etherfi-protocol/smart-contracts/master/audits/2024.06.25%20-%20Halborn%20-%20EtherFi_L2_Governance.md",
             "html_url": "https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2024.06.25%20-%20Halborn%20-%20EtherFi_L2_Governance.md"},
        ]

        # Stage 1 LLM: tree page is an audit listing.
        # Filename LLM (called with the three filenames in one batch): returns
        # cleanly classified entries. The URL-collision and per-file URL
        # behavior we're testing is independent of the LLM working perfectly.
        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                return json.dumps([
                    {"filename": "2023.05.16 - Omniscia.pdf", "auditor": "Omniscia",
                     "date": "2023-05-16", "title": "Omniscia EtherFi audit"},
                    {"filename": "2023.12.20 - Hats Finance.md", "auditor": "Hats Finance",
                     "date": "2023-12-20", "title": "Hats Finance EtherFi audit"},
                    {"filename": "2024.06.25 - Halborn - EtherFi_L2_Governance.md",
                     "auditor": "Halborn", "date": "2024-06-25",
                     "title": "Halborn EtherFi L2 Governance audit"},
                ])
            return json.dumps([
                {"url": tree_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "EtherFi audits directory",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("utils.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        class FakeResp:
            def __init__(self, payload, status=200, content_type="application/json"):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": content_type}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)

            def json(self):
                return self._payload

            def close(self):
                pass

            def iter_content(self, chunk_size=64_000):
                yield self.text.encode()

        def fake_requests_get(url, **kwargs):
            # Auto-org-hop triggers on the etherfi-protocol tree URL and
            # queries the org's repo list. Return an empty list so the
            # auto-hop produces zero sibling reports, isolating the test
            # to the tree-URL extraction path it was written to cover.
            if url.startswith("https://api.github.com/orgs/"):
                return FakeResp([])
            if url.startswith("https://api.github.com/users/"):
                return FakeResp({"message": "Not Found"}, status=404)
            if "api.github.com" in url:
                return FakeResp(api_payload)
            return FakeResp("", status=404, content_type="text/plain")

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("EtherFi")
        reports = result["reports"]

        urls = {r["url"] for r in reports}
        assert len(urls) == 3, f"expected 3 distinct URLs, got {urls}"

        omniscia = next(r for r in reports if "Omniscia" in r["url"])
        assert omniscia["auditor"] == "Omniscia"
        assert omniscia["date"] == "2023-05-16"
        assert omniscia["pdf_url"] is not None and omniscia["pdf_url"].endswith(".pdf")

        hats = next(r for r in reports if "Hats" in r["url"])
        assert hats["auditor"] == "Hats Finance"
        assert hats["date"] == "2023-12-20"
        assert hats["pdf_url"] is None  # .md file
        # The .md file's URL is its own GitHub blob URL, not the tree URL.
        assert hats["url"].endswith("Hats%20Finance.md")

        halborn = next(r for r in reports if "Halborn" in r["url"])
        assert halborn["auditor"] == "Halborn"
        assert halborn["date"] == "2024-06-25"

    def test_mirror_dedup_keeps_numbered_siblings(self, monkeypatch):
        """Regression: ``V3.Prelude - 1`` and ``V3.Prelude - 2`` are two
        consecutive Certora audits delivered on the same day. The single-
        character ``1``/``2`` distinguishing tokens used to be filtered by
        the title-token tokenizer, which caused mirror dedup to collapse
        the pair."""
        a = "https://github.com/etherfi/audits/V3.Prelude%20-%201.pdf"
        b = "https://github.com/etherfi/audits/V3.Prelude%20-%202.pdf"
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [
                _tavily_result(a, "Certora EtherFi V3.Prelude 1"),
                _tavily_result(b, "Certora EtherFi V3.Prelude 2"),
            ],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *aa, **kw: json.dumps([
                {"url": a, "is_audit": True, "type": "pdf",
                 "auditor": "Certora", "title": "EtherFi V3.Prelude - 1",
                 "date": "2025-08-01", "confidence": 0.95},
                {"url": b, "is_audit": True, "type": "pdf",
                 "auditor": "Certora", "title": "EtherFi V3.Prelude - 2",
                 "date": "2025-08-01", "confidence": 0.95},
            ]),
        )
        result = search_audit_reports("EtherFi")
        urls = {r["url"] for r in result["reports"]}
        assert a in urls and b in urls
        assert len(result["reports"]) == 2

    def test_mirror_dedup_keeps_distinct_same_day_audits(self, monkeypatch):
        """Regression: Certora v2.49 and "Instant Withdrawal Merge into v2.49"
        are two distinct audits delivered on the same day. Earlier subset-
        token dedup wrongly collapsed them because {etherfi, v2.49} is a
        subset of {etherfi, instant, withdrawal, merge, into, v2.49}."""
        a = "https://github.com/etherfi/audits/2025.03.26%20-%20Certora%20-%20EtherFi%20-%20v2.49.pdf"
        b = "https://github.com/etherfi/audits/2025.03.26%20-%20Certora%20-%20EtherFi%20-%20Instant%20Withdrawal%20Merge%20into%20v2.49.pdf"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [
                _tavily_result(a, "Certora EtherFi v2.49"),
                _tavily_result(b, "Certora Instant Withdrawal Merge into v2.49"),
            ],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *aa, **kw: json.dumps([
                {"url": a, "is_audit": True, "type": "pdf",
                 "auditor": "Certora", "title": "EtherFi v2.49",
                 "date": "2025-03-26", "confidence": 0.95},
                {"url": b, "is_audit": True, "type": "pdf",
                 "auditor": "Certora",
                 "title": "EtherFi Instant Withdrawal Merge into v2.49",
                 "date": "2025-03-26", "confidence": 0.95},
            ]),
        )

        result = search_audit_reports("EtherFi")
        urls = {r["url"] for r in result["reports"]}
        assert a in urls and b in urls
        assert len(result["reports"]) == 2

    def test_mirror_dedup_collapses_unknown_with_named_same_date(self, monkeypatch):
        """An ``Unknown``-auditor PDF on the same date as a named audit
        (different mirror of the same report) should be dropped, while two
        distinct audits by the same auditor on the same day must survive."""
        gitbook_pdf = "https://gitbook.example/Omniscia_Audit_EtherFi.pdf"
        github_pdf = "https://raw.githubusercontent.com/etherfi/audits/2023.05.16%20-%20Omniscia.pdf"
        # Two distinct same-day Certora audits with different titles
        cert_a = "https://github.com/etherfi/audits/2025.03.26%20-%20Certora%20-%20EtherFi%20-%20v2.49.pdf"
        cert_b = "https://github.com/etherfi/audits/2025.03.26%20-%20Certora%20-%20EtherFi%20-%20Instant%20Withdrawal.pdf"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [
                _tavily_result(gitbook_pdf, "[PDF] EtherFi audit"),
                _tavily_result(github_pdf, "Omniscia EtherFi audit"),
                _tavily_result(cert_a, "Certora EtherFi v2.49 audit"),
                _tavily_result(cert_b, "Certora EtherFi Instant Withdrawal audit"),
            ],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: json.dumps([
                # Stage 1 classification: gitbook PDF gets Unknown auditor
                {"url": gitbook_pdf, "is_audit": True, "type": "pdf",
                 "auditor": None, "title": "EtherFi audit (PDF)",
                 "date": "2023-05-16", "confidence": 0.7},
                {"url": github_pdf, "is_audit": True, "type": "pdf",
                 "auditor": "Omniscia", "title": "Omniscia EtherFi audit",
                 "date": "2023-05-16", "confidence": 0.95},
                {"url": cert_a, "is_audit": True, "type": "pdf",
                 "auditor": "Certora", "title": "EtherFi v2.49",
                 "date": "2025-03-26", "confidence": 0.95},
                {"url": cert_b, "is_audit": True, "type": "pdf",
                 "auditor": "Certora", "title": "EtherFi Instant Withdrawal",
                 "date": "2025-03-26", "confidence": 0.95},
            ]),
        )

        result = search_audit_reports("EtherFi")
        urls = {r["url"] for r in result["reports"]}

        # Unknown gitbook PDF dropped — Omniscia kept.
        assert gitbook_pdf not in urls
        assert github_pdf in urls
        # Two Certora audits on the same day stay separate (different titles).
        assert cert_a in urls
        assert cert_b in urls
        # Final count: Omniscia + 2× Certora = 3 reports.
        assert len(result["reports"]) == 3

    def test_pdf_fallback_uses_llm_metadata(self, monkeypatch):
        """A PDF hit that takes the Tavily-fallback path should propagate the
        Stage-1 LLM's title/date/auditor onto the report — no in-orchestrator
        regex parsing required."""
        pdf_url = "https://example.com/audits/Omniscia_EtherFi.pdf"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(
                pdf_url,
                "[PDF] SMART CONTRACT AUDIT REPORT May 16, 2023 EtherFi ETH2.0",
                "Audit by Omniscia covering staking",
            )],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: json.dumps([
                {"url": pdf_url, "is_audit": True, "type": "pdf",
                 "auditor": "Omniscia", "title": "Omniscia EtherFi ETH2.0 Audit",
                 "date": "2023-05-16", "confidence": 0.95},
            ]),
        )

        result = search_audit_reports("EtherFi")
        assert len(result["reports"]) == 1
        r = result["reports"][0]
        assert r["auditor"] == "Omniscia"
        assert r["date"] == "2023-05-16"
        assert r["title"] == "Omniscia EtherFi ETH2.0 Audit"
        assert r["pdf_url"] == pdf_url

    def test_org_enumeration_skips_library_fork_repos(self, monkeypatch):
        """Regression: ``morpho-org/openzeppelin-contracts`` is Morpho's
        vendored fork of the OpenZeppelin contracts library. Its ``audits/``
        folder holds OZ's own audit history (``Audit Report 2017-03.pdf``
        etc.), which isn't a Morpho audit. Auto-org-hop was adding them as
        6 spurious Morpho entries.
        """
        org_url = "https://github.com/morpho-org"
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(org_url, "morpho-org")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                import re as _re
                names = _re.findall(r"^- (.+)$", content, _re.MULTILINE)
                return json.dumps([{
                    "filename": n, "auditor": "Unknown",
                    "title": n.rsplit(".", 1)[0], "date": "2017-03",
                } for n in names])
            return json.dumps([
                {"url": org_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "morpho-org",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        class FakeResp:
            def __init__(self, payload, status=200):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": "application/json"}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)
            def json(self): return self._payload
            def close(self): pass
            def iter_content(self, chunk_size=64_000): yield self.text.encode()

        def fake_requests_get(url, **kwargs):
            if url.startswith("https://api.github.com/orgs/morpho-org/repos"):
                return FakeResp([
                    {"name": "openzeppelin-contracts", "default_branch": "master"},
                ])
            if url.startswith("https://api.github.com/users/morpho-org/repos"):
                return FakeResp({"message": "Not Found"}, status=404)
            if url == "https://api.github.com/repos/morpho-org/openzeppelin-contracts":
                return FakeResp({"default_branch": "master"})
            if "/git/trees/master?recursive=1" in url:
                return FakeResp({"tree": [{"path": "audits", "type": "tree"}]})
            if "/contents/audits" in url:
                return FakeResp([{
                    "name": "Audit Report 2017-03.pdf", "type": "file",
                    "download_url": "https://raw.githubusercontent.com/morpho-org/openzeppelin-contracts/master/audits/Audit%20Report%202017-03.pdf",
                    "html_url": "https://github.com/morpho-org/openzeppelin-contracts/blob/master/audits/Audit%20Report%202017-03.pdf",
                }])
            return FakeResp({}, status=404)

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("morpho")
        urls = [r["url"] for r in result["reports"]]
        assert not any("openzeppelin-contracts" in u for u in urls), (
            f"library fork 'openzeppelin-contracts' should be skipped; urls={urls}"
        )

    def test_vendored_submodule_audit_folders_skipped(self, monkeypatch):
        """Regression: ``etherfi-protocol/beHYPE`` vendors OZ contracts at
        ``lib/openzeppelin-contracts/`` with that library's own ``audits/``
        folder inside. The recursive-tree scan was surfacing both the
        top-level ``audits/`` (real BeHYPE audits) AND the nested
        ``lib/openzeppelin-contracts/audits/`` (spurious OZ library audits)
        because both folder names end in ``audits``. Nested paths under
        conventional vendored-dependency directories must be skipped.
        """
        tree_url = "https://github.com/etherfi-protocol/beHYPE"
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(tree_url, "EtherFi beHYPE")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                import re as _re
                names = _re.findall(r"^- (.+)$", content, _re.MULTILINE)
                return json.dumps([
                    {"filename": n,
                     "auditor": "Certora" if "behype" in n.lower() else "Unknown",
                     "title": n.rsplit(".", 1)[0], "date": None}
                    for n in names
                ])
            return json.dumps([
                {"url": tree_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "etherfi beHYPE",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        class FakeResp:
            def __init__(self, payload, status=200):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": "application/json"}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)
            def json(self): return self._payload
            def close(self): pass
            def iter_content(self, chunk_size=64_000): yield self.text.encode()

        def fake_requests_get(url, **kwargs):
            # Keep auto-org-hop a no-op for this test.
            if url.startswith("https://api.github.com/orgs/"):
                return FakeResp([])
            if url.startswith("https://api.github.com/users/"):
                return FakeResp({"message": "Not Found"}, status=404)
            # Repo metadata
            if url == "https://api.github.com/repos/etherfi-protocol/beHYPE":
                return FakeResp({"default_branch": "main"})
            # Recursive tree shows BOTH the real audits/ and the vendored one.
            if "/git/trees/main?recursive=1" in url:
                return FakeResp({"tree": [
                    {"path": "audits", "type": "tree"},
                    {"path": "lib/openzeppelin-contracts/audits", "type": "tree"},
                ]})
            if url.endswith("/contents/audits?ref=main"):
                return FakeResp([{
                    "name": "EtherFi BeHype Audit.pdf", "type": "file",
                    "download_url": "https://raw.githubusercontent.com/etherfi-protocol/beHYPE/main/audits/EtherFi%20BeHype%20Audit.pdf",
                    "html_url": "https://github.com/etherfi-protocol/beHYPE/blob/main/audits/EtherFi%20BeHype%20Audit.pdf",
                }])
            if "lib/openzeppelin-contracts/audits" in url:
                return FakeResp([{
                    "name": "Audit Report 2017-03.pdf", "type": "file",
                    "download_url": "https://raw.githubusercontent.com/etherfi-protocol/beHYPE/main/lib/openzeppelin-contracts/audits/Audit%20Report%202017-03.pdf",
                    "html_url": "https://github.com/etherfi-protocol/beHYPE/blob/main/lib/openzeppelin-contracts/audits/Audit%20Report%202017-03.pdf",
                }])
            return FakeResp({}, status=404)

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("etherfi")
        urls = [r["url"] for r in result["reports"]]
        joined = " ".join(urls)
        assert "EtherFi%20BeHype%20Audit" in joined, (
            f"real beHYPE audit missing; urls={urls}"
        )
        assert "lib/openzeppelin-contracts" not in joined and "lib%2Fopenzeppelin" not in joined, (
            f"vendored OZ audit folder should be skipped; urls={urls}"
        )

    def test_singular_audit_folder_is_discovered(self, monkeypatch):
        """Regression: EtherFi's ``cash-v3`` and ``weETH-cross-chain`` repos
        ship audits under ``audit/`` (singular), not ``audits/`` (plural).
        Before this fix ``_AUDIT_FOLDER_CANDIDATES`` only listed the plural
        form, so 14 reports across those two repos were silently skipped."""
        repo_url = "https://github.com/etherfi-protocol/cash-v3"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(repo_url, "EtherFi cash-v3 repo")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                return json.dumps([{
                    "filename": "EtherFi-Bundle_11.pdf",
                    "auditor": "Certora", "date": None,
                    "title": "EtherFi Bundle 11",
                }])
            return json.dumps([
                {"url": repo_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "etherfi-protocol/cash-v3",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        class FakeResp:
            def __init__(self, payload, status=200):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": "application/json"}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)
            def json(self): return self._payload
            def close(self): pass
            def iter_content(self, chunk_size=64_000): yield self.text.encode()

        def fake_requests_get(url, **kwargs):
            # Repo metadata
            if url == "https://api.github.com/repos/etherfi-protocol/cash-v3":
                return FakeResp({"default_branch": "master"})
            # Recursive tree — only has `audit/` (singular)
            if "/git/trees/master?recursive=1" in url:
                return FakeResp({"tree": [{"path": "audit", "type": "tree"}]})
            # Plural `audits/` — returns 404, must fall through
            if url.endswith("/contents/audits?ref=master") or "/contents/audits?" in url and "cash-v3" in url:
                return FakeResp({"message": "Not Found"}, status=404)
            # Singular `audit/` — the actual folder
            if "/contents/audit?" in url or url.endswith("/contents/audit"):
                return FakeResp([{
                    "name": "EtherFi-Bundle_11.pdf", "type": "file",
                    "download_url": "https://raw.githubusercontent.com/etherfi-protocol/cash-v3/master/audit/EtherFi-Bundle_11.pdf",
                    "html_url": "https://github.com/etherfi-protocol/cash-v3/blob/master/audit/EtherFi-Bundle_11.pdf",
                }])
            return FakeResp({}, status=404)

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("etherfi")
        urls = [r["url"] for r in result["reports"]]
        assert any("EtherFi-Bundle_11.pdf" in u for u in urls), (
            f"audit (singular) folder was not discovered; urls={urls}"
        )

    def test_repo_url_auto_hops_to_sibling_org_repos(self, monkeypatch):
        """Regression: Tavily commonly returns a single protocol repo URL
        like ``github.com/etherfi-protocol/smart-contracts``. The pipeline
        must also enumerate the org's other repos (``cash-contracts``,
        ``cash-v3``, etc.) when the owner name matches the company — those
        sibling repos hold ~14 EtherFi audits each. Before this fix the org
        fan-out only fired when Tavily returned a bare org URL."""
        repo_url = "https://github.com/etherfi-protocol/smart-contracts"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(repo_url, "etherfi-protocol/smart-contracts")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                import re as _re
                names = _re.findall(r"^- (.+)$", content, _re.MULTILINE)
                return json.dumps([
                    {"filename": n, "auditor": "Certora", "date": None,
                     "title": n.rsplit(".", 1)[0]}
                    for n in names
                ])
            return json.dumps([
                {"url": repo_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "etherfi-protocol/smart-contracts",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        class FakeResp:
            def __init__(self, payload, status=200):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": "application/json"}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)
            def json(self): return self._payload
            def close(self): pass
            def iter_content(self, chunk_size=64_000): yield self.text.encode()

        # Org listing (for etherfi-protocol) returns both smart-contracts and cash-v3
        org_repos = [
            {"name": "smart-contracts", "default_branch": "master"},
            {"name": "cash-v3", "default_branch": "master"},
        ]

        audits_by_repo = {
            "smart-contracts": [{
                "name": "2025.01.16 - Certora - EETH share inflation by burn shares.pdf",
                "type": "file",
                "download_url": "https://raw.githubusercontent.com/etherfi-protocol/smart-contracts/master/audits/2025.01.16%20-%20Certora%20-%20EETH%20share%20inflation%20by%20burn%20shares.pdf",
                "html_url": "https://github.com/etherfi-protocol/smart-contracts/blob/master/audits/2025.01.16%20-%20Certora%20-%20EETH%20share%20inflation%20by%20burn%20shares.pdf",
            }],
            "cash-v3": [{
                "name": "EtherFi-Bundle_11.pdf",
                "type": "file",
                "download_url": "https://raw.githubusercontent.com/etherfi-protocol/cash-v3/master/audit/EtherFi-Bundle_11.pdf",
                "html_url": "https://github.com/etherfi-protocol/cash-v3/blob/master/audit/EtherFi-Bundle_11.pdf",
            }],
        }

        def fake_requests_get(url, **kwargs):
            # Org enumeration
            if url.startswith("https://api.github.com/orgs/etherfi-protocol/repos"):
                return FakeResp(org_repos)
            if url.startswith("https://api.github.com/users/etherfi-protocol/repos"):
                return FakeResp({"message": "Not Found"}, status=404)
            # Repo metadata
            for repo in ("smart-contracts", "cash-v3"):
                if url == f"https://api.github.com/repos/etherfi-protocol/{repo}":
                    return FakeResp({"default_branch": "master"})
            # Recursive tree — smart-contracts has `audits/`, cash-v3 has `audit/`
            if "/git/trees/master?recursive=1" in url:
                if "/smart-contracts/" in url:
                    return FakeResp({"tree": [{"path": "audits", "type": "tree"}]})
                if "/cash-v3/" in url:
                    return FakeResp({"tree": [{"path": "audit", "type": "tree"}]})
            # Contents endpoints
            if "/contents/audits" in url and "/smart-contracts/" in url:
                return FakeResp(audits_by_repo["smart-contracts"])
            if "/contents/audit" in url and "/cash-v3/" in url and "/contents/audits" not in url:
                return FakeResp(audits_by_repo["cash-v3"])
            return FakeResp({}, status=404)

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("etherfi")
        urls = [r["url"] for r in result["reports"]]
        joined = " ".join(urls)
        assert "smart-contracts" in joined, (
            f"smart-contracts audit missing; urls={urls}"
        )
        assert "cash-v3" in joined, (
            f"cash-v3 sibling repo was not auto-enumerated; urls={urls}"
        )

    def test_mirror_dedup_collapses_cross_host_same_auditor_named_mirrors(self, monkeypatch):
        """Regression: EtherFi's Omniscia 2023-05-16 audit showed up twice in
        the pipeline output — once at
        ``Certora/etherfi-smart-contracts-fork/audits/2023.05.16 - Omniscia.pdf``
        and once on a gitbook CDN (``246895607-files.gitbook.io/...``).
        Both classifications correctly identified the auditor as ``Omniscia``
        with date ``2023-05-16``, but with different titles (``Omniscia Audit``
        vs ``EtherFi ETH2.0 Smart Contract Audit Report``). The existing
        title-token pass couldn't see them as mirrors because the non-generic
        tokens are disjoint (``{omniscia}`` vs ``{etherfi, eth2.0}``).
        Cross-host same-(auditor, date) pairs should collapse to one entry.
        """
        github_pdf = "https://raw.githubusercontent.com/etherfi/audits/2023.05.16%20-%20Omniscia.pdf"
        gitbook_pdf = "https://246895607-files.gitbook.io/spaces/G3Lk76lfvw9ecPIg0mK8/uploads/Omniscia_Audit_EtherFi.pdf"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [
                _tavily_result(github_pdf, "Omniscia EtherFi Audit"),
                _tavily_result(
                    gitbook_pdf,
                    "[PDF] EtherFi ETH2.0 Smart Contract Audit Report",
                ),
            ],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports_llm.llm.chat",
            lambda *a, **kw: json.dumps([
                {"url": github_pdf, "is_audit": True, "type": "pdf",
                 "auditor": "Omniscia", "title": "Omniscia Audit",
                 "date": "2023-05-16", "confidence": 0.95},
                {"url": gitbook_pdf, "is_audit": True, "type": "pdf",
                 "auditor": "Omniscia",
                 "title": "EtherFi ETH2.0 Smart Contract Audit Report",
                 "date": "2023-05-16", "confidence": 0.95},
            ]),
        )

        result = search_audit_reports("EtherFi")
        omniscia = [r for r in result["reports"] if r["auditor"] == "Omniscia"]
        assert len(omniscia) == 1, (
            f"cross-host mirrors of the same Omniscia 2023-05-16 audit "
            f"should collapse; got {len(omniscia)}: "
            f"{[r.get('url') for r in omniscia]}"
        )

    def test_mirror_dedup_keeps_same_folder_unknown_siblings(self, monkeypatch):
        """Regression: three audits delivered on the same day that all live
        side-by-side in ``morpho-org/vault-v2/audits/`` must all survive,
        even when the filename-LLM doesn't recognize one firm's name
        ("Blackthorn" isn't on the known-auditors list, so its entry comes
        back as auditor='Unknown'). Before this fix the Unknown entry was
        collapsed by pass-1 of ``_collapse_same_audit_mirrors`` — that pass
        treats same-date Unknown entries as mirrors of a named one, which
        is correct across hosts (gitbook PDF vs. github PDF) but wrong for
        sibling files in the same folder.
        """
        tree_url = "https://github.com/morpho-org/vault-v2/tree/main/audits"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(tree_url, "Morpho vault-v2 audits")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        api_payload = [
            {"name": "2025-09-15-chainsecurity.pdf", "type": "file",
             "download_url": "https://raw.githubusercontent.com/morpho-org/vault-v2/main/audits/2025-09-15-chainsecurity.pdf",
             "html_url": "https://github.com/morpho-org/vault-v2/blob/main/audits/2025-09-15-chainsecurity.pdf"},
            {"name": "2025-09-15-spearbit.pdf", "type": "file",
             "download_url": "https://raw.githubusercontent.com/morpho-org/vault-v2/main/audits/2025-09-15-spearbit.pdf",
             "html_url": "https://github.com/morpho-org/vault-v2/blob/main/audits/2025-09-15-spearbit.pdf"},
            {"name": "2025-09-15-blackthorn.pdf", "type": "file",
             "download_url": "https://raw.githubusercontent.com/morpho-org/vault-v2/main/audits/2025-09-15-blackthorn.pdf",
             "html_url": "https://github.com/morpho-org/vault-v2/blob/main/audits/2025-09-15-blackthorn.pdf"},
        ]

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                # Realistic filename-LLM response: Blackthorn is not in the
                # known-firms list, so the LLM returns Unknown for it.
                return json.dumps([
                    {"filename": "2025-09-15-chainsecurity.pdf",
                     "auditor": "ChainSecurity", "date": "2025-09-15",
                     "title": "ChainSecurity Audit"},
                    {"filename": "2025-09-15-spearbit.pdf",
                     "auditor": "Spearbit", "date": "2025-09-15",
                     "title": "Spearbit Audit"},
                    {"filename": "2025-09-15-blackthorn.pdf",
                     "auditor": None, "date": "2025-09-15",
                     "title": "2025-09-15-blackthorn"},
                ])
            return json.dumps([
                {"url": tree_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "Morpho vault-v2 audits",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        class FakeResp:
            def __init__(self, payload, status=200):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": "application/json"}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)

            def json(self):
                return self._payload

            def close(self):
                pass

            def iter_content(self, chunk_size=64_000):
                yield self.text.encode()

        def fake_requests_get(url, **kwargs):
            # Auto-org-hop short-circuit: return an empty repo list so the
            # sibling enumeration contributes nothing, isolating this test
            # to the folder-siblings logic under the single tree URL.
            if url.startswith("https://api.github.com/orgs/"):
                return FakeResp([])
            if url.startswith("https://api.github.com/users/"):
                return FakeResp({"message": "Not Found"}, status=404)
            if "api.github.com" in url:
                return FakeResp(api_payload)
            return FakeResp("", status=404)

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("morpho")
        urls = {r["url"] for r in result["reports"]}

        # Every file in the same folder is a distinct audit — all three should survive.
        assert len(result["reports"]) == 3, (
            f"expected 3 reports (one per folder sibling); got "
            f"{len(result['reports'])}: {urls}"
        )
        assert any("blackthorn" in u.lower() for u in urls), (
            f"Blackthorn audit missing; got {urls}"
        )
        assert any("chainsecurity" in u.lower() for u in urls)
        assert any("spearbit" in u.lower() for u in urls)

    def test_github_org_url_enumerates_repos_across_org(self, monkeypatch):
        """Regression: when Tavily hits just a GitHub org URL like
        ``github.com/morpho-org``, the pipeline should expand it via the
        GitHub API — enumerating every repo in the org and pulling audit
        folders from each. Before this fix the org URL fell through to
        plain HTML fetch of a React SPA and the LLM couldn't surface the
        sibling repos that hold the core Morpho Blue / Morpho Optimizers
        audits (verified against docs.morpho.org)."""
        org_url = "https://github.com/morpho-org"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(org_url, "morpho-org Organization")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            import re as _re
            content = messages[0]["content"]
            if "Below are file names" in content:
                # Per-filename batch: return one entry per `- filename` line.
                names = _re.findall(r"^- (.+)$", content, _re.MULTILINE)
                out = []
                for name in names:
                    base = name.rsplit(".", 1)[0]
                    # Collapse separators so real-world filenames like
                    # ``open-zeppelin`` still match ``openzeppelin``.
                    norm = _re.sub(r"[^a-z0-9]", "", name.lower())
                    if "trailofbits" in norm or "tob" in norm:
                        auditor = "Trail of Bits"
                    elif "zeppelin" in norm:
                        auditor = "OpenZeppelin"
                    elif "spearbit" in norm:
                        auditor = "Spearbit"
                    else:
                        auditor = "Unknown"
                    out.append({
                        "filename": name, "auditor": auditor,
                        "title": base, "date": "2024-01-01",
                    })
                return json.dumps(out)
            # Stage 1 classification for the org URL.
            return json.dumps([
                {"url": org_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "morpho-org",
                 "date": None, "confidence": 0.95},
            ])

        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        # The org's HTML page never helps — this forces the "before" code
        # path to register just a fallback entry (no sibling repos found).
        monkeypatch.setattr(
            "services.discovery.audit_reports._fetch_html_page",
            lambda url, debug=False: None,
        )

        class FakeResp:
            def __init__(self, payload, status=200):
                self._payload = payload
                self.status_code = status
                self.headers = {"content-type": "application/json"}
                self.text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)

            def json(self):
                return self._payload

            def close(self):
                pass

            def iter_content(self, chunk_size=64_000):
                yield self.text.encode()

        # A minimal slice of morpho-org: three repos with audits + one without.
        repo_listing = [
            {"name": "morpho-blue", "default_branch": "main", "archived": False},
            {"name": "morpho-optimizers", "default_branch": "main", "archived": False},
            {"name": "vault-v2", "default_branch": "main", "archived": False},
            {"name": "frontend", "default_branch": "main", "archived": False},
        ]

        audits_per_repo = {
            "morpho-blue": [{
                "name": "2023-10-13-morpho-blue-and-speed-jump-irm-open-zeppelin.pdf", "type": "file",
                "download_url": "https://raw.githubusercontent.com/morpho-org/morpho-blue/main/audits/2023-10-13-morpho-blue-and-speed-jump-irm-open-zeppelin.pdf",
                "html_url": "https://github.com/morpho-org/morpho-blue/blob/main/audits/2023-10-13-morpho-blue-and-speed-jump-irm-open-zeppelin.pdf",
            }],
            "morpho-optimizers": [{
                "name": "TrailOfBits_Morpho_Compound.pdf", "type": "file",
                "download_url": "https://raw.githubusercontent.com/morpho-org/morpho-optimizers/main/audits/TrailOfBits_Morpho_Compound.pdf",
                "html_url": "https://github.com/morpho-org/morpho-optimizers/blob/main/audits/TrailOfBits_Morpho_Compound.pdf",
            }],
            "vault-v2": [{
                "name": "2025-05-19-spearbit.pdf", "type": "file",
                "download_url": "https://raw.githubusercontent.com/morpho-org/vault-v2/main/audits/2025-05-19-spearbit.pdf",
                "html_url": "https://github.com/morpho-org/vault-v2/blob/main/audits/2025-05-19-spearbit.pdf",
            }],
        }

        def fake_requests_get(url, **kwargs):
            # Org listing (orgs endpoint)
            if url.startswith("https://api.github.com/orgs/morpho-org/repos"):
                return FakeResp(repo_listing)
            if url.startswith("https://api.github.com/users/morpho-org/repos"):
                return FakeResp({"message": "Not Found"}, status=404)
            # Per-repo metadata
            for name in ("morpho-blue", "morpho-optimizers", "vault-v2", "frontend"):
                if url == f"https://api.github.com/repos/morpho-org/{name}":
                    return FakeResp({"default_branch": "main"})
            # Recursive tree — surfaces an `audits` folder for the three repos we care about
            if "/git/trees/main?recursive=1" in url:
                for name in ("morpho-blue", "morpho-optimizers", "vault-v2"):
                    if f"/repos/morpho-org/{name}/" in url:
                        return FakeResp({"tree": [{"path": "audits", "type": "tree"}]})
                return FakeResp({"tree": [{"path": "src", "type": "tree"}]})
            # Contents endpoint for each repo's audits directory
            if "/contents/audits" in url:
                for name, files in audits_per_repo.items():
                    if f"/repos/morpho-org/{name}/" in url:
                        return FakeResp(files)
            return FakeResp({}, status=404)

        monkeypatch.setattr(
            "services.discovery.audit_reports._requests.get", fake_requests_get,
        )

        result = search_audit_reports("morpho")
        urls = [r["url"] for r in result["reports"]]
        joined = " ".join(urls)
        assert "morpho-blue" in joined, f"morpho-blue audit missing; urls={urls}"
        assert "morpho-optimizers" in joined, f"morpho-optimizers audit missing; urls={urls}"
        assert "vault-v2" in joined, f"vault-v2 audit missing; urls={urls}"
        # At least one real audit per discovered repo.
        assert len(result["reports"]) >= 3, f"expected ≥3 reports, got {len(result['reports'])}"


# ============================================================================
# 6. _sync_audit_reports_to_db
# ============================================================================


class TestSyncAuditReportsToDb:
    def test_upserts_reports(self, monkeypatch):
        """Verify the sync function calls pg_insert with on_conflict_do_update."""
        from workers.discovery import _sync_audit_reports_to_db

        session = MagicMock()
        reports = [
            {
                "url": "https://a.com/audit",
                "auditor": "OZ",
                "title": "Audit 1",
                "pdf_url": None,
                "date": "2023-01-01",
                "confidence": 0.9,
                "source_url": "https://a.com/audit",
            },
        ]

        _sync_audit_reports_to_db(session, 1, reports)

        # session.execute should have been called once for the insert
        assert session.execute.call_count == 1
        # session.commit should have been called
        session.commit.assert_called_once()

    def test_skips_invalid_entries(self):
        """Reports missing url, auditor, or title are skipped."""
        from workers.discovery import _sync_audit_reports_to_db

        session = MagicMock()
        reports = [
            {"url": "", "auditor": "OZ", "title": "Audit"},  # empty URL
            {"url": "https://a.com", "auditor": "", "title": "Audit"},  # empty auditor
            {"url": "https://a.com", "auditor": "OZ", "title": ""},  # empty title
        ]

        _sync_audit_reports_to_db(session, 1, reports)

        # No inserts should have been made
        assert session.execute.call_count == 0
        session.commit.assert_called_once()


# ============================================================================
# 7. Discovery worker integration
# ============================================================================


class TestDiscoveryWorkerAuditIntegration:
    def _make_job(self, **overrides):
        defaults = {
            "id": uuid.uuid4(),
            "address": None,
            "company": "Aave",
            "name": None,
            "protocol_id": None,
            "request": {"company": "Aave", "chain": None, "analyze_limit": 5},
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_audit_search_called_during_company_discovery(self, monkeypatch):
        """Verify audit search is triggered and artifact is stored."""
        from workers.discovery import DiscoveryWorker

        job = self._make_job()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        # Track what gets stored
        stored_artifacts = {}

        # Mock all the dependencies
        monkeypatch.setattr(
            "workers.discovery.find_previous_company_inventory",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "workers.discovery.search_protocol_inventory",
            lambda *a, **kw: {"contracts": [], "official_domain": "aave.com"},
        )
        monkeypatch.setattr(
            "workers.discovery.merge_inventory",
            lambda prev, new: new,
        )
        monkeypatch.setattr(
            "workers.discovery.store_artifact",
            lambda s, jid, name, data=None, text_data=None: stored_artifacts.update({name: data}),
        )
        monkeypatch.setattr(
            "workers.discovery.get_or_create_protocol",
            lambda s, name, official_domain=None: SimpleNamespace(id=1),
        )
        monkeypatch.setattr(
            "workers.discovery.get_artifact",
            lambda s, jid, name: None,
        )

        audit_called = {"called": False}
        audit_result = {"company": "Aave", "reports": [_report()], "queries_used": 2, "errors": [], "notes": []}

        def fake_search_audits(company, official_domain=None, **kw):
            audit_called["called"] = True
            return audit_result

        monkeypatch.setattr("workers.discovery.search_audit_reports", fake_search_audits)
        monkeypatch.setattr("workers.discovery.merge_audit_reports", lambda prev, new: new)
        monkeypatch.setattr("workers.discovery._sync_audit_reports_to_db", lambda s, pid, reports: None)
        monkeypatch.setattr(
            "workers.discovery.count_analysis_children",
            lambda s, jid: 0,
        )

        # The _process_company will raise JobHandledDirectly at the end
        from workers.base import JobHandledDirectly

        worker = DiscoveryWorker()
        monkeypatch.setattr(worker, "update_detail", lambda s, j, d: None)

        with pytest.raises(JobHandledDirectly):
            worker._process_company(session, job)

        assert audit_called["called"]
        assert "audit_reports" in stored_artifacts
        assert len(stored_artifacts["audit_reports"]["reports"]) == 1

    def test_audit_failure_does_not_block_discovery(self, monkeypatch):
        """If audit search raises, the company discovery continues."""
        from workers.discovery import DiscoveryWorker

        job = self._make_job()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        monkeypatch.setattr("workers.discovery.find_previous_company_inventory", lambda *a, **kw: None)
        monkeypatch.setattr(
            "workers.discovery.search_protocol_inventory",
            lambda *a, **kw: {"contracts": [], "official_domain": None},
        )
        monkeypatch.setattr(
            "workers.discovery.store_artifact",
            lambda s, jid, name, data=None, text_data=None: None,
        )
        monkeypatch.setattr(
            "workers.discovery.get_or_create_protocol",
            lambda s, name, official_domain=None: SimpleNamespace(id=1),
        )
        monkeypatch.setattr("workers.discovery.get_artifact", lambda s, jid, name: None)

        # Make audit search crash
        def exploding_search(*a, **kw):
            raise RuntimeError("Tavily is down")

        monkeypatch.setattr("workers.discovery.search_audit_reports", exploding_search)
        monkeypatch.setattr("workers.discovery.count_analysis_children", lambda s, jid: 0)

        from workers.base import JobHandledDirectly

        worker = DiscoveryWorker()
        monkeypatch.setattr(worker, "update_detail", lambda s, j, d: None)

        # Should still complete (raises JobHandledDirectly from the "no contracts" path)
        with pytest.raises(JobHandledDirectly):
            worker._process_company(session, job)

    def test_prev_audit_artifact_merged(self, monkeypatch):
        """When a previous audit artifact exists, it's merged with new results."""
        from workers.discovery import DiscoveryWorker

        job = self._make_job()
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None

        prev_job = SimpleNamespace(id=uuid.uuid4())
        prev_audits = {"company": "Aave", "reports": [_report(url="https://old.com/audit")]}
        new_audits = {"company": "Aave", "reports": [_report(url="https://new.com/audit")], "queries_used": 2, "errors": [], "notes": []}

        stored = {}
        merge_calls = []

        monkeypatch.setattr(
            "workers.discovery.find_previous_company_inventory",
            lambda *a, **kw: prev_job,
        )
        monkeypatch.setattr(
            "workers.discovery.search_protocol_inventory",
            lambda *a, **kw: {"contracts": [], "official_domain": None},
        )

        def fake_get_artifact(s, jid, name):
            if name == "audit_reports" and jid == prev_job.id:
                return prev_audits
            return None

        monkeypatch.setattr("workers.discovery.get_artifact", fake_get_artifact)
        monkeypatch.setattr(
            "workers.discovery.store_artifact",
            lambda s, jid, name, data=None, text_data=None: stored.update({name: data}),
        )
        monkeypatch.setattr(
            "workers.discovery.get_or_create_protocol",
            lambda s, name, official_domain=None: SimpleNamespace(id=1),
        )
        monkeypatch.setattr("workers.discovery.search_audit_reports", lambda *a, **kw: new_audits)

        def fake_merge(prev, new):
            merge_calls.append((prev, new))
            return merge_audit_reports(prev, new)

        monkeypatch.setattr("workers.discovery.merge_audit_reports", fake_merge)
        monkeypatch.setattr("workers.discovery._sync_audit_reports_to_db", lambda s, pid, reports: None)
        monkeypatch.setattr("workers.discovery.count_analysis_children", lambda s, jid: 0)

        from workers.base import JobHandledDirectly

        worker = DiscoveryWorker()
        monkeypatch.setattr(worker, "update_detail", lambda s, j, d: None)

        with pytest.raises(JobHandledDirectly):
            worker._process_company(session, job)

        # merge_audit_reports was called
        assert len(merge_calls) == 1
        # Both old and new reports should be in the stored artifact
        stored_reports = stored["audit_reports"]["reports"]
        urls = {r["url"] for r in stored_reports}
        assert "https://old.com/audit" in urls
        assert "https://new.com/audit" in urls


# ============================================================================
# 6. Date extraction + LLM-filled auditor (Fix 3)
#
# Auditor inference is now an LLM concern (filename batch + final validation
# pass). The deterministic side keeps only date extraction, which is exact
# and benefits from being free of LLM nondeterminism.
# ============================================================================


class TestDateExtraction:
    @pytest.mark.parametrize("filename, expected", [
        ("2024.06.25 - Halborn - EFIP.pdf", "2024-06-25"),
        ("2023-12-20 - Hats Finance.md", "2023-12-20"),
        ("2025_03_15_audit.pdf", "2025-03-15"),
        ("20241109-scroll-native-minting.md", "2024-11-09"),
        ("2024-06_audit.pdf", "2024-06"),
        ("Audit-2023.pdf", "2023"),
        # URL-encoded
        ("2025.10.20%20-%20WeETH%20withdrawal%20adapter.pdf", "2025-10-20"),
        # Invalid month → falls through to year-only
        ("2024-13-01.pdf", "2024"),
        # Random digits don't match (audit report ID-like)
        ("Bundle_11.pdf", None),
        ("Cash Audit Report.pdf", None),
    ])
    def test_date_extraction(self, filename, expected):
        from services.discovery.audit_reports import _extract_date_from_filename
        assert _extract_date_from_filename(filename) == expected

    def test_augment_fills_date_only(self):
        """LLM output wins for date; auditor pass-through (LLM-only now)."""
        from services.discovery.audit_reports import _augment_filename_metadata
        # LLM date present — left untouched
        meta = {"auditor": "Cantina", "date": "2024-01-01", "title": "X"}
        out = _augment_filename_metadata("2025.10.20-Halborn.pdf", meta)
        assert out["date"] == "2024-01-01"
        assert out["auditor"] == "Cantina"
        # LLM date null → fill from filename
        meta = {"auditor": "Halborn", "date": None, "title": "X"}
        out = _augment_filename_metadata("2025.10.20-Halborn.pdf", meta)
        assert out["date"] == "2025-10-20"
        assert out["auditor"] == "Halborn"  # untouched
        # LLM auditor null AND filename has obvious auditor → still null
        # (auditor inference is the LLM's job now, not the augmenter's)
        meta = {"auditor": None, "date": None, "title": "X"}
        out = _augment_filename_metadata("2025.10.20-Halborn.pdf", meta)
        assert out["date"] == "2025-10-20"
        assert out.get("auditor") is None


# ============================================================================
# 7. Audit folder name matching (Fix 4)
# ============================================================================


class TestFolderNameMatching:
    """The recursive-tree scan recognises the broader folder name set."""

    @pytest.mark.parametrize("folder_name", [
        "audits", "audit", "audit-reports", "security-audits",
        "reviews", "security-reviews", "external-audits",
        "code-audits", "formal-verification", "security-reports",
    ])
    def test_recognized_folders(self, folder_name):
        from services.discovery.audit_reports import _AUDIT_FOLDER_LAST_SEGMENTS
        assert folder_name in _AUDIT_FOLDER_LAST_SEGMENTS

    @pytest.mark.parametrize("folder_name", [
        "src", "test", "lib", "node_modules", "docs",
    ])
    def test_unrelated_folders_excluded(self, folder_name):
        from services.discovery.audit_reports import _AUDIT_FOLDER_LAST_SEGMENTS
        assert folder_name not in _AUDIT_FOLDER_LAST_SEGMENTS


# ============================================================================
# 8. Org-enumeration dedup (Fix 1)
# ============================================================================


class TestOrgDedup:
    """``auto_hopped_orgs`` prevents the same GitHub org from being enumerated
    twice in a single ``search_audit_reports`` call."""

    def test_should_auto_hop_includes_org_kind(self):
        from services.discovery.audit_reports import _should_auto_hop_org
        # Bare org URL whose owner matches company name → auto-hop
        assert _should_auto_hop_org("https://github.com/morpho-org", "Morpho", set())
        # Same org already enumerated → don't re-hop
        assert not _should_auto_hop_org(
            "https://github.com/morpho-org", "Morpho", {"morpho-org"}
        )
        # Org name doesn't substring-match company → don't hop
        assert not _should_auto_hop_org("https://github.com/Certora", "Morpho", set())

    def test_org_then_tree_does_not_re_enumerate(self, monkeypatch):
        """Two URLs into the same org (one bare org, one tree) trigger
        exactly one ``_fetch_github_org_as_reports`` call."""
        org_url = "https://github.com/morpho-org"
        tree_url = "https://github.com/morpho-org/vault-v2/tree/main/audits"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [
                _tavily_result(org_url, "Morpho org"),
                _tavily_result(tree_url, "Morpho vault audits"),
            ],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        # Stage 1 LLM: classify both as audits.
        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                return json.dumps([])  # not used in this test
            return json.dumps([
                {"url": org_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "Morpho org",
                 "date": None, "confidence": 0.9},
                {"url": tree_url, "is_audit": True, "type": "listing",
                 "auditor": None, "title": "Morpho vault audits",
                 "date": None, "confidence": 0.9},
            ])

        monkeypatch.setattr("utils.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        # Count how many times the org-enumerate function is called.
        from services.discovery import audit_reports as ar
        call_count = {"n": 0}
        original = ar._fetch_github_org_as_reports

        def counted(*args, **kwargs):
            call_count["n"] += 1
            # Return non-None so the dedup set gets populated.
            return {"reports": [], "linked_urls": []}

        monkeypatch.setattr(ar, "_fetch_github_org_as_reports", counted)

        ar.search_audit_reports("Morpho")
        assert call_count["n"] == 1, (
            f"expected exactly one org enumeration, got {call_count['n']}"
        )

    def test_failed_enumeration_does_not_block_fallback(self, monkeypatch):
        """If org enumeration returns None (rate-limit, no repos), the
        owner is NOT marked covered, so a per-URL tree fetch can still run."""
        from services.discovery import audit_reports as ar

        auto_hopped: set[str] = set()
        monkeypatch.setattr(
            ar, "_fetch_github_org_as_reports",
            lambda *a, **kw: None,  # simulate failed enumeration
        )
        ar._maybe_auto_hop_to_org(
            "https://github.com/morpho-org/vault-v2/tree/main/audits",
            "Morpho", auto_hopped, 0.9, "2024-01-01T00:00:00", [],
        )
        assert "morpho-org" not in auto_hopped, (
            "failed enumeration must not register coverage"
        )


# ============================================================================
# 9. GitHub blob PDF routing (Fix 2)
# ============================================================================


class TestBlobPdfRouting:
    """A Stage-2 hit on ``Zellic/publications/blob/.../X.pdf`` routes to
    ``_expand_blob_to_directory`` instead of becoming a fallback-only entry."""

    def test_blob_pdf_triggers_directory_expansion(self, monkeypatch):
        from services.discovery import audit_reports as ar

        blob_url = (
            "https://github.com/Zellic/publications/blob/master/"
            "EtherFi%20-%20Zellic%20Audit%20Report.pdf"
        )

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(blob_url, "EtherFi Zellic audit")],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                return json.dumps([])
            return json.dumps([
                {"url": blob_url, "is_audit": True, "type": "report",
                 "auditor": "Zellic", "title": "EtherFi Zellic Audit",
                 "date": "2024-01-01", "confidence": 0.95},
            ])

        monkeypatch.setattr("utils.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        # Track invocations of the directory-expansion helper.
        expand_calls = {"n": 0}

        def counted_expand(*args, **kwargs):
            expand_calls["n"] += 1
            return {"reports": [], "linked_urls": []}

        monkeypatch.setattr(ar, "_expand_blob_to_directory", counted_expand)

        # The URL is for github.com/Zellic — the company "EtherFi" doesn't
        # substring-match "zellic", so no auto-hop fires; this isolates the
        # blob-PDF routing change.
        ar.search_audit_reports("EtherFi")

        assert expand_calls["n"] == 1, (
            f"expected blob PDF to trigger _expand_blob_to_directory once, "
            f"got {expand_calls['n']}"
        )


# ============================================================================
# 10. LLM validate-and-cluster pass
# ============================================================================


class TestLLMValidateAndCluster:
    """``_llm_validate_and_cluster`` clusters mirrors, drops invalids, fills
    auditors, and falls back gracefully on LLM failure."""

    def test_clusters_mirrors_and_picks_richest(self, monkeypatch):
        from services.discovery import audit_reports as ar

        reports = [
            # Cluster 1: same audit, two mirror entries; entry 1 has the PDF
            {"url": "https://docs.morpho.org/audit-X-2024.html",
             "pdf_url": None, "auditor": "Spearbit", "title": "Morpho X",
             "date": "2024-05-01"},
            {"url": "https://github.com/morpho-org/X/audits/spearbit-X.pdf",
             "pdf_url": "https://github.com/morpho-org/X/audits/spearbit-X.pdf",
             "auditor": "Spearbit", "title": "Spearbit Morpho X audit",
             "date": "2024-05-01"},
            # Standalone audit
            {"url": "https://example.com/halborn-Y.pdf",
             "pdf_url": "https://example.com/halborn-Y.pdf",
             "auditor": "Halborn", "title": "Halborn Y", "date": "2024-08-01"},
        ]

        def fake_llm_chat(messages, **kwargs):
            return json.dumps({
                "entries": [
                    {"i": 0, "valid": True, "cluster": 1},
                    {"i": 1, "valid": True, "cluster": 1},
                    {"i": 2, "valid": True, "cluster": 2},
                ],
            })

        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        result = ar._llm_validate_and_cluster(reports, "Morpho")
        assert result is not None
        cleaned, stats = result
        assert len(cleaned) == 2
        # The PDF-bearing entry wins as canonical
        cluster1_canonical = next(
            r for r in cleaned if "Morpho" in (r.get("title") or "")
            or "Spearbit" in (r.get("title") or "")
        )
        assert cluster1_canonical["pdf_url"] is not None
        assert stats["collapsed_mirrors"] == 1
        assert stats["dropped_invalid"] == 0

    def test_drops_invalid_entries(self, monkeypatch):
        from services.discovery import audit_reports as ar

        reports = [
            {"url": "https://example.com/audit.pdf", "pdf_url": "x.pdf",
             "auditor": "Spearbit", "title": "X audit", "date": "2024-01-01"},
            {"url": "https://mintlify.com/morpho-org/security/audits",
             "pdf_url": None, "auditor": "Unknown", "title": "audits",
             "date": None},
        ]

        def fake_llm_chat(messages, **kwargs):
            return json.dumps({
                "entries": [
                    {"i": 0, "valid": True, "cluster": 1},
                    {"i": 1, "valid": False, "reason": "docs landing page"},
                ],
            })

        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        result = ar._llm_validate_and_cluster(reports, "Morpho")
        assert result is not None
        cleaned, stats = result
        assert len(cleaned) == 1
        assert cleaned[0]["url"].endswith(".pdf")
        assert stats["dropped_invalid"] == 1

    def test_fills_unknown_auditor(self, monkeypatch):
        from services.discovery import audit_reports as ar

        reports = [
            {"url": "https://example.com/x.pdf", "pdf_url": "x.pdf",
             "auditor": "Unknown", "title": "Bundle 11", "date": None},
        ]

        def fake_llm_chat(messages, **kwargs):
            return json.dumps({
                "entries": [
                    {"i": 0, "valid": True, "cluster": 1, "auditor": "Certora"},
                ],
            })

        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        result = ar._llm_validate_and_cluster(reports, "EtherFi")
        assert result is not None
        cleaned, stats = result
        assert cleaned[0]["auditor"] == "Certora"
        assert stats["auditor_filled"] == 1

    def test_returns_none_on_llm_failure(self, monkeypatch):
        from services.discovery import audit_reports as ar

        def boom(*a, **kw):
            raise RuntimeError("LLM down")

        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", boom)

        reports = [{"url": "x", "pdf_url": None, "auditor": "X",
                    "title": "Y", "date": None}] * 2
        assert ar._llm_validate_and_cluster(reports, "Foo") is None

    def test_returns_none_on_unparseable_response(self, monkeypatch):
        from services.discovery import audit_reports as ar

        monkeypatch.setattr(
            "services.discovery.audit_reports.llm.chat",
            lambda *a, **kw: "not json at all",
        )

        reports = [{"url": "x", "pdf_url": None, "auditor": "X",
                    "title": "Y", "date": None}] * 2
        assert ar._llm_validate_and_cluster(reports, "Foo") is None

    def test_short_circuits_on_empty_list(self):
        from services.discovery import audit_reports as ar
        result = ar._llm_validate_and_cluster([], "Foo")
        assert result is not None
        cleaned, stats = result
        assert cleaned == []
        assert stats["collapsed_mirrors"] == 0

    def test_portfolio_allowlist_crawls_each_repo(self, monkeypatch):
        """Stage 3.5 calls ``_list_repo_root_for_company`` for every entry
        in ``_AUDITOR_PORTFOLIO_REPOS`` once, and skips entries whose owner
        was already auto-hopped."""
        from services.discovery import audit_reports as ar

        # No-op Tavily / Solodit / classification.
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.llm.chat",
            lambda *a, **kw: json.dumps({"entries": []}),
        )

        called_with: list[tuple[str, str]] = []

        def fake_root(owner, repo, company, debug=False):
            called_with.append((owner, repo))
            # Pretend Zellic returns one report; everyone else returns nothing.
            if owner == "Zellic" and repo == "publications":
                return {"reports": [{
                    "auditor": "Zellic", "title": "Foo Audit",
                    "date": "2024-01-01",
                    "pdf_url": "https://x.pdf",
                    "report_url": "https://x.pdf",
                }], "linked_urls": []}
            return None

        monkeypatch.setattr(
            "services.discovery.audit_reports._list_repo_root_for_company",
            fake_root,
        )

        result = ar.search_audit_reports("Foo")

        # Every portfolio entry should have been probed exactly once.
        assert called_with == list(ar._AUDITOR_PORTFOLIO_REPOS)
        # The Zellic hit should make it into the final result set.
        assert any(r.get("auditor") == "Zellic" for r in result["reports"])

    def test_portfolio_allowlist_skips_already_enumerated_orgs(self, monkeypatch):
        """If auto-hop already enumerated ``Layr-Labs`` (an allowlist entry),
        Stage 3.5 doesn't double-fetch it."""
        from services.discovery import audit_reports as ar

        # Force Tavily to return a Layr-Labs URL so auto-hop fires.
        # We also need the company name to substring-match; use "Layr".
        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [_tavily_result(
                "https://github.com/Layr-Labs/eigenlayer-contracts/tree/main/audits",
                "Layr-Labs audits",
            )],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "Below are file names" in content:
                return json.dumps([])
            if "candidate audit reports discovered" in content:
                return json.dumps({"entries": []})
            return json.dumps([{
                "url": "https://github.com/Layr-Labs/eigenlayer-contracts/tree/main/audits",
                "is_audit": True, "type": "listing",
                "auditor": None, "title": "Layr audits",
                "date": None, "confidence": 0.9,
            }])

        monkeypatch.setattr("utils.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        # Force the org-enumerate to return non-None so Layr-Labs gets
        # registered in auto_hopped_orgs.
        monkeypatch.setattr(
            "services.discovery.audit_reports._fetch_github_org_as_reports",
            lambda *a, **kw: {"reports": [], "linked_urls": []},
        )

        # Track portfolio calls — Layr-Labs entry should be skipped.
        portfolio_calls: list[tuple[str, str]] = []

        def fake_root(owner, repo, company, debug=False):
            portfolio_calls.append((owner, repo))
            return None

        monkeypatch.setattr(
            "services.discovery.audit_reports._list_repo_root_for_company",
            fake_root,
        )

        ar.search_audit_reports("Layr")

        # Layr-Labs is in the allowlist; auto-hop covered it; portfolio
        # crawl should skip the (Layr-Labs, eigenlayer-contracts) entry.
        assert ("Layr-Labs", "eigenlayer-contracts") not in portfolio_calls
        # All other portfolio entries should still be probed.
        for owner, repo in ar._AUDITOR_PORTFOLIO_REPOS:
            if owner == "Layr-Labs":
                continue
            assert (owner, repo) in portfolio_calls, (
                f"expected portfolio probe for {owner}/{repo}"
            )

    def test_orchestrator_falls_back_to_heuristic_on_llm_failure(self, monkeypatch):
        """When the validation LLM fails, ``_collapse_same_audit_mirrors``
        still runs so cross-host duplicates don't survive."""
        from services.discovery import audit_reports as ar

        # Fake Stage 1+2 to return two mirror entries on different hosts.
        url_a = "https://docs.example.com/audit.pdf"
        url_b = "https://github.com/proto/audits/audit.pdf"

        monkeypatch.setattr(
            "services.discovery.audit_reports._tavily_search",
            lambda *a, **kw: [
                _tavily_result(url_a, "Audit"),
                _tavily_result(url_b, "Audit"),
            ],
        )
        monkeypatch.setattr(
            "services.discovery.audit_reports.generate_followup_query",
            lambda *a, **kw: None,
        )

        # All LLM calls return data EXCEPT the validate-and-cluster one,
        # which we force to fail.
        def fake_llm_chat(messages, **kwargs):
            content = messages[0]["content"]
            if "candidate audit reports discovered" in content:
                # validate-and-cluster prompt
                raise RuntimeError("validation LLM down")
            if "Below are file names" in content:
                return json.dumps([])
            # Stage 1 classification: both URLs are direct PDFs
            return json.dumps([
                {"url": url_a, "is_audit": True, "type": "pdf",
                 "auditor": "Halborn", "title": "Halborn audit",
                 "date": "2024-01-01", "confidence": 0.9},
                {"url": url_b, "is_audit": True, "type": "pdf",
                 "auditor": "Halborn", "title": "Halborn audit",
                 "date": "2024-01-01", "confidence": 0.9},
            ])

        monkeypatch.setattr("utils.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports_llm.llm.chat", fake_llm_chat)
        monkeypatch.setattr("services.discovery.audit_reports.llm.chat", fake_llm_chat)

        result = ar.search_audit_reports("Proto")
        # Either the new filename-based pre-pass OR the heuristic mirror
        # collapse should dedupe the cross-host pair down to 1.
        assert len(result["reports"]) == 1
        assert any(
            "heuristic" in n.lower()
            or "Mirror dedup" in n
            or "Cross-source dedup" in n
            for n in result["notes"]
        )


# ============================================================================
# 11. Cross-source filename dedup + inherited-infrastructure tag
# ============================================================================


class TestFilenameDedup:
    def test_collapses_same_filename_different_urls(self):
        from services.discovery.audit_reports import _collapse_by_filename
        reports = [
            # Same PDF mirrored on two hosts — should collapse
            {"url": "https://solodit.cyfrin.io/audit.pdf",
             "pdf_url": "https://s3/audit.pdf",
             "auditor": "Spearbit", "title": "Foo", "date": "2024-05-01"},
            {"url": "https://github.com/spearbit/portfolio/blob/main/audit.pdf",
             "pdf_url": "https://raw.github.com/spearbit/portfolio/main/audit.pdf",
             "auditor": "Spearbit", "title": "Foo Audit longer title",
             "date": "2024-05-01"},
            # Different filename — standalone
            {"url": "https://github.com/x/y/other.pdf",
             "pdf_url": "https://github.com/x/y/other.pdf",
             "auditor": "Halborn", "title": "Bar", "date": "2024-05-01"},
        ]
        out = _collapse_by_filename(reports)
        assert len(out) == 2
        # Richer entry (longer title) wins
        foo = next(r for r in out if r["auditor"] == "Spearbit")
        assert "longer title" in foo["title"]

    def test_different_year_month_stays_separate(self):
        """Same filename, different date = different audits (retest, etc.)"""
        from services.discovery.audit_reports import _collapse_by_filename
        reports = [
            {"url": "x/audit.pdf", "pdf_url": "x/audit.pdf",
             "auditor": "Foo", "title": "A", "date": "2024-05-01"},
            {"url": "y/audit.pdf", "pdf_url": "y/audit.pdf",
             "auditor": "Foo", "title": "A", "date": "2024-11-01"},
        ]
        out = _collapse_by_filename(reports)
        assert len(out) == 2

    def test_missing_filename_never_groups(self):
        """Opaque URLs (cantina.xyz/portfolio/<uuid>) shouldn't merge with
        other opaque URLs."""
        from services.discovery.audit_reports import _collapse_by_filename
        reports = [
            {"url": "https://cantina.xyz/portfolio/abcd", "pdf_url": None,
             "auditor": "Cantina", "title": "X", "date": "2024-05-01"},
            {"url": "https://cantina.xyz/portfolio/efgh", "pdf_url": None,
             "auditor": "Cantina", "title": "Y", "date": "2024-05-01"},
        ]
        out = _collapse_by_filename(reports)
        # The trailing path segments differ, so stems differ → no collapse
        assert len(out) == 2

    def test_prefers_pdf_url_over_no_pdf(self):
        from services.discovery.audit_reports import _collapse_by_filename
        reports = [
            {"url": "x/audit.pdf", "pdf_url": None,
             "auditor": "Foo", "title": "A", "date": "2024-05-01"},
            {"url": "y/audit.pdf", "pdf_url": "y/audit.pdf",
             "auditor": "Foo", "title": "A", "date": "2024-05-01"},
        ]
        out = _collapse_by_filename(reports)
        assert len(out) == 1
        assert out[0]["pdf_url"] is not None


class TestProvenanceFields:
    def test_build_report_entry_passes_source_commit(self):
        """Forensic provenance fields (source_commit/repo/path) surface on
        the final report entry when the upstream fetch captured them."""
        from services.discovery.audit_reports import _build_report_entry
        sha = "a" * 40
        out = _build_report_entry(
            {
                "auditor": "Foo", "title": "T", "date": "2024-01-01",
                "pdf_url": "https://x.pdf",
                "source_commit": sha,
                "source_repo": "owner/repo",
                "source_path": "audits/X.pdf",
            },
            "https://src/", 0.9, "2024-01-01T00:00:00Z",
        )
        assert out["source_commit"] == sha
        assert out["source_repo"] == "owner/repo"
        assert out["source_path"] == "audits/X.pdf"

    def test_build_report_entry_omits_provenance_when_missing(self):
        """Non-GitHub sources (Solodit, docs pages) don't supply a commit
        SHA; the report entry should not carry empty placeholder fields."""
        from services.discovery.audit_reports import _build_report_entry
        out = _build_report_entry(
            {"auditor": "Foo", "title": "T", "date": "2024-01-01",
             "pdf_url": "https://x.pdf"},
            "https://src/", 0.9, "2024-01-01T00:00:00Z",
        )
        assert "source_commit" not in out
        assert "source_repo" not in out
        assert "source_path" not in out


class TestResolveBranchCommit:
    def test_resolves_and_caches(self, monkeypatch):
        from services.discovery import audit_reports as ar

        # Reset the module-level cache so this test runs in isolation.
        ar._BRANCH_SHA_CACHE.clear()

        call_count = {"n": 0}
        sha = "b" * 40

        def fake_get(url, **kwargs):
            call_count["n"] += 1
            class R:
                status_code = 200
                def json(self):
                    return {"ref": "refs/heads/main", "object": {"sha": sha}}
            return R()

        monkeypatch.setattr(ar._requests, "get", fake_get)
        assert ar._resolve_branch_commit("owner", "repo", "main") == sha
        assert call_count["n"] == 1
        # Second call should hit the cache, no extra HTTP
        assert ar._resolve_branch_commit("owner", "repo", "main") == sha
        assert call_count["n"] == 1

    def test_returns_none_on_404(self, monkeypatch):
        from services.discovery import audit_reports as ar
        ar._BRANCH_SHA_CACHE.clear()

        def fake_get(url, **kwargs):
            class R:
                status_code = 404
                def json(self):
                    return {"message": "Not Found"}
            return R()

        monkeypatch.setattr(ar._requests, "get", fake_get)
        assert ar._resolve_branch_commit("owner", "ghost-repo", "main") is None
