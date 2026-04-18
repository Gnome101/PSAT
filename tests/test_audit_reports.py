"""Pure-logic tests for the audit report discovery module.

The orchestrator — ``search_audit_reports`` and the LLM classify/extract
helpers — is exercised end-to-end in ``test_audit_discovery_integration.py``
against real HTTP fixtures via the ``responses`` library. That test is the
source of truth for the discovery pipeline's behaviour.

What lives here is everything that is *pure* (no HTTP, no LLM, no DB):
    - JSON parsing helpers tolerant of markdown fences / surrounding text
    - ``merge_audit_reports`` append-only dedup + richness selection
    - filename date-extraction regex
    - audit folder-name allowlist
    - single-org auto-hop policy check
    - cross-source filename dedup
    - provenance field plumbing
    - branch → commit SHA cache

Any test that would need mocks for Tavily / LLM / GitHub / Solodit belongs
in the integration test, not here.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery.audit_reports import merge_audit_reports
from services.discovery.audit_reports_llm import _parse_json_array, _parse_json_object

# ---------------------------------------------------------------------------
# Helper
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


# ---------------------------------------------------------------------------
# JSON parsing helpers — LLM outputs arrive wrapped in prose + markdown fences
# ---------------------------------------------------------------------------


class TestJsonParsing:
    def test_parse_array_clean(self):
        assert _parse_json_array('[{"a": 1}]') == [{"a": 1}]

    def test_parse_array_with_markdown_fences(self):
        assert _parse_json_array('```json\n[{"a": 1}]\n```') == [{"a": 1}]

    def test_parse_array_with_surrounding_text(self):
        assert _parse_json_array('Here is the result: [{"a": 1}] Done.') == [{"a": 1}]

    def test_parse_array_garbage_returns_none(self):
        assert _parse_json_array("not json at all") is None

    def test_parse_array_empty(self):
        assert _parse_json_array("[]") == []

    def test_parse_object_clean(self):
        assert _parse_json_object('{"a": 1}') == {"a": 1}

    def test_parse_object_with_markdown_fences(self):
        assert _parse_json_object('```json\n{"a": 1}\n```') == {"a": 1}

    def test_parse_object_with_surrounding_text(self):
        assert _parse_json_object('Here is the result: {"a": 1} Done.') == {"a": 1}

    def test_parse_object_garbage_returns_none(self):
        assert _parse_json_object("not json") is None

    def test_parse_object_empty(self):
        assert _parse_json_object("{}") == {}


# ---------------------------------------------------------------------------
# merge_audit_reports — append-only URL-keyed dedup, richness selection
# ---------------------------------------------------------------------------


class TestMergeAuditReports:
    def test_append_only_keeps_prev_only_reports(self):
        prev = {"company": "Aave", "reports": [_report(url="https://a.com/old")]}
        new = {"company": "Aave", "reports": [_report(url="https://b.com/new")]}
        merged = merge_audit_reports(prev, new)
        urls = {r["url"] for r in merged["reports"]}
        assert urls == {"https://a.com/old", "https://b.com/new"}

    def test_richer_entry_wins_on_overlap(self):
        sparse = _report(url="https://a.com/report", pdf_url=None, date=None)
        rich = _report(
            url="https://a.com/report",
            pdf_url="https://a.com/report.pdf",
            date="2023-06-15",
        )
        merged = merge_audit_reports(
            {"company": "X", "reports": [sparse]},
            {"company": "X", "reports": [rich]},
        )
        assert len(merged["reports"]) == 1
        assert merged["reports"][0]["pdf_url"] == "https://a.com/report.pdf"

    def test_prev_richer_beats_new(self):
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
        """Trailing slashes and case differences treated as the same URL."""
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


# ---------------------------------------------------------------------------
# Filename date extraction — the regex path that augments LLM-null dates
# ---------------------------------------------------------------------------


class TestFilenameDateExtraction:
    @pytest.mark.parametrize(
        "filename, expected",
        [
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
        ],
    )
    def test_date_extraction(self, filename, expected):
        from services.discovery.audit_reports import _extract_date_from_filename

        assert _extract_date_from_filename(filename) == expected

    def test_augment_only_fills_missing_llm_date(self):
        """Filename augmenter never overwrites an LLM-supplied date, and
        never guesses an auditor — that's the LLM's job."""
        from services.discovery.audit_reports import _augment_filename_metadata

        meta = {"auditor": "Cantina", "date": "2024-01-01", "title": "X"}
        out = _augment_filename_metadata("2025.10.20-Halborn.pdf", meta)
        assert out["date"] == "2024-01-01"
        assert out["auditor"] == "Cantina"

        meta = {"auditor": "Halborn", "date": None, "title": "X"}
        out = _augment_filename_metadata("2025.10.20-Halborn.pdf", meta)
        assert out["date"] == "2025-10-20"
        assert out["auditor"] == "Halborn"

        # LLM auditor null + filename has obvious auditor → augmenter still
        # leaves auditor unset.
        meta = {"auditor": None, "date": None, "title": "X"}
        out = _augment_filename_metadata("2025.10.20-Halborn.pdf", meta)
        assert out["date"] == "2025-10-20"
        assert out.get("auditor") is None


# ---------------------------------------------------------------------------
# Audit folder-name allowlist — decides what `src/`-siblings get enumerated
# ---------------------------------------------------------------------------


class TestFolderNameMatching:
    @pytest.mark.parametrize(
        "folder_name",
        [
            "audits",
            "audit",
            "audit-reports",
            "security-audits",
            "reviews",
            "security-reviews",
            "external-audits",
            "code-audits",
            "formal-verification",
            "security-reports",
        ],
    )
    def test_recognized_folders(self, folder_name):
        from services.discovery.audit_reports import _AUDIT_FOLDER_LAST_SEGMENTS

        assert folder_name in _AUDIT_FOLDER_LAST_SEGMENTS

    @pytest.mark.parametrize(
        "folder_name",
        [
            "src",
            "test",
            "lib",
            "node_modules",
            "docs",
        ],
    )
    def test_unrelated_folders_excluded(self, folder_name):
        from services.discovery.audit_reports import _AUDIT_FOLDER_LAST_SEGMENTS

        assert folder_name not in _AUDIT_FOLDER_LAST_SEGMENTS


# ---------------------------------------------------------------------------
# Auto-hop policy — pure predicate, no HTTP
# ---------------------------------------------------------------------------


class TestAutoHopPolicy:
    def test_should_auto_hop_includes_org_kind(self):
        from services.discovery.audit_reports import _should_auto_hop_org

        # Bare org URL whose owner matches company name → hop
        assert _should_auto_hop_org("https://github.com/morpho-org", "Morpho", set())
        # Same org already enumerated → don't re-hop
        assert not _should_auto_hop_org("https://github.com/morpho-org", "Morpho", {"morpho-org"})
        # Org name doesn't substring-match company → don't hop
        assert not _should_auto_hop_org("https://github.com/Certora", "Morpho", set())


# ---------------------------------------------------------------------------
# Cross-source filename dedup — the same PDF mirrored across hosts
# ---------------------------------------------------------------------------


class TestFilenameDedup:
    def test_collapses_same_filename_different_urls(self):
        from services.discovery.audit_reports import _collapse_by_filename

        reports = [
            # Same PDF mirrored on two hosts — should collapse
            {
                "url": "https://solodit.cyfrin.io/audit.pdf",
                "pdf_url": "https://s3/audit.pdf",
                "auditor": "Spearbit",
                "title": "Foo",
                "date": "2024-05-01",
            },
            {
                "url": "https://github.com/spearbit/portfolio/blob/main/audit.pdf",
                "pdf_url": "https://raw.github.com/spearbit/portfolio/main/audit.pdf",
                "auditor": "Spearbit",
                "title": "Foo Audit longer title",
                "date": "2024-05-01",
            },
            # Different filename — standalone
            {
                "url": "https://github.com/x/y/other.pdf",
                "pdf_url": "https://github.com/x/y/other.pdf",
                "auditor": "Halborn",
                "title": "Bar",
                "date": "2024-05-01",
            },
        ]
        out = _collapse_by_filename(reports)
        assert len(out) == 2
        # Richer entry (longer title) wins
        foo = next(r for r in out if r["auditor"] == "Spearbit")
        assert "longer title" in foo["title"]

    def test_different_year_month_stays_separate(self):
        """Same filename + different dates = different audits (retest)."""
        from services.discovery.audit_reports import _collapse_by_filename

        reports = [
            {"url": "x/audit.pdf", "pdf_url": "x/audit.pdf", "auditor": "Foo", "title": "A", "date": "2024-05-01"},
            {"url": "y/audit.pdf", "pdf_url": "y/audit.pdf", "auditor": "Foo", "title": "A", "date": "2024-11-01"},
        ]
        assert len(_collapse_by_filename(reports)) == 2

    def test_missing_filename_never_groups(self):
        """Opaque URLs (cantina.xyz/portfolio/<uuid>) shouldn't merge with
        other opaque URLs even if they look similar."""
        from services.discovery.audit_reports import _collapse_by_filename

        reports = [
            {
                "url": "https://cantina.xyz/portfolio/abcd",
                "pdf_url": None,
                "auditor": "Cantina",
                "title": "X",
                "date": "2024-05-01",
            },
            {
                "url": "https://cantina.xyz/portfolio/efgh",
                "pdf_url": None,
                "auditor": "Cantina",
                "title": "Y",
                "date": "2024-05-01",
            },
        ]
        assert len(_collapse_by_filename(reports)) == 2

    def test_prefers_pdf_url_over_no_pdf(self):
        from services.discovery.audit_reports import _collapse_by_filename

        reports = [
            {"url": "x/audit.pdf", "pdf_url": None, "auditor": "Foo", "title": "A", "date": "2024-05-01"},
            {"url": "y/audit.pdf", "pdf_url": "y/audit.pdf", "auditor": "Foo", "title": "A", "date": "2024-05-01"},
        ]
        out = _collapse_by_filename(reports)
        assert len(out) == 1
        assert out[0]["pdf_url"] is not None


# ---------------------------------------------------------------------------
# Provenance fields — GitHub-sourced audits carry commit/repo/path through
# ---------------------------------------------------------------------------


class TestProvenanceFields:
    def test_build_report_entry_passes_source_commit(self):
        from services.discovery.audit_reports import _build_report_entry

        sha = "a" * 40
        out = _build_report_entry(
            {
                "auditor": "Foo",
                "title": "T",
                "date": "2024-01-01",
                "pdf_url": "https://x.pdf",
                "source_commit": sha,
                "source_repo": "owner/repo",
                "source_path": "audits/X.pdf",
            },
            "https://src/",
            0.9,
            "2024-01-01T00:00:00Z",
        )
        assert out["source_commit"] == sha
        assert out["source_repo"] == "owner/repo"
        assert out["source_path"] == "audits/X.pdf"

    def test_build_report_entry_omits_provenance_when_missing(self):
        """Non-GitHub sources don't supply a commit SHA — entry stays clean."""
        from services.discovery.audit_reports import _build_report_entry

        out = _build_report_entry(
            {"auditor": "Foo", "title": "T", "date": "2024-01-01", "pdf_url": "https://x.pdf"},
            "https://src/",
            0.9,
            "2024-01-01T00:00:00Z",
        )
        assert "source_commit" not in out
        assert "source_repo" not in out
        assert "source_path" not in out


# ---------------------------------------------------------------------------
# Branch → commit SHA resolver cache — keeps GitHub rate limit out of the
# hot path when many URLs point at the same branch
# ---------------------------------------------------------------------------


class TestResolveBranchCommit:
    def test_resolves_and_caches(self, monkeypatch):
        from services.discovery import audit_reports as ar

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
        # Second call: cache hit, no extra HTTP.
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
