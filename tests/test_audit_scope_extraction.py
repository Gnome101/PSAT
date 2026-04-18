"""Unit tests for ``services.audits.scope_extraction``.

No Postgres, no minio, no LLM — every function is tested in isolation.
The LLM stub env (``PSAT_LLM_STUB_DIR``) is cleared per test so we can
drive ``_call_llm`` through different code paths deterministically.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.audits.scope_extraction import (  # noqa: E402
    PROMPT_VERSION,
    LLMUnavailableError,
    ScopeSection,
    _build_prompt,
    _call_llm,
    _split_text_into_chunks,
    build_artifact_payload,
    extract_contracts_regex_fallback,
    extract_date_from_pdf_text,
    extract_scope_via_chunk_scan,
    extract_scope_with_llm,
    locate_scope_section,
    scope_artifact_key,
    validate_contracts,
)

# ---------------------------------------------------------------------------
# Helpers — build page-annotated fixture text matching extract_text_from_pdf
# ---------------------------------------------------------------------------


def _page(n: int, body: str) -> str:
    """Wrap ``body`` in the same page marker that pypdf extraction emits."""
    return f"\f\n--- page {n} ---\n\f\n{body}"


def _doc(*pages: str) -> str:
    return "".join(pages).strip()


@pytest.fixture(autouse=True)
def _clear_stub_env(monkeypatch):
    """Ensure tests control PSAT_LLM_STUB_DIR explicitly."""
    monkeypatch.delenv("PSAT_LLM_STUB_DIR", raising=False)
    monkeypatch.delenv("PSAT_SCOPE_LLM_MODEL", raising=False)


# ---------------------------------------------------------------------------
# locate_scope_section
# ---------------------------------------------------------------------------


def test_locate_scope_section_finds_basic_scope_header():
    text = _doc(
        _page(1, "Audit Report for Example Protocol\nExecutive Summary text."),
        _page(2, "Scope\nThe following contracts were reviewed: Pool.sol, Vault.sol"),
        _page(3, "Findings\nNone critical."),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert sections[0].start_page == 2
    assert "Pool.sol" in sections[0].text_slice


def test_locate_scope_section_finds_longer_phrases():
    # "Smart Contracts in Scope" appears before "Scope" — we should hit
    # the more-specific one first. Both resolve to the same region due to
    # the overlap-merge.
    text = _doc(
        _page(1, "Introduction"),
        _page(2, "Smart Contracts in Scope\nPool.sol\nVault.sol\nStrategy.sol"),
        _page(3, "End of scope"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Pool.sol" in sections[0].text_slice


def test_locate_scope_section_case_insensitive():
    text = _doc(
        _page(1, "Intro"),
        _page(2, "FILES IN SCOPE\nPool.sol"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1


def test_locate_scope_section_returns_empty_when_no_header():
    text = _doc(
        _page(1, "Executive Summary"),
        _page(2, "We reviewed several contracts."),
        _page(3, "Conclusion"),
    )
    assert locate_scope_section(text) == []


def test_locate_scope_section_captures_three_pages_of_context():
    # Header on page 2, table extends onto page 4. Worker should include
    # pages 2-4 (3 pages total) in the slice.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "Scope\n\nPool.sol 420 nSLOC"),
        _page(3, "Vault.sol 310 nSLOC"),
        _page(4, "Strategy.sol 180 nSLOC\nEnd of table"),
        _page(5, "Findings"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Pool.sol" in sections[0].text_slice
    assert "Strategy.sol" in sections[0].text_slice
    # Page 5 stays out of the slice.
    assert "Findings" not in sections[0].text_slice


def test_locate_scope_section_merges_overlapping_slices():
    # "Scope" on p.2 and "Files in scope" on p.3 — their 3-page windows
    # overlap, so we expect one merged section, not two.
    text = _doc(
        _page(1, "Intro"),
        _page(2, "Scope\nsome prose"),
        _page(3, "Files in scope\nPool.sol"),
        _page(4, "More prose"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1


def test_locate_scope_section_survives_no_page_markers():
    # Guard against bodies that somehow skipped the page-marker shim.
    text = "Scope\nPool.sol reviewed.\nMore content."
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert sections[0].start_page == 1


def test_locate_scope_section_matches_numbered_headers():
    # Halborn: "5. SCOPE" — numbered section prefix. Real-world audit PDF
    # format that the first iteration of the regex rejected.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "5. SCOPE\nFILES AND REPOSITORY\n(c) Items in scope:\nsrc/Token.sol"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Token.sol" in sections[0].text_slice


def test_locate_scope_section_matches_decimal_numbered_headers():
    # "5.1 Files in scope" — sub-section numbering.
    text = _doc(
        _page(1, "Intro"),
        _page(2, "5.1 Files in scope\nPool.sol\nVault.sol"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Pool.sol" in sections[0].text_slice


def test_locate_scope_section_matches_project_scope_header():
    # Certora: "Project Scope" as a section heading.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "Project Scope\nProject Name\nether.fi"),
        _page(
            3,
            "The following contract list is included in the scope of this audit:\n- src/Pool.sol",
        ),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Pool.sol" in sections[0].text_slice


def test_locate_scope_section_matches_audited_files_header():
    # Nethermind: "2 Audited Files" — the heading style most audits with
    # inline file-count tables use.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "2 Audited Files\nContract LoC Comments\n1 src/Pool.sol 420"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Pool.sol" in sections[0].text_slice


def test_locate_scope_section_tolerates_double_spaces_between_words():
    # pypdf emits "Project  Scope" (two spaces) for Certora-style PDFs.
    # A rigid single-space match would miss the header.
    text = _doc(
        _page(1, "Intro"),
        _page(2, "Project  Scope  \nProject  Name: ether.fi\nPool.sol"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "Pool.sol" in sections[0].text_slice


# ---------------------------------------------------------------------------
# Ligature normalization
# ---------------------------------------------------------------------------


def test_locate_scope_section_normalizes_ligatures_in_headers():
    # A scope-section header containing "scope" is unaffected, but a
    # filename like "EthﬁL2Token.sol" has to survive through to the
    # caller as "EthfiL2Token.sol" so validation passes.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "Scope\nItems in scope: src/EthﬁL2Token.sol"),
    )
    sections = locate_scope_section(text)
    assert len(sections) == 1
    assert "EthfiL2Token.sol" in sections[0].text_slice


def test_validate_contracts_after_ligature_normalization():
    # The worker normalizes raw text before validation; this test pins
    # the post-normalization behaviour — "EthfiL2Token" should survive
    # even if the LLM returned the clean form.
    from services.audits.scope_extraction import _normalize_ligatures

    raw_with_ligature = "Items in scope: src/EthﬁL2Token.sol reviewed."
    normalized = _normalize_ligatures(raw_with_ligature)
    assert "EthfiL2Token" in normalized
    assert validate_contracts(["EthfiL2Token"], normalized) == ["EthfiL2Token"]


# ---------------------------------------------------------------------------
# validate_contracts
# ---------------------------------------------------------------------------


def test_validate_contracts_drops_hallucinated_names():
    names = ["Pool", "Vault", "FakeContract"]
    raw = "We audited Pool and Vault. Findings inside."
    assert validate_contracts(names, raw) == ["Pool", "Vault"]


def test_validate_contracts_is_case_insensitive():
    names = ["POOL", "vault"]
    raw = "Pool and Vault contracts."
    assert validate_contracts(names, raw) == ["POOL", "vault"]


def test_validate_contracts_empty_input():
    assert validate_contracts([], "anything") == []


def test_validate_contracts_drops_empty_strings():
    assert validate_contracts(["", "Pool", "  "], "Pool") == ["Pool"]


# ---------------------------------------------------------------------------
# extract_contracts_regex_fallback
# ---------------------------------------------------------------------------


def test_regex_fallback_picks_up_dotsol_names():
    text = "Reviewed Pool.sol and Vault.sol; also mentioned Mocks/foo.txt."
    assert extract_contracts_regex_fallback(text) == ["Pool", "Vault"]


def test_regex_fallback_ignores_lowercase_start():
    text = "pool.sol is a dep; Vault.sol is in scope."
    assert extract_contracts_regex_fallback(text) == ["Vault"]


def test_regex_fallback_dedupes():
    text = "Pool.sol in repo A, Pool.sol in repo B, Vault.sol elsewhere."
    assert extract_contracts_regex_fallback(text) == ["Pool", "Vault"]


def test_regex_fallback_handles_vyper():
    text = "CurvePool.vy and ConvexBooster.sol"
    assert extract_contracts_regex_fallback(text) == ["CurvePool", "ConvexBooster"]


# ---------------------------------------------------------------------------
# extract_date_from_pdf_text
# ---------------------------------------------------------------------------


def test_extract_date_iso_format():
    text = "Audit Report\nSpearbit 2024-12-19\nby Alice and Bob"
    assert extract_date_from_pdf_text(text) == "2024-12-19"


def test_extract_date_day_month_year():
    text = "Cover page\nPublished 19 December 2024 by Spearbit"
    assert extract_date_from_pdf_text(text) == "2024-12-19"


def test_extract_date_month_year_only():
    text = "Cover page\nAudit delivered December 2024"
    assert extract_date_from_pdf_text(text) == "2024-12-00"


def test_extract_date_returns_none_when_no_match():
    text = "Cover page with no date anywhere on the first few lines."
    assert extract_date_from_pdf_text(text) is None


def test_extract_date_looks_only_at_title_region():
    # A date deep in the body (past the title-region window) should not
    # be picked up as the title-page date.
    title = "Cover page without any date\n" + ("filler " * 1200)
    footer = "2024-01-01"
    text = title + footer
    assert extract_date_from_pdf_text(text) is None


def test_extract_date_handles_ordinal_suffix_day_first():
    # Halborn-style: "19th December 2024"
    text = "Cover\nDelivered on 19th December 2024 by Firm"
    assert extract_date_from_pdf_text(text) == "2024-12-19"


def test_extract_date_handles_ordinal_suffix_month_first():
    # "December 19th, 2024"
    text = "Cover\nPublished December 19th, 2024"
    assert extract_date_from_pdf_text(text) == "2024-12-19"


def test_extract_date_handles_all_ordinal_suffixes():
    # 1st / 2nd / 3rd / 4th — all four forms.
    for day_str, day in (("1st", 1), ("2nd", 2), ("3rd", 3), ("4th", 4)):
        text = f"Cover\nDelivered {day_str} January 2024"
        assert extract_date_from_pdf_text(text) == f"2024-01-{day:02d}"


def test_extract_date_handles_us_slash_format():
    # "12/19/2024" — second group > 12 so this is unambiguously MM/DD/YYYY.
    text = "Cover\nAudit date: 12/19/2024"
    assert extract_date_from_pdf_text(text) == "2024-12-19"


def test_extract_date_disambiguates_slash_format_when_first_is_day():
    # "19/12/2024" — first group > 12, so must be DD/MM/YYYY. We flip.
    text = "Cover\n19/12/2024"
    assert extract_date_from_pdf_text(text) == "2024-12-19"


def test_extract_date_skips_ambiguous_slash_format():
    # "05/02/2024" — both operands ≤ 12, could be May 2 or Feb 5.
    # Rather than guessing (and silently producing wrong dates for
    # auditors that use DD/MM like Certora), skip to the next pattern.
    # With no other date in the text, the extractor returns None.
    text = "Cover\nAudit date: 05/02/2024"
    assert extract_date_from_pdf_text(text) is None


def test_extract_date_prefers_prose_over_ambiguous_slash():
    # When an ambiguous slash date appears alongside a prose date, we
    # should return the prose one (which is unambiguous).
    text = "Cover\nAudit: 05/02/2024\nDelivered: 10 March 2024"
    assert extract_date_from_pdf_text(text) == "2024-03-10"


def test_extract_date_extended_window_catches_dates_past_2000_chars():
    # Some PDFs have long cover boilerplate before the date. The window
    # was extended from 2000 → 6000 chars; a date at ~3500 should hit.
    prefix = "boilerplate " * 250  # ~3000 chars
    text = prefix + "2024-03-14 " + "more " * 100
    assert extract_date_from_pdf_text(text) == "2024-03-14"


# ---------------------------------------------------------------------------
# _call_llm stub mechanism
# ---------------------------------------------------------------------------


def test_call_llm_uses_digest_stub_when_available(tmp_path, monkeypatch):
    prompt = "Prompt one"
    digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    (tmp_path / f"{digest}.json").write_text('["Pool","Vault"]')
    monkeypatch.setenv("PSAT_LLM_STUB_DIR", str(tmp_path))
    response, model = _call_llm(prompt)
    assert response == '["Pool","Vault"]'
    assert model.startswith("stub:")


def test_call_llm_falls_back_to_default(tmp_path, monkeypatch):
    (tmp_path / "_default.json").write_text('["Default"]')
    monkeypatch.setenv("PSAT_LLM_STUB_DIR", str(tmp_path))
    response, model = _call_llm("anything")
    assert response == '["Default"]'
    assert model == "stub:_default"


def test_call_llm_raises_when_no_stub(tmp_path, monkeypatch):
    monkeypatch.setenv("PSAT_LLM_STUB_DIR", str(tmp_path))
    with pytest.raises(LLMUnavailableError):
        _call_llm("no matching fixture")


# ---------------------------------------------------------------------------
# extract_scope_with_llm — parsing tolerance
# ---------------------------------------------------------------------------


def _setup_stub(tmp_path, monkeypatch, response_body: str) -> None:
    (tmp_path / "_default.json").write_text(response_body)
    monkeypatch.setenv("PSAT_LLM_STUB_DIR", str(tmp_path))


def test_extract_scope_with_llm_parses_string_array(tmp_path, monkeypatch):
    _setup_stub(tmp_path, monkeypatch, '["Pool","Vault","Strategy"]')
    sections = [ScopeSection(1, 1, "scope", "Pool.sol Vault.sol Strategy.sol")]
    names, _, _ = extract_scope_with_llm(sections, "T", "A")
    assert names == ["Pool", "Vault", "Strategy"]


def test_extract_scope_with_llm_tolerates_markdown_fence(tmp_path, monkeypatch):
    _setup_stub(tmp_path, monkeypatch, '```json\n["Pool"]\n```')
    sections = [ScopeSection(1, 1, "scope", "Pool.sol")]
    names, _, _ = extract_scope_with_llm(sections, "T", "A")
    assert names == ["Pool"]


def test_extract_scope_with_llm_strips_extensions_and_dedupes(tmp_path, monkeypatch):
    _setup_stub(tmp_path, monkeypatch, '["Pool.sol","pool","Vault.vy"]')
    sections = [ScopeSection(1, 1, "scope", "Pool Vault")]
    names, _, _ = extract_scope_with_llm(sections, "T", "A")
    # "Pool.sol" and "pool" dedupe under case-insensitive match.
    assert names == ["Pool", "Vault"]


def test_extract_scope_with_llm_accepts_object_entries(tmp_path, monkeypatch):
    _setup_stub(
        tmp_path,
        monkeypatch,
        '[{"name":"Pool"},{"contract_name":"Vault"},{"file":"Strategy.sol"}]',
    )
    sections = [ScopeSection(1, 1, "scope", "Pool Vault Strategy.sol")]
    names, _, _ = extract_scope_with_llm(sections, "T", "A")
    assert names == ["Pool", "Vault", "Strategy"]


def test_extract_scope_with_llm_raises_on_unparseable(tmp_path, monkeypatch):
    _setup_stub(tmp_path, monkeypatch, "this is not JSON at all")
    sections = [ScopeSection(1, 1, "scope", "anything")]
    with pytest.raises(LLMUnavailableError):
        extract_scope_with_llm(sections, "T", "A")


def test_extract_scope_with_llm_handles_empty_array(tmp_path, monkeypatch):
    _setup_stub(tmp_path, monkeypatch, "[]")
    sections = [ScopeSection(1, 1, "scope", "anything")]
    names, _, _ = extract_scope_with_llm(sections, "T", "A")
    assert names == []


# ---------------------------------------------------------------------------
# build_artifact_payload
# ---------------------------------------------------------------------------


def test_build_artifact_payload_has_every_expected_key():
    payload = build_artifact_payload(
        ["Pool", "Vault"],
        method="llm",
        model="google/gemini-2.0-flash-001",
        extracted_date="2024-12-19",
        raw_response='["Pool","Vault"]',
    )
    assert payload["contracts"] == ["Pool", "Vault"]
    assert payload["method"] == "llm"
    assert payload["model"] == "google/gemini-2.0-flash-001"
    assert payload["extracted_date"] == "2024-12-19"
    assert payload["prompt_version"] == PROMPT_VERSION
    assert payload["raw_llm_response"] == '["Pool","Vault"]'
    assert "extracted_at" in payload


def test_build_artifact_payload_json_roundtrip():
    payload = build_artifact_payload(
        ["Pool"],
        method="regex_fallback",
        model=None,
        extracted_date=None,
        raw_response=None,
    )
    dumped = json.dumps(payload)
    again = json.loads(dumped)
    assert again["contracts"] == ["Pool"]
    assert again["method"] == "regex_fallback"


def test_build_artifact_payload_preserves_scope_section_text():
    payload = build_artifact_payload(
        ["Pool"],
        method="llm_chunk_scan",
        model="google/gemini-2.0-flash-001",
        extracted_date="2024-12-19",
        raw_response='["Pool"]',
        scope_section_text="The following contracts were audited:\nsrc/Pool.sol",
    )
    assert payload["scope_section_text"] == (
        "The following contracts were audited:\nsrc/Pool.sol"
    )


def test_build_artifact_payload_caps_scope_section_text_at_20k():
    huge = "x" * 50_000
    payload = build_artifact_payload(
        ["Pool"],
        method="llm",
        model=None,
        extracted_date=None,
        raw_response=None,
        scope_section_text=huge,
    )
    # Capped at 20k so the artifact stays manageable.
    assert payload["scope_section_text"] is not None
    assert len(payload["scope_section_text"]) <= 20_000


# ---------------------------------------------------------------------------
# scope_artifact_key + build_prompt sanity
# ---------------------------------------------------------------------------


def test_scope_artifact_key_format():
    assert scope_artifact_key(42) == "audits/scope/42.json"


def test_build_prompt_includes_title_and_scope_text():
    sections = [ScopeSection(1, 1, "scope", "Pool.sol  Vault.sol")]
    prompt = _build_prompt(sections, "My Audit", "SomeFirm")
    assert "My Audit" in prompt
    assert "SomeFirm" in prompt
    assert "Pool.sol" in prompt


def test_build_prompt_truncates_very_large_scope_text():
    huge = "A" * 100_000
    sections = [ScopeSection(1, 1, "scope", huge)]
    prompt = _build_prompt(sections, "T", "A")
    # The prompt body shouldn't approach 100k chars — it's capped.
    assert len(prompt) < 60_000


# ---------------------------------------------------------------------------
# Content-pattern matching (body-prose scope intros)
# ---------------------------------------------------------------------------


def test_locate_scope_section_matches_body_prose_contract_list():
    # Certora-style: scope is introduced by a prose phrase, not a section
    # header. The pattern catches the intro and the 2-page window pulls
    # in the bulleted list that follows.
    text = _doc(
        _page(1, "Cover\nProject Overview"),
        _page(
            2,
            "The following contract list is included in the scope of this audit:\n"
            "- src/Pool.sol\n- src/Vault.sol",
        ),
    )
    sections = locate_scope_section(text)
    assert len(sections) >= 1
    assert any("Pool.sol" in s.text_slice for s in sections)


def test_locate_scope_section_matches_following_files_phrase():
    text = _doc(
        _page(1, "Cover"),
        _page(
            2,
            "We reviewed the following files:\nPool.sol\nVault.sol\nStrategy.sol",
        ),
    )
    sections = locate_scope_section(text)
    assert len(sections) >= 1
    assert any("Pool.sol" in s.text_slice for s in sections)


def test_locate_scope_section_matches_colon_intro():
    # "Contracts reviewed:" / "Files audited:" — a common trailing-colon form.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "Contracts reviewed:\n- Pool.sol\n- Vault.sol"),
    )
    sections = locate_scope_section(text)
    assert len(sections) >= 1
    assert any("Pool.sol" in s.text_slice for s in sections)


def test_content_pattern_does_not_match_mere_scope_mention():
    # Body prose like "falls outside the scope" should NOT match — that
    # would pollute the results with unrelated prose.
    text = _doc(
        _page(1, "Cover"),
        _page(2, "Certain edge cases fall outside the scope of this review."),
        _page(3, "More prose without any scope listing."),
    )
    sections = locate_scope_section(text)
    # No header, no valid content intro — should be []
    assert sections == []


def test_content_pattern_coexists_with_header_pattern():
    # When BOTH a header and a content-pattern exist, both get captured
    # (the LLM benefits from seeing both locations).
    text = _doc(
        _page(1, "Cover"),
        _page(2, "Scope\n(see later sections)"),
        _page(4, "The following contracts were audited:\n- Pool.sol"),
    )
    sections = locate_scope_section(text)
    # At least one section; could be merged or two, depending on overlap.
    assert len(sections) >= 1
    combined = "\n".join(s.text_slice for s in sections)
    assert "Pool.sol" in combined


# ---------------------------------------------------------------------------
# Chunk-scan fallback
# ---------------------------------------------------------------------------


def test_split_text_into_chunks_caps_at_max_chunks():
    # 10 pages × default chunk size of 5 pages → 2 chunks. But the cap is
    # 4 chunks, so a 30-page document should produce 4 chunks, not 6.
    text = _doc(*(_page(i, f"page {i} body") for i in range(1, 31)))
    chunks = _split_text_into_chunks(text)
    assert 1 <= len(chunks) <= 4


def test_split_text_into_chunks_covers_pages_contiguously():
    text = _doc(*(_page(i, f"page {i} body ") * 30 for i in range(1, 21)))
    chunks = _split_text_into_chunks(text)
    # First chunk starts at page 1. Pages are contiguous across chunks.
    assert chunks[0].start_page == 1
    for prev, nxt in zip(chunks, chunks[1:]):
        assert nxt.start_page == prev.end_page + 1


def test_chunk_scan_stops_at_first_hit(tmp_path, monkeypatch):
    # Configure the LLM stub so chunk 1 returns [] but chunk 2 returns
    # scope. The scan should stop after chunk 2 — the fake_call's predicate
    # looks for a unique marker present ONLY in chunk 2's content, since
    # the prompt template itself mentions "Pool" as an example contract.
    prompts_seen: list[str] = []

    def fake_call(prompt):
        prompts_seen.append(prompt)
        if "UNIQUE_SCOPE_MARKER_XYZ" in prompt:
            return '["Pool", "Vault"]', "stub"
        return "[]", "stub"

    monkeypatch.setattr(
        "services.audits.scope_extraction._call_llm", fake_call
    )

    text = _doc(
        *(_page(i, "boilerplate " * 30) for i in range(1, 6)),
        *(
            _page(i, "Pool.sol Vault.sol UNIQUE_SCOPE_MARKER_XYZ reviewed")
            for i in range(6, 11)
        ),
    )
    names, response, model, chunks_used, winning_chunk = extract_scope_via_chunk_scan(
        text, "Title", "Auditor"
    )
    assert names == ["Pool", "Vault"]
    assert chunks_used == 2
    assert model == "stub"
    # The winning chunk carries the exact text the LLM saw — asserting
    # its presence and marker content proves the provenance field is
    # populated for artifact writes.
    assert winning_chunk is not None
    assert "UNIQUE_SCOPE_MARKER_XYZ" in winning_chunk.text_slice
    assert len(prompts_seen) == 2


def test_chunk_scan_returns_empty_when_no_chunk_has_scope(
    tmp_path, monkeypatch
):
    def fake_call(prompt):
        return "[]", "stub"

    monkeypatch.setattr(
        "services.audits.scope_extraction._call_llm", fake_call
    )

    text = _doc(*(_page(i, "no scope anywhere " * 20) for i in range(1, 11)))
    names, response, model, chunks_used, winning_chunk = extract_scope_via_chunk_scan(
        text, "T", "A"
    )
    assert names == []
    # Every chunk got consulted because none hit.
    assert chunks_used >= 1
    assert winning_chunk is None


def test_chunk_scan_raises_only_when_every_call_fails(monkeypatch):
    # If the LLM raises on every chunk, propagate. A single bad chunk
    # should not mask a later successful one, but all-failures must surface.
    def fake_call(prompt):
        raise LLMUnavailableError("network down")

    monkeypatch.setattr(
        "services.audits.scope_extraction._call_llm", fake_call
    )
    text = _doc(*(_page(i, "x") for i in range(1, 6)))
    with pytest.raises(LLMUnavailableError):
        extract_scope_via_chunk_scan(text, "T", "A")
