"""Live integration tests for company-scoped admin mutations.

Covers two idempotent admin-only POSTs that operate on a protocol's
existing inventory:

    POST /api/company/{name}/analyze-remaining     queue analysis for un-analyzed contracts
    POST /api/company/{name}/refresh_coverage      rebuild audit_contract_coverage rows

Both are idempotent — analyze-remaining is a no-op once every contract
has a job_id; refresh_coverage just re-derives coverage links — so they
are safe to run repeatedly against the staging DB.
"""

from __future__ import annotations

from tests.live.conftest import DEFAULT_TEST_COMPANY, LiveClient


def test_analyze_remaining_response_shape(analyzed_company, live_client: LiveClient):
    """``analyzed_company`` ran with ``limit=1``, so etherfi's inventory
    typically has un-analyzed entries on a fresh preview. Subsequent runs
    return ``queued=0`` once every contract has a ``job_id``. We assert
    only the response shape — exact ``queued`` count depends on prior
    test runs against this preview's DB."""
    body = live_client.analyze_remaining(DEFAULT_TEST_COMPANY)
    assert isinstance(body.get("queued"), int)
    assert body["queued"] >= 0
    assert isinstance(body.get("jobs"), list)
    # Each queued entry is {job_id, address}; spot-check shape on first.
    if body["jobs"]:
        first = body["jobs"][0]
        for key in ("job_id", "address"):
            assert key in first, f"queued job entry missing {key!r}: {first}"


def test_analyze_remaining_unknown_company_404(live_client: LiveClient):
    r = live_client._session.post(
        live_client._url("/api/company/psat-unknown-company-xyz/analyze-remaining"),
        timeout=15,
    )
    assert r.status_code == 404, f"unknown company should 404, got {r.status_code}: {r.text[:200]}"


def test_refresh_coverage_returns_count(analyzed_company, live_client: LiveClient):
    """Idempotent backfill — assert the response shape and that the row
    count is a non-negative integer. Whether any rows actually exist
    depends on whether the company has scoped audits; either way the
    endpoint must return cleanly."""
    body = live_client.refresh_company_coverage(DEFAULT_TEST_COMPANY)
    assert body["company"] == DEFAULT_TEST_COMPANY
    assert isinstance(body.get("protocol_id"), int)
    assert isinstance(body.get("coverage_rows"), int)
    assert body["coverage_rows"] >= 0
    # We pass verify_source_equivalence=False explicitly; the response
    # should echo it so the caller can confirm the fast path was used.
    assert body.get("verify_source_equivalence") is False


def test_refresh_coverage_unknown_company_404(live_client: LiveClient):
    r = live_client._session.post(
        live_client._url("/api/company/psat-unknown-company-xyz/refresh_coverage"),
        params={"verify_source_equivalence": "false"},
        timeout=30,
    )
    assert r.status_code == 404, f"unknown company should 404, got {r.status_code}: {r.text[:200]}"


def test_refresh_coverage_idempotent(analyzed_company, live_client: LiveClient):
    """Calling refresh twice in a row should produce the same row count.
    A non-idempotent backfill would either grow rows on each call (bug:
    duplicates) or shrink them (bug: deletion-then-reinsert race)."""
    first = live_client.refresh_company_coverage(DEFAULT_TEST_COMPANY)
    second = live_client.refresh_company_coverage(DEFAULT_TEST_COMPANY)
    assert first["coverage_rows"] == second["coverage_rows"], (
        "refresh_coverage is not idempotent: "
        f"first call={first['coverage_rows']} rows, second={second['coverage_rows']} rows"
    )


def test_analyze_remaining_unauth_rejected(live_base_url: str):
    # Belt-and-suspenders: confirm the admin-key gate is wired on this
    # endpoint specifically. Other endpoints have similar tests in
    # test_auth_and_errors.py; this one's in scope here because it's
    # newly-covered and its mutation could be expensive if the gate slipped.
    import requests as _r

    r = _r.post(
        live_base_url + f"/api/company/{DEFAULT_TEST_COMPANY}/analyze-remaining",
        timeout=15,
    )
    assert r.status_code in (401, 403), (
        f"unauth POST analyze-remaining should be 401/403, got {r.status_code}: {r.text[:200]}"
    )
