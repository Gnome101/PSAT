"""Live integration tests for the company-scoped read endpoints.

``GET /api/company/{name}`` is the biggest endpoint in api.py — a
700+-line aggregation across jobs, contracts, controls, audits, and
upgrade history. Shape regressions on this one break the entire
company page.
"""

from __future__ import annotations

from tests.live.conftest import DEFAULT_TEST_COMPANY, LiveClient


def test_company_overview_basic_shape(analyzed_company, live_client: LiveClient):
    overview = live_client.company_overview(DEFAULT_TEST_COMPANY)
    assert isinstance(overview, dict)
    # At least one of the expected top-level keys must be present. The
    # endpoint has evolved over time; loosely asserting membership here
    # rather than an exact schema keeps the test resilient to additions
    # without letting a full-payload regression slip through.
    expected_any = {"contracts", "contract_count", "owner_groups", "protocol_id", "company"}
    assert expected_any & set(overview.keys()), (
        f"company overview missing every expected key: got {list(overview.keys())}"
    )


def test_company_overview_includes_contracts(analyzed_company, live_client: LiveClient):
    overview = live_client.company_overview(DEFAULT_TEST_COMPANY)
    contracts = overview.get("contracts") or []
    assert contracts, "analyzed_company fixture ran with limit=1 but overview shows no contracts"


def test_company_audits_list_shape(analyzed_company, live_client: LiveClient):
    audits = live_client.list_company_audits(DEFAULT_TEST_COMPANY)
    assert audits["company"] == DEFAULT_TEST_COMPANY
    assert "audits" in audits
    assert isinstance(audits["audits"], list)
    assert "audit_count" in audits
    assert audits["audit_count"] == len(audits["audits"])


def test_company_not_found_returns_404(live_client: LiveClient):
    # Session directly (not the wrapper) so raise_for_status doesn't fire
    # before we can inspect the status code.
    r = live_client._session.get(live_client._url("/api/company/psat-unknown-company-xyz"), timeout=15)
    assert r.status_code == 404, f"unknown company should 404, got {r.status_code}: {r.text}"
