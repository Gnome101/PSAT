"""Company-scoped reads. ``GET /api/company/{name}`` is the 700+-line aggregator the company page hangs off."""

from __future__ import annotations

from tests.live.conftest import DEFAULT_TEST_COMPANY, LiveClient


def test_company_overview_basic_shape(analyzed_company, live_client: LiveClient):
    overview = live_client.company_overview(DEFAULT_TEST_COMPANY)
    assert isinstance(overview, dict)
    # Loose membership check — schema evolves; catches total regression without flapping on additions.
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
    r = live_client._session.get(live_client._url("/api/company/psat-unknown-company-xyz"), timeout=15)
    assert r.status_code == 404, f"unknown company should 404, got {r.status_code}: {r.text}"
