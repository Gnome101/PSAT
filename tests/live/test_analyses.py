"""Live integration tests for the analyses list + detail endpoints.

These are the two biggest read endpoints the frontend hits (the analyses
shelf on every page, plus the per-contract detail view). Both fan out
across the artifacts + relational tables, so regressions tend to show up
as missing fields rather than 500s — these tests check shape, not just
status code.
"""

from __future__ import annotations

from tests.live.conftest import WETH_ADDRESS, LiveClient


def test_analyses_list_includes_weth(analyzed_weth, live_client: LiveClient):
    entries = live_client.analyses()
    assert isinstance(entries, list)
    run_names = {e.get("run_name") for e in entries}
    assert analyzed_weth["name"] in run_names, (
        f"WETH run {analyzed_weth['name']} missing from /api/analyses (got {len(entries)} entries)"
    )


def test_analyses_list_entry_shape(analyzed_weth, live_client: LiveClient):
    entries = live_client.analyses()
    entry = next((e for e in entries if e.get("run_name") == analyzed_weth["name"]), None)
    assert entry is not None
    # Fields the frontend's list view reads — if any go missing the UI breaks silently.
    for key in ("run_name", "job_id", "address", "available_artifacts"):
        assert key in entry, f"analyses list entry missing '{key}'"
    assert isinstance(entry["available_artifacts"], list)


def test_analysis_detail_roundtrip(analyzed_weth, live_client: LiveClient):
    detail = live_client.analysis_detail(analyzed_weth["name"])
    assert detail["run_name"] == analyzed_weth["name"]
    assert detail["job_id"] == analyzed_weth["job_id"]
    assert (detail.get("address") or "").lower() == WETH_ADDRESS.lower()
    # The detail endpoint inlines a pre-selected set of artifacts (see
    # api.py:770). Make sure the list-of-available-names pass happened.
    assert isinstance(detail.get("available_artifacts"), list)


def test_analysis_detail_inlines_contract_analysis(analyzed_weth, live_client: LiveClient):
    detail = live_client.analysis_detail(analyzed_weth["name"])
    # contract_analysis is unconditionally inlined (api.py:770-779) and is
    # the primary payload the detail page renders. Check both presence and
    # that it matches the standalone artifact route.
    assert isinstance(detail.get("contract_analysis"), dict)
    via_artifact = live_client.artifact(analyzed_weth["name"], "contract_analysis")
    assert isinstance(via_artifact, dict)
    assert detail["contract_analysis"].get("subject", {}).get("name") == via_artifact.get("subject", {}).get("name")


def test_analysis_detail_contract_id_is_usable(analyzed_weth, live_client: LiveClient):
    detail = live_client.analysis_detail(analyzed_weth["name"])
    contract_id = detail.get("contract_id")
    assert isinstance(contract_id, int), (
        f"contract_id should be an int so downstream endpoints (audit_timeline, etc.) "
        f"can look it up, got {type(contract_id).__name__}"
    )
