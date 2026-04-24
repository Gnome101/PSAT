"""Auth enforcement + error-response contracts. Raw requests: tests need to inspect non-2xx codes."""

from __future__ import annotations

import requests


def test_analyze_without_admin_key_rejected(live_base_url: str):
    r = requests.post(
        live_base_url + "/api/analyze",
        json={"address": "0x" + "a" * 40},
        timeout=15,
    )
    assert r.status_code in (401, 403), f"unauth POST /api/analyze returned {r.status_code}: {r.text}"


def test_analyze_malformed_address_rejected(live_base_url: str, live_admin_key: str):
    # 42 chars (passes Pydantic length) but no 0x prefix — fails the handler's explicit check.
    bad_address = "ab" + "c" * 40
    assert len(bad_address) == 42 and not bad_address.startswith("0x")
    r = requests.post(
        live_base_url + "/api/analyze",
        json={"address": bad_address},
        headers={"X-PSAT-Admin-Key": live_admin_key},
        timeout=15,
    )
    assert r.status_code == 400, f"malformed address should 400, got {r.status_code}: {r.text}"


def test_unknown_job_id_returns_404(live_base_url: str):
    missing_id = "00000000-0000-0000-0000-000000000000"
    r = requests.get(live_base_url + f"/api/jobs/{missing_id}", timeout=15)
    assert r.status_code == 404, f"unknown job_id should 404, got {r.status_code}: {r.text}"


def test_unknown_run_name_returns_404(live_base_url: str):
    r = requests.get(
        live_base_url + "/api/analyses/psat-live-test-unknown-run/artifact/contract_analysis.json",
        timeout=15,
    )
    assert r.status_code == 404, f"unknown run_name should 404, got {r.status_code}: {r.text}"
