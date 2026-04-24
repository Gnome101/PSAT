"""Live integration tests for auth enforcement + error-response contracts.

Deliberately uses the raw ``requests`` module rather than ``live_client``
for the entire file: every test here needs to inspect a non-2xx status
code, and the client wrapper raises for status. Using raw requests keeps
the expected/actual status comparison explicit and avoids try/except
gymnastics around the wrapper.
"""

from __future__ import annotations

import requests


def test_analyze_without_admin_key_rejected(live_base_url: str):
    # No X-PSAT-Admin-Key header. The admin-key dependency should short-
    # circuit before Pydantic validation even runs, so a syntactically
    # valid body still gets 401.
    r = requests.post(
        live_base_url + "/api/analyze",
        json={"address": "0x" + "a" * 40},
        timeout=15,
    )
    assert r.status_code in (401, 403), f"unauth POST /api/analyze returned {r.status_code}: {r.text}"


def test_analyze_malformed_address_rejected(live_base_url: str, live_admin_key: str):
    # 42-char value that bypasses Pydantic's length check but fails the
    # explicit startswith("0x") assertion inside the handler.
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
    # Valid UUID format but guaranteed not present in the DB.
    missing_id = "00000000-0000-0000-0000-000000000000"
    r = requests.get(live_base_url + f"/api/jobs/{missing_id}", timeout=15)
    assert r.status_code == 404, f"unknown job_id should 404, got {r.status_code}: {r.text}"


def test_unknown_run_name_returns_404(live_base_url: str):
    r = requests.get(
        live_base_url + "/api/analyses/psat-live-test-unknown-run/artifact/contract_analysis.json",
        timeout=15,
    )
    assert r.status_code == 404, f"unknown run_name should 404, got {r.status_code}: {r.text}"
