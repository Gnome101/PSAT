"""Live integration tests for CORS enforcement.

The PSAT_SITE_ORIGIN gate is what stops a malicious origin from making
authenticated browser requests against the API on behalf of a logged-in
operator. Per the pr-preview workflow, every preview env is deployed
with ``PSAT_SITE_ORIGIN=https://psat-pr-{N}.fly.dev`` — i.e. the same
host as ``PSAT_LIVE_URL`` itself.

Uses raw ``requests`` everywhere because:
    1. CORS is browser-enforced based on response headers, not request
       behavior; the wrapper's session-level admin-key header is irrelevant
       and could mask the actual middleware response.
    2. The wrapper raises for status, but a missing CORS header is the
       correct (200) response — it just lacks ``Access-Control-Allow-Origin``.
"""

from __future__ import annotations

from urllib.parse import urlparse

import pytest
import requests

ATTACKER_ORIGIN = "https://attacker.example"


def _expected_allowed_origin(live_base_url: str) -> str:
    """The preview env always sets PSAT_SITE_ORIGIN to its own host, so the
    expected allowed origin equals the base URL stripped to scheme + host."""
    parsed = urlparse(live_base_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def test_cors_rejects_attacker_origin(live_base_url: str):
    """Preflight from an unauthorized origin should not echo the origin
    or return ``*``. Some misconfigurations would echo any incoming
    origin via ``Access-Control-Allow-Origin: <origin>`` — the test
    asserts the response either omits the header entirely or returns a
    non-permissive value."""
    r = requests.options(
        live_base_url + "/api/stats",
        headers={
            "Origin": ATTACKER_ORIGIN,
            "Access-Control-Request-Method": "GET",
        },
        timeout=15,
    )
    # CORS preflight typically returns 200 either way — the gate is in
    # the response header, not the status code.
    allowed = r.headers.get("Access-Control-Allow-Origin")
    assert allowed not in (ATTACKER_ORIGIN, "*"), (
        f"CORS echoed attacker origin: ACAO={allowed!r}; "
        f"PSAT_SITE_ORIGIN may be misconfigured (set to '*' or echoing arbitrary origins)"
    )


def test_cors_allows_configured_origin(live_base_url: str):
    """The preview's own origin should be in the allow list (set by
    pr-preview.yml). Skips when running against a non-preview URL — local
    runs typically don't set PSAT_SITE_ORIGIN."""
    expected = _expected_allowed_origin(live_base_url)
    if not expected.endswith(".fly.dev"):
        pytest.skip(
            f"base URL {live_base_url} is not a Fly preview; "
            "PSAT_SITE_ORIGIN is only guaranteed for preview deployments"
        )

    r = requests.options(
        live_base_url + "/api/stats",
        headers={
            "Origin": expected,
            "Access-Control-Request-Method": "GET",
        },
        timeout=15,
    )
    allowed = r.headers.get("Access-Control-Allow-Origin")
    assert allowed == expected, (
        f"expected ACAO={expected!r}, got {allowed!r}; PSAT_SITE_ORIGIN is "
        "either unset or doesn't include the preview's own origin"
    )
