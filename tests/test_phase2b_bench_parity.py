"""Phase 2b gate — legacy `external_call_guards` field stays in
coverage-parity with `sinks` across the 7 bench contracts.

Loads each bench contract's `contract_analysis` artifact from the
local Postgres at :5433 (docker-compose stack). For every privileged
function, asserts:

1. `external_call_guards` is non-empty iff at least one `sinks`
   record has `kind == "caller_external_call"`. Monotonic coverage.
2. The projection's `(target_state_var, method)` pairs are a
   subset-equal of the `sinks` pairs. No drift.

Skipped unless `PSAT_RUN_LOCAL_BENCH=1` — we don't want CI to touch
a running DB. Locally, run with the storage env vars exported so the
artifact resolver can reach minio:

    ARTIFACT_STORAGE_ENDPOINT=http://localhost:9000 \
    ARTIFACT_STORAGE_BUCKET=psat-artifacts \
    ARTIFACT_STORAGE_ACCESS_KEY=psat-minio \
    ARTIFACT_STORAGE_SECRET_KEY=psat-minio-secret \
    PSAT_RUN_LOCAL_BENCH=1 \
    .venv/bin/python -m pytest tests/test_phase2b_bench_parity.py -v

The test is parametrized across the seven bench contracts. Each case
fails independently so operators can see exactly which contract
drifted.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytestmark = pytest.mark.skipif(
    os.environ.get("PSAT_RUN_LOCAL_BENCH") != "1",
    reason="Requires the local postgres-on-5433 bench stack. Set PSAT_RUN_LOCAL_BENCH=1 to run.",
)

# (label, target address — the impl when the contract is a proxy,
# since that's where the real function analysis lives)
BENCH: list[tuple[str, str]] = [
    ("etherfi_lp_impl", "0x83bc649fcdb2c8da146b2154a559ddedf937ef12"),
    ("morpho_registry", "0x3696c5eae4a7ffd04ea163564571e9cd8ed9364e"),
    ("morpho_blue", "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"),
    ("wstETH", "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"),
    ("accounting_impl", "0xd43a3e984071f40d5d840f60708af0e9526785df"),
    ("sky", "0x56072c95faa701256059aa122697b133aded9279"),
    ("usdc_impl", "0x43506849d7c04f9138d1a2050bbf3a0c054402dd"),
]


@pytest.fixture(autouse=True)
def _restore_storage_env(monkeypatch):
    """The conftest's autouse `delenv` fixture wipes ARTIFACT_STORAGE_*
    so unit tests don't accidentally hit prod storage. This bench test
    deliberately reads the REAL dev minio to diff legacy-vs-new fields
    on analyzed artifacts, so we re-inject the env from os.environ
    (the user is expected to export the storage creds before running
    this bench via PSAT_RUN_LOCAL_BENCH=1) and reset the client cache."""
    import os as _os

    from db.storage import reset_client_cache

    for key in (
        "ARTIFACT_STORAGE_ENDPOINT",
        "ARTIFACT_STORAGE_BUCKET",
        "ARTIFACT_STORAGE_ACCESS_KEY",
        "ARTIFACT_STORAGE_SECRET_KEY",
    ):
        value = _os.environ.get(key) or _os.environ.get("TEST_" + key)
        # Fall back to sensible local dev defaults so the user doesn't
        # need to re-paste the same docker-compose creds in every run.
        if not value:
            value = {
                "ARTIFACT_STORAGE_ENDPOINT": "http://localhost:9000",
                "ARTIFACT_STORAGE_BUCKET": "psat-artifacts",
                "ARTIFACT_STORAGE_ACCESS_KEY": "psat-minio",
                "ARTIFACT_STORAGE_SECRET_KEY": "psat-minio-secret",
            }[key]
        monkeypatch.setenv(key, value)
    reset_client_cache()
    yield
    reset_client_cache()


def _load_analysis(address: str) -> dict[str, Any] | None:
    from sqlalchemy import select

    from db.models import Artifact, Job, SessionLocal
    from db.queue import _artifact_row_to_value

    with SessionLocal() as s:
        a = s.execute(
            select(Artifact)
            .join(Job, Job.id == Artifact.job_id)
            .where(Job.address == address, Artifact.name == "contract_analysis")
            .order_by(Artifact.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if a is None:
            return None
        value = _artifact_row_to_value(a)
        # `contract_analysis` is always a top-level dict — narrow away
        # the list|str alternatives pyright sees on `_artifact_row_to_value`.
        return value if isinstance(value, dict) else None


@pytest.mark.parametrize("label,address", BENCH, ids=[b[0] for b in BENCH])
def test_external_call_guards_projection_matches_sinks(label: str, address: str):
    """For each privileged function, the derived `external_call_guards`
    list must carry the same (target, method) pairs as the
    `caller_external_call` sinks. No drift allowed."""
    analysis = _load_analysis(address)
    if analysis is None:
        pytest.skip(f"No contract_analysis artifact for {label} at {address} — re-run the bench first")

    priv = analysis.get("access_control", {}).get("privileged_functions", [])
    drift: list[str] = []
    for fn in priv:
        sinks = fn.get("sinks") or []
        legacy = fn.get("external_call_guards") or []

        # Sinks projected to (target, method)
        sink_pairs = {
            (s.get("external_target_state_var", ""), s.get("external_method", ""))
            for s in sinks
            if s.get("kind") == "caller_external_call"
        }
        legacy_pairs = {(g.get("target_state_var", ""), g.get("method", "")) for g in legacy}

        # Monotonic: if sinks say the function has an external call guard,
        # the legacy list must say so too (since we derive one from the other).
        if sink_pairs and not legacy_pairs:
            drift.append(f"  {fn['function']}: sinks have {sink_pairs}, legacy empty")
            continue
        # Subset-equal: no (target,method) in legacy that didn't come from a sink.
        extra = legacy_pairs - sink_pairs
        if extra:
            drift.append(f"  {fn['function']}: legacy has {extra} not in sinks")

    assert not drift, f"{label} drifted:\n" + "\n".join(drift)


@pytest.mark.parametrize("label,address", BENCH, ids=[b[0] for b in BENCH])
def test_sinks_field_populated_when_guards_present(label: str, address: str):
    """Structural invariant: any function the legacy field flags as
    having external_call_guards must also have a caller_external_call
    sink. Ensures retiring the legacy detector doesn't lose coverage."""
    analysis = _load_analysis(address)
    if analysis is None:
        pytest.skip(f"No contract_analysis artifact for {label} at {address}")
    priv = analysis.get("access_control", {}).get("privileged_functions", [])
    regressions: list[str] = []
    for fn in priv:
        legacy = fn.get("external_call_guards") or []
        sinks = fn.get("sinks") or []
        if legacy and not any(s.get("kind") == "caller_external_call" for s in sinks):
            regressions.append(f"  {fn['function']}: legacy has {len(legacy)} guards, no sinks")
    assert not regressions, f"{label} coverage regression:\n" + "\n".join(regressions)
