"""Benchmarks for high-impact API hotspots (issues #1-6 in the perf review).

Seeds a realistic-shape protocol (50 contracts × 50 effective functions
each, control graph nodes/edges, balances, upgrade events, principal
labels) and times the three offending endpoints while counting SQL
statements via the SQLAlchemy ``before_cursor_execute`` event.

Run:
    set -a; source .env; set +a
    uv run pytest tests/test_api_perf_benchmark.py -s -m "not live"

The ``-s`` flag is what surfaces the printed result table.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager

import pytest
from sqlalchemy import event, text

from db.models import (
    Artifact,
    Contract,
    ContractBalance,
    ContractSummary,
    ControlGraphEdge,
    ControlGraphNode,
    ControllerValue,
    EffectiveFunction,
    FunctionPrincipal,
    Job,
    JobStage,
    JobStatus,
    PrincipalLabel,
    Protocol,
    UpgradeEvent,
)
from tests.conftest import requires_postgres

PROTOCOL_NAME = "perftest"
N_CONTRACTS = 50
N_FUNCTIONS_PER_CONTRACT = 50
N_PRINCIPALS_PER_FUNCTION = 2
N_NODES_PER_CONTRACT = 6
N_EDGES_PER_CONTRACT = 8


def _addr(seed: int) -> str:
    """Deterministic 0x-prefixed 20-byte address."""
    return "0x" + format(seed, "040x")


def _wipe_perf_data(session) -> None:
    """Remove rows the benchmark may have left behind on the shared test DB.

    The standard ``db_session`` fixture only cleans monitoring + protocol
    tables — Job/Contract rows from prior benchmark runs would skew SQL
    counts. Clean by Protocol name (cascades to AuditReport, Contract via
    SET NULL on contracts.protocol_id, Jobs via SET NULL on jobs.protocol_id)
    plus a sweep of the company-tagged jobs.
    """
    session.execute(
        text("DELETE FROM artifacts WHERE job_id IN (SELECT id FROM jobs WHERE company = :c)"),
        {"c": PROTOCOL_NAME},
    )
    session.execute(
        text(
            "DELETE FROM contract_dependencies WHERE contract_id IN "
            "(SELECT id FROM contracts WHERE protocol_id IN "
            "(SELECT id FROM protocols WHERE name = :n))"
        ),
        {"n": PROTOCOL_NAME},
    )
    session.execute(
        text("DELETE FROM contracts WHERE protocol_id IN (SELECT id FROM protocols WHERE name = :n)"),
        {"n": PROTOCOL_NAME},
    )
    session.execute(text("DELETE FROM jobs WHERE company = :c"), {"c": PROTOCOL_NAME})
    session.execute(text("DELETE FROM protocols WHERE name = :n"), {"n": PROTOCOL_NAME})
    session.commit()


@pytest.fixture()
def seeded(db_session):
    _wipe_perf_data(db_session)

    protocol = Protocol(name=PROTOCOL_NAME, chains=["ethereum"])
    db_session.add(protocol)
    db_session.flush()

    jobs: list[Job] = []
    contracts: list[Contract] = []

    for i in range(N_CONTRACTS):
        addr = _addr(i + 1)
        job = Job(
            id=uuid.uuid4(),
            address=addr,
            company=PROTOCOL_NAME,
            name=f"perf_{i:03d}",
            status=JobStatus.completed,
            stage=JobStage.done,
            request={"chain": "ethereum"},
            protocol_id=protocol.id,
        )
        db_session.add(job)
        db_session.flush()
        jobs.append(job)

        contract = Contract(
            job_id=job.id,
            protocol_id=protocol.id,
            address=addr,
            chain="ethereum",
            contract_name=f"PerfContract_{i:03d}",
            source_verified=True,
            is_proxy=False,
            rank_score=float(N_CONTRACTS - i),
            discovery_sources=["seed"],
        )
        db_session.add(contract)
        db_session.flush()
        contracts.append(contract)

        db_session.add(
            ContractSummary(
                contract_id=contract.id,
                control_model="role",
                is_upgradeable=False,
                is_pausable=True,
                has_timelock=False,
                risk_level="low",
                is_factory=False,
                source_verified=True,
                standards=["ERC20"],
            )
        )

        # Two balance rows per contract (native + one ERC20)
        db_session.add(
            ContractBalance(
                contract_id=contract.id,
                token_address=None,
                token_symbol="ETH",
                token_name="Ether",
                decimals=18,
                raw_balance="1000000000000000000",
                usd_value=2500.0,
                price_usd=2500.0,
            )
        )

        # Effective functions + principals
        ef_rows = []
        for f in range(N_FUNCTIONS_PER_CONTRACT):
            ef = EffectiveFunction(
                contract_id=contract.id,
                function_name=f"fn_{f:03d}",
                selector=f"0x{f:08x}",
                abi_signature=f"fn_{f:03d}(uint256,address)",
                effect_labels=["asset_pull"] if f % 5 == 0 else [],
                effect_targets=[],
                action_summary=f"action {f}",
                authority_public=False,
                authority_roles=[],
            )
            db_session.add(ef)
            ef_rows.append(ef)
        db_session.flush()

        for ef in ef_rows:
            for p in range(N_PRINCIPALS_PER_FUNCTION):
                db_session.add(
                    FunctionPrincipal(
                        function_id=ef.id,
                        address=_addr(10000 + p * 100 + (i % 50)),
                        resolved_type="safe" if p == 0 else "eoa",
                        origin=f"controller_{p}",
                        principal_type="direct_owner" if p == 0 else "controller",
                        details={"k": "v"},
                    )
                )

        # Control graph (nodes + edges)
        for n in range(N_NODES_PER_CONTRACT):
            db_session.add(
                ControlGraphNode(
                    contract_id=contract.id,
                    address=_addr(20000 + n + i * 10),
                    node_type="contract" if n % 2 == 0 else "principal",
                    resolved_type="safe" if n % 3 == 0 else "eoa",
                    label=f"label_{n}",
                    contract_name=f"Ctl_{n}",
                    depth=n,
                    analyzed=False,
                    details={},
                )
            )
        for e in range(N_EDGES_PER_CONTRACT):
            db_session.add(
                ControlGraphEdge(
                    contract_id=contract.id,
                    from_node_id=f"address:{_addr(20000 + (e % N_NODES_PER_CONTRACT) + i * 10)}",
                    to_node_id=f"address:{addr}",
                    relation="safe_owner" if e % 4 == 0 else "controller_value",
                    label=f"edge_{e}",
                    source_controller_id=f"src_{e}",
                    notes=[],
                )
            )

        # Controller values
        for cv_i in range(3):
            db_session.add(
                ControllerValue(
                    contract_id=contract.id,
                    controller_id=f"owner_{cv_i}",
                    value=_addr(30000 + cv_i),
                    resolved_type="safe",
                    source="storage",
                    block_number=1000 + cv_i,
                    details={},
                    observed_via="rpc",
                )
            )

        # Upgrade events
        db_session.add(
            UpgradeEvent(
                contract_id=contract.id,
                proxy_address=addr,
                old_impl=None,
                new_impl=_addr(40000 + i),
                block_number=1000 + i,
                tx_hash=f"0x{i:064x}",
            )
        )

        # Principal labels
        for pl in range(2):
            db_session.add(
                PrincipalLabel(
                    contract_id=contract.id,
                    address=_addr(50000 + pl + i * 10),
                    label=f"plabel_{pl}",
                    display_name=f"Display {pl}",
                    resolved_type="safe",
                    labels=["governance"],
                    confidence="high",
                    details={},
                    graph_context=[],
                )
            )

        # contract_analysis artifact (small JSON to mimic real shape)
        db_session.add(
            Artifact(
                job_id=job.id,
                name="contract_analysis",
                data={
                    "subject": {"name": f"PerfContract_{i:03d}"},
                    "summary": "perf summary",
                },
            )
        )
        db_session.add(
            Artifact(
                job_id=job.id,
                name="contract_flags",
                data={"is_proxy": False},
            )
        )
        db_session.add(
            Artifact(
                job_id=job.id,
                name="dependencies",
                data={"deps": []},
            )
        )

    db_session.commit()
    yield {"protocol": protocol, "jobs": jobs, "contracts": contracts}
    _wipe_perf_data(db_session)


class _QueryCounter:
    def __init__(self) -> None:
        self.count: int = 0
        self.elapsed_ms: float = 0.0

    def __call__(self, conn, cursor, statement, params, context, executemany):
        self.count += 1


@contextmanager
def _measure(engine):
    counter = _QueryCounter()
    event.listen(engine, "before_cursor_execute", counter)
    t0 = time.perf_counter()
    try:
        yield counter
    finally:
        counter.elapsed_ms = (time.perf_counter() - t0) * 1000
        event.remove(engine, "before_cursor_execute", counter)


def _run(api_client, db_session, label: str, fn) -> dict:
    # Warm-up call so connection-pool / planner caches don't unfairly punish
    # the first measurement; we measure on the second hit.
    fn()
    db_session.commit()
    db_session.expire_all()
    with _measure(db_session.get_bind()) as counter:
        resp = fn()
    assert resp.status_code == 200, f"{label}: {resp.status_code} {resp.text[:200]}"
    return {"label": label, "queries": counter.count, "elapsed_ms": counter.elapsed_ms}


@requires_postgres
def test_benchmark_high_impact_endpoints(seeded, api_client, db_session):
    results: list[dict] = []

    results.append(
        _run(
            api_client,
            db_session,
            "GET /api/company/{name}",
            lambda: api_client.get(f"/api/company/{PROTOCOL_NAME}"),
        )
    )

    results.append(
        _run(
            api_client,
            db_session,
            "GET /api/analyses",
            lambda: api_client.get("/api/analyses"),
        )
    )

    sample_run = "perf_000"
    results.append(
        _run(
            api_client,
            db_session,
            "GET /api/analyses/{run_name}",
            lambda: api_client.get(f"/api/analyses/{sample_run}"),
        )
    )

    print()
    print("=" * 78)
    print(f"{'endpoint':<40} {'queries':>10} {'wall_ms':>10}")
    print("-" * 78)
    for r in results:
        print(f"{r['label']:<40} {r['queries']:>10} {r['elapsed_ms']:>10.1f}")
    print("=" * 78)

    out_path = os.environ.get("PSAT_BENCH_OUT")
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

    # Sanity assertions: every endpoint must return data.
    overview = api_client.get(f"/api/company/{PROTOCOL_NAME}").json()
    assert overview["contract_count"] == N_CONTRACTS
    analyses = api_client.get("/api/analyses").json()
    assert len([a for a in analyses if a.get("company") == PROTOCOL_NAME]) == N_CONTRACTS
    detail = api_client.get(f"/api/analyses/{sample_run}").json()
    assert detail["run_name"] == sample_run
    assert "effective_permissions" in detail
    assert len(detail["effective_permissions"]["functions"]) == N_FUNCTIONS_PER_CONTRACT
