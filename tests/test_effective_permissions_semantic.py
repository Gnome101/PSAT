"""Wave 3 Track B.1 — per-kind row representation tests for the semantic
``build_effective_permissions`` + ``write_effective_function_rows``
pipeline.

Each test fabricates a ``CapabilityExpr`` directly and asserts the
resulting ``EffectiveFunction`` columns and ``FunctionPrincipal`` row
counts match the Option A table:

| kind                          | EF columns                       | FP rows |
|-------------------------------|----------------------------------|---------|
| finite_set                    | capability_expr only             | N       |
| threshold_group               | capability_expr only             | 1       |
| signature_witness(finite)     | capability_expr only             | N       |
| signature_witness(non-finite) | capability_expr only             | 0       |
| cofinite_blacklist            | capability_expr only             | 0       |
| external_check_only           | capability_expr only             | 0       |
| conditional_universal         | + conditions, status='public',   | 0       |
|                               |   authority_public=True          |         |
| unsupported                   | + status='unsupported'           | 0       |
| AND/OR irreducible composite  | full tree in capability_expr     | 0       |
| OR pure-finite                | resolver simplifies to union     | union   |

Tests don't go through Slither; they instantiate ``CapabilityExpr``
shapes directly and feed them to the writer through a SQLAlchemy
in-memory session backed by an SQLite store.

The Postgres-only column types (JSONB, ARRAY, GIN index) are swapped
to their SQLite equivalents inside ``_in_memory_session`` so the test
suite runs offline.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.types import JSON

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.policy.effective_permissions_writer import (
    _column_values_for_capability,
    _principal_rows_for_capability,
    write_effective_function_rows,
)
from services.resolution.capabilities import (
    CapabilityExpr,
    Condition,
    ExternalCheck,
)
from services.resolution.capability_resolver import capability_to_dict

# ---------------------------------------------------------------------------
# In-memory SQLite mirror of the columns the writer touches.
# Lets us assert row writes without spinning up Postgres.
# ---------------------------------------------------------------------------


_TestBase = declarative_base()


class _TContract(_TestBase):
    __tablename__ = "contracts"
    id = Column(Integer, primary_key=True)
    address = Column(String(42))


class _TEffectiveFunction(_TestBase):
    __tablename__ = "effective_functions"
    id = Column(Integer, primary_key=True)
    contract_id = Column(Integer, ForeignKey("contracts.id"))
    function_name = Column(String(255))
    selector = Column(String(10))
    abi_signature = Column(Text)
    effect_labels = Column(JSON)
    effect_targets = Column(JSON)
    action_summary = Column(Text)
    authority_public = Column(Boolean, default=False)
    authority_roles = Column(JSON)
    capability_expr = Column(JSON)
    conditions = Column(JSON)
    status = Column(String(50))
    principals = relationship(
        "_TFunctionPrincipal",
        backref="function",
        cascade="all, delete-orphan",
    )


class _TFunctionPrincipal(_TestBase):
    __tablename__ = "function_principals"
    id = Column(Integer, primary_key=True)
    function_id = Column(Integer, ForeignKey("effective_functions.id"))
    address = Column(String(42))
    resolved_type = Column(String(50))
    origin = Column(String(255))
    principal_type = Column(String(50))
    details = Column(JSON)


@pytest.fixture
def db_session(monkeypatch: pytest.MonkeyPatch):
    """In-memory SQLite session with the writer's models swapped for
    JSON-friendly equivalents."""
    engine = create_engine("sqlite:///:memory:")
    _TestBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    monkeypatch.setattr(
        "services.policy.effective_permissions_writer.EffectiveFunction",
        _TEffectiveFunction,
    )
    monkeypatch.setattr(
        "services.policy.effective_permissions_writer.FunctionPrincipal",
        _TFunctionPrincipal,
    )

    contract = _TContract(id=1, address="0x" + "1" * 40)
    session.add(contract)
    session.commit()
    yield session
    session.close()
    engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fn_record(signature: str, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "function": signature,
        "abi_signature": signature,
        "selector": "0xdeadbeef",
        "effect_labels": [],
        "effect_targets": [],
        "action_summary": "stub",
        "authority_public": False,
        "authority_roles": [],
        "controllers": [],
        "direct_owner": None,
    }
    base.update(overrides)
    return base


def _ef_row(session) -> Any:
    return session.query(_TEffectiveFunction).first()


def _principals(session) -> list[Any]:
    return list(session.query(_TFunctionPrincipal).order_by(_TFunctionPrincipal.address).all())


# ---------------------------------------------------------------------------
# finite_set
# ---------------------------------------------------------------------------


def test_finite_set_emits_n_principal_rows(db_session) -> None:
    members = [
        "0x" + "a" * 40,
        "0x" + "b" * 40,
        "0x" + "c" * 40,
    ]
    cap = CapabilityExpr.finite_set(members)

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("doThing()")],
        capability_by_function={"doThing()": cap},
    )
    db_session.commit()

    rows = _principals(db_session)
    assert len(rows) == 3
    assert {r.address for r in rows} == set(m.lower() for m in members)
    for r in rows:
        assert r.principal_type == "controller"
    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "finite_set"
    assert sorted(ef.capability_expr["members"]) == sorted(m.lower() for m in members)
    assert ef.conditions is None
    assert ef.status is None
    assert ef.authority_public is False


# ---------------------------------------------------------------------------
# threshold_group (Safe)
# ---------------------------------------------------------------------------


def test_threshold_group_emits_one_safe_row(db_session) -> None:
    signers = [f"0x{(0x10 + i):040x}" for i in range(5)]
    cap = CapabilityExpr.threshold_group(3, signers)
    safe_addr = "0x" + "5" * 40

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("manage()")],
        capability_by_function={"manage()": cap},
        safe_address_lookup={"default": safe_addr},
    )
    db_session.commit()

    rows = _principals(db_session)
    assert len(rows) == 1
    row = rows[0]
    assert row.address == safe_addr.lower()
    assert row.resolved_type == "safe"
    assert row.principal_type == "controller"
    assert row.details["threshold"] == 3
    assert len(row.details["owners"]) == 5
    assert all(o.startswith("0x") for o in row.details["owners"])

    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "threshold_group"
    assert ef.capability_expr["threshold"]["m"] == 3
    assert len(ef.capability_expr["threshold"]["signers"]) == 5


# ---------------------------------------------------------------------------
# signature_witness
# ---------------------------------------------------------------------------


def test_signature_witness_finite_emits_signer_rows(db_session) -> None:
    inner = CapabilityExpr.finite_set(["0x" + "a" * 40, "0x" + "b" * 40])
    cap = CapabilityExpr.signature_witness(inner)

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("permit()")],
        capability_by_function={"permit()": cap},
    )
    db_session.commit()

    rows = _principals(db_session)
    assert len(rows) == 2
    for r in rows:
        assert r.principal_type == "signature_witness"
        assert r.details["signer_kind"] == "finite_set"
    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "signature_witness"
    assert ef.capability_expr["signer"]["kind"] == "finite_set"


def test_signature_witness_external_emits_zero_rows(db_session) -> None:
    inner = CapabilityExpr.external_check_only(
        ExternalCheck(target_address="0x" + "9" * 40, target_call_selector="0x12345678"),
    )
    cap = CapabilityExpr.signature_witness(inner)

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("permit()")],
        capability_by_function={"permit()": cap},
    )
    db_session.commit()

    rows = _principals(db_session)
    assert len(rows) == 0
    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "signature_witness"
    assert ef.capability_expr["signer"]["kind"] == "external_check_only"


# ---------------------------------------------------------------------------
# cofinite_blacklist / external_check_only
# ---------------------------------------------------------------------------


def test_cofinite_blacklist_emits_zero_rows(db_session) -> None:
    cap = CapabilityExpr.cofinite_blacklist(["0x" + "a" * 40, "0x" + "b" * 40])

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("openCall()")],
        capability_by_function={"openCall()": cap},
    )
    db_session.commit()

    assert len(_principals(db_session)) == 0
    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "cofinite_blacklist"
    assert len(ef.capability_expr["blacklist"]) == 2
    assert ef.status is None
    assert ef.authority_public is False


def test_external_check_only_emits_zero_rows(db_session) -> None:
    check = ExternalCheck(
        target_address="0x" + "5" * 40,
        target_call_selector="0xdeadbeef",
        extra={"kind": "eip1271"},
    )
    cap = CapabilityExpr.external_check_only(check)

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("validate()")],
        capability_by_function={"validate()": cap},
    )
    db_session.commit()

    assert len(_principals(db_session)) == 0
    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "external_check_only"
    assert ef.capability_expr["check"]["target_address"] == "0x" + "5" * 40
    assert ef.capability_expr["check"]["target_call_selector"] == "0xdeadbeef"


# ---------------------------------------------------------------------------
# conditional_universal / unsupported
# ---------------------------------------------------------------------------


def test_conditional_universal_emits_zero_rows_authority_public_true(db_session) -> None:
    cap = CapabilityExpr.conditional_universal(
        Condition(kind="time", description="after 2026-01-01"),
    )

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("settle()")],
        capability_by_function={"settle()": cap},
    )
    db_session.commit()

    assert len(_principals(db_session)) == 0
    ef = _ef_row(db_session)
    assert ef.authority_public is True
    assert ef.status == "public"
    assert ef.conditions is not None
    assert len(ef.conditions) == 1
    assert ef.conditions[0]["kind"] == "time"


def test_unsupported_emits_zero_rows_status_unsupported(db_session) -> None:
    cap = CapabilityExpr.unsupported("opaque_authority_check")

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("opaque()")],
        capability_by_function={"opaque()": cap},
    )
    db_session.commit()

    assert len(_principals(db_session)) == 0
    ef = _ef_row(db_session)
    assert ef.status == "unsupported"
    assert ef.capability_expr["kind"] == "unsupported"
    assert ef.capability_expr["unsupported_reason"] == "opaque_authority_check"


# ---------------------------------------------------------------------------
# AND / OR
# ---------------------------------------------------------------------------


def test_irreducible_and_emits_zero_rows_with_tree(db_session) -> None:
    """``finite_set AND threshold_group`` doesn't reduce to a single
    kind (the resolver's ``intersect`` returns ``structural_and`` for
    that mix). The tree lives on ``capability_expr``; zero principal
    rows because no consumer should treat one leaf in isolation as
    'address can call as itself'."""
    finite = CapabilityExpr.finite_set(["0x" + "a" * 40])
    safe = CapabilityExpr.threshold_group(2, ["0x" + "b" * 40, "0x" + "c" * 40])
    cap = CapabilityExpr.structural_and([finite, safe])

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("dangerous()")],
        capability_by_function={"dangerous()": cap},
    )
    db_session.commit()

    assert len(_principals(db_session)) == 0
    ef = _ef_row(db_session)
    assert ef.capability_expr["kind"] == "AND"
    children = ef.capability_expr["children"]
    assert len(children) == 2
    assert {c["kind"] for c in children} == {"finite_set", "threshold_group"}


def test_or_pure_set_emits_union(db_session) -> None:
    """OR of two finite_sets is simplified by the resolver's ``union``
    combinator into a single finite_set covering the merged member
    list. The writer sees a finite_set and emits N rows."""
    from services.resolution.capabilities import union

    a = CapabilityExpr.finite_set(["0x" + "a" * 40, "0x" + "b" * 40])
    b = CapabilityExpr.finite_set(["0x" + "b" * 40, "0x" + "c" * 40])
    merged = union(a, b)
    assert merged.kind == "finite_set"

    write_effective_function_rows(
        db_session,
        contract_id=1,
        function_records=[_fn_record("anyOf()")],
        capability_by_function={"anyOf()": merged},
    )
    db_session.commit()

    rows = _principals(db_session)
    assert len(rows) == 3
    assert {r.address for r in rows} == {
        "0x" + "a" * 40,
        "0x" + "b" * 40,
        "0x" + "c" * 40,
    }


# ---------------------------------------------------------------------------
# Pure-function helpers
# ---------------------------------------------------------------------------


def test_principal_rows_for_capability_finite_set() -> None:
    cap_dict = capability_to_dict(CapabilityExpr.finite_set(["0x" + "a" * 40]))
    rows = _principal_rows_for_capability(cap_dict)
    assert len(rows) == 1
    assert rows[0]["principal_type"] == "controller"


def test_column_values_conditional_universal() -> None:
    cap_dict = capability_to_dict(
        CapabilityExpr.conditional_universal(Condition(kind="pause", description="paused")),
    )
    cols = _column_values_for_capability(cap_dict)
    assert cols["status"] == "public"
    assert cols["authority_public"] is True
    assert cols["conditions"] and cols["conditions"][0]["kind"] == "pause"


def test_column_values_public_or_composite() -> None:
    left = CapabilityExpr.conditional_universal(Condition(kind="business", description="initialized branch"))
    right = CapabilityExpr.conditional_universal(Condition(kind="business", description="constructor branch"))
    cap_dict = capability_to_dict(CapabilityExpr.structural_or([left, right]))

    cols = _column_values_for_capability(cap_dict)

    assert cols["status"] == "public"
    assert cols["authority_public"] is True
    assert cols["conditions"] is None


def test_column_values_unsupported() -> None:
    cap_dict = capability_to_dict(CapabilityExpr.unsupported("reason_x"))
    cols = _column_values_for_capability(cap_dict)
    assert cols["status"] == "unsupported"
    assert cols["authority_public"] is False
