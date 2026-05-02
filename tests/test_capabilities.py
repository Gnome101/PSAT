"""Tests for ``CapabilityExpr`` + total combinators.

Covers:
  - Factory constructors produce well-formed values
  - intersect/union/negate are total over the kind cross-product
  - Quality lattice (exact/lower/upper) propagates correctly
  - Confidence lattice (enumerable/partial/check_only) propagates
  - Address canonicalization (lowercase + sort + dedup)
  - Property identities: intersect(A, A) ≡ A; intersect(A, universe) ≡ A
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.capabilities import (  # noqa: E402
    CapabilityExpr,
    Condition,
    ExternalCheck,
    intersect,
    negate,
    union,
)


# Test fixtures.
ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_C = "0xcccccccccccccccccccccccccccccccccccccccc"


# ---------------------------------------------------------------------------
# Factories + canonicalization
# ---------------------------------------------------------------------------


def test_finite_set_lowercases_and_sorts():
    cap = CapabilityExpr.finite_set([ADDR_B.upper(), ADDR_A, ADDR_A])
    assert cap.members == [ADDR_A.lower(), ADDR_B.lower()]
    assert cap.membership_quality == "exact"
    assert cap.confidence == "enumerable"


def test_threshold_group_canonicalizes():
    cap = CapabilityExpr.threshold_group(2, [ADDR_B, ADDR_A])
    assert cap.threshold == (2, [ADDR_A.lower(), ADDR_B.lower()])


def test_unsupported_carries_reason():
    cap = CapabilityExpr.unsupported("test_reason")
    assert cap.kind == "unsupported"
    assert cap.unsupported_reason == "test_reason"


# ---------------------------------------------------------------------------
# Intersect — finite × finite
# ---------------------------------------------------------------------------


def test_intersect_finite_exact_exact():
    a = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    b = CapabilityExpr.finite_set([ADDR_B, ADDR_C])
    out = intersect(a, b)
    assert out.kind == "finite_set"
    assert out.members == [ADDR_B.lower()]
    assert out.membership_quality == "exact"


def test_intersect_finite_exact_with_lower_yields_lower():
    a = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    b = CapabilityExpr.finite_set([ADDR_B, ADDR_C], quality="lower_bound")
    out = intersect(a, b)
    assert out.membership_quality == "lower_bound"


def test_intersect_finite_disjoint_yields_empty():
    a = CapabilityExpr.finite_set([ADDR_A])
    b = CapabilityExpr.finite_set([ADDR_B])
    out = intersect(a, b)
    assert out.kind == "finite_set"
    assert out.members == []


def test_intersect_idempotent():
    """``intersect(A, A) ≡ A`` for canonical finite sets."""
    a = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    out = intersect(a, a)
    assert out.kind == "finite_set"
    assert out.members == a.members


# ---------------------------------------------------------------------------
# Intersect — finite × cofinite_blacklist
# ---------------------------------------------------------------------------


def test_intersect_finite_with_blacklist():
    """``finite ∩ cofinite_blacklist`` = ``finite - blacklist``."""
    fin = CapabilityExpr.finite_set([ADDR_A, ADDR_B, ADDR_C])
    bl = CapabilityExpr.cofinite_blacklist([ADDR_B])
    out = intersect(fin, bl)
    assert out.kind == "finite_set"
    assert out.members == [ADDR_A.lower(), ADDR_C.lower()]


def test_intersect_blacklist_with_finite_commutes():
    fin = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    bl = CapabilityExpr.cofinite_blacklist([ADDR_B])
    a = intersect(fin, bl)
    b = intersect(bl, fin)
    assert a.members == b.members


# ---------------------------------------------------------------------------
# Intersect — cofinite_blacklist × cofinite_blacklist
# ---------------------------------------------------------------------------


def test_intersect_blacklists_unions_them():
    """Excluding A AND excluding B = excluding (A ∪ B)."""
    a = CapabilityExpr.cofinite_blacklist([ADDR_A])
    b = CapabilityExpr.cofinite_blacklist([ADDR_B])
    out = intersect(a, b)
    assert out.kind == "cofinite_blacklist"
    assert set(out.blacklist) == {ADDR_A.lower(), ADDR_B.lower()}


# ---------------------------------------------------------------------------
# Intersect — conditional_universal preserves
# ---------------------------------------------------------------------------


def test_intersect_finite_with_conditional_universal_keeps_set():
    fin = CapabilityExpr.finite_set([ADDR_A])
    cond = Condition(kind="time", description="after T")
    cu = CapabilityExpr.conditional_universal(cond)
    out = intersect(fin, cu)
    assert out.kind == "finite_set"
    assert out.members == [ADDR_A.lower()]
    assert any(c.kind == "time" for c in out.conditions)


# ---------------------------------------------------------------------------
# Intersect — unsupported absorbs
# ---------------------------------------------------------------------------


def test_intersect_unsupported_absorbs():
    fin = CapabilityExpr.finite_set([ADDR_A])
    u = CapabilityExpr.unsupported("opaque_control_flow")
    out = intersect(fin, u)
    assert out.kind == "unsupported"
    assert "opaque_control_flow" in out.unsupported_reason
    out2 = intersect(u, fin)
    assert out2.kind == "unsupported"


# ---------------------------------------------------------------------------
# Intersect — threshold × finite stays structural
# ---------------------------------------------------------------------------


def test_intersect_threshold_with_finite_stays_structural():
    tg = CapabilityExpr.threshold_group(2, [ADDR_A, ADDR_B, ADDR_C])
    fin = CapabilityExpr.finite_set([ADDR_A])
    out = intersect(tg, fin)
    assert out.kind == "AND"
    assert len(out.children) == 2


# ---------------------------------------------------------------------------
# Union — finite × finite
# ---------------------------------------------------------------------------


def test_union_finite_exact_exact():
    a = CapabilityExpr.finite_set([ADDR_A])
    b = CapabilityExpr.finite_set([ADDR_B])
    out = union(a, b)
    assert out.kind == "finite_set"
    assert out.members == [ADDR_A.lower(), ADDR_B.lower()]
    assert out.membership_quality == "exact"


def test_union_finite_exact_with_lower_yields_lower():
    a = CapabilityExpr.finite_set([ADDR_A])
    b = CapabilityExpr.finite_set([ADDR_B], quality="lower_bound")
    out = union(a, b)
    assert out.membership_quality == "lower_bound"


def test_union_idempotent():
    a = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    out = union(a, a)
    assert out.kind == "finite_set"
    assert out.members == a.members


# ---------------------------------------------------------------------------
# Union — cofinite_blacklist intersects
# ---------------------------------------------------------------------------


def test_union_blacklists_intersects():
    """Excluding A OR excluding B = excluding (A ∩ B)."""
    a = CapabilityExpr.cofinite_blacklist([ADDR_A, ADDR_B])
    b = CapabilityExpr.cofinite_blacklist([ADDR_B, ADDR_C])
    out = union(a, b)
    assert out.kind == "cofinite_blacklist"
    assert out.blacklist == [ADDR_B.lower()]


# ---------------------------------------------------------------------------
# Union — finite ∪ cofinite_blacklist
# ---------------------------------------------------------------------------


def test_union_finite_with_blacklist_yields_blacklist_minus_finite():
    fin = CapabilityExpr.finite_set([ADDR_A])
    bl = CapabilityExpr.cofinite_blacklist([ADDR_A, ADDR_B])
    out = union(fin, bl)
    # ADDR_A is in finite (allowed), so it's removed from the
    # remaining blacklist. Result: anyone except ADDR_B.
    assert out.kind == "cofinite_blacklist"
    assert out.blacklist == [ADDR_B.lower()]


# ---------------------------------------------------------------------------
# Union — structural OR for incompatible kinds
# ---------------------------------------------------------------------------


def test_union_threshold_with_finite_stays_structural():
    tg = CapabilityExpr.threshold_group(2, [ADDR_A, ADDR_B])
    fin = CapabilityExpr.finite_set([ADDR_C])
    out = union(tg, fin)
    assert out.kind == "OR"
    assert len(out.children) == 2


# ---------------------------------------------------------------------------
# Negate
# ---------------------------------------------------------------------------


def test_negate_finite_exact_yields_blacklist():
    fin = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    out = negate(fin)
    assert out.kind == "cofinite_blacklist"
    assert out.blacklist == [ADDR_A.lower(), ADDR_B.lower()]


def test_negate_finite_lower_bound_unsupported():
    fin = CapabilityExpr.finite_set([ADDR_A], quality="lower_bound")
    out = negate(fin)
    assert out.kind == "unsupported"
    assert "negate_partial_set" in out.unsupported_reason


def test_negate_blacklist_yields_finite():
    bl = CapabilityExpr.cofinite_blacklist([ADDR_A, ADDR_B])
    out = negate(bl)
    assert out.kind == "finite_set"
    assert out.members == [ADDR_A.lower(), ADDR_B.lower()]


def test_negate_finite_then_negate_returns_finite():
    """Double negation: negate(negate(finite_exact)) == finite_exact (canonical)."""
    fin = CapabilityExpr.finite_set([ADDR_A, ADDR_B])
    twice = negate(negate(fin))
    assert twice.kind == "finite_set"
    assert twice.members == fin.members


def test_negate_threshold_unsupported():
    tg = CapabilityExpr.threshold_group(2, [ADDR_A, ADDR_B])
    out = negate(tg)
    assert out.kind == "unsupported"
    assert "threshold_group" in out.unsupported_reason


def test_negate_unsupported_chains():
    u = CapabilityExpr.unsupported("test")
    out = negate(u)
    assert out.kind == "unsupported"
    assert "test" in out.unsupported_reason


def test_negate_de_morgan_and():
    a = CapabilityExpr.finite_set([ADDR_A])
    b = CapabilityExpr.finite_set([ADDR_B])
    and_node = CapabilityExpr.structural_and([a, b])
    out = negate(and_node)
    # NOT(A AND B) = NOT A OR NOT B (each becomes a blacklist).
    assert out.kind == "OR"
    assert len(out.children) == 2
    assert all(c.kind == "cofinite_blacklist" for c in out.children)


# ---------------------------------------------------------------------------
# Total-function discipline: nothing raises across the kind cross-product.
# ---------------------------------------------------------------------------


def _all_kinds() -> list[CapabilityExpr]:
    return [
        CapabilityExpr.finite_set([ADDR_A]),
        CapabilityExpr.finite_set([ADDR_A], quality="lower_bound"),
        CapabilityExpr.finite_set([ADDR_A], quality="upper_bound"),
        CapabilityExpr.threshold_group(2, [ADDR_A, ADDR_B]),
        CapabilityExpr.cofinite_blacklist([ADDR_A]),
        CapabilityExpr.signature_witness(CapabilityExpr.finite_set([ADDR_A])),
        CapabilityExpr.external_check_only(ExternalCheck(target_address=ADDR_A, target_call_selector="0xabcdef00")),
        CapabilityExpr.conditional_universal(Condition(kind="time")),
        CapabilityExpr.unsupported("test"),
    ]


def test_intersect_total_over_all_kinds():
    """No combination raises; every result is a typed CapabilityExpr."""
    for a in _all_kinds():
        for b in _all_kinds():
            out = intersect(a, b)
            assert out.kind in (
                "finite_set",
                "threshold_group",
                "cofinite_blacklist",
                "signature_witness",
                "external_check_only",
                "conditional_universal",
                "unsupported",
                "AND",
                "OR",
            )


def test_union_total_over_all_kinds():
    for a in _all_kinds():
        for b in _all_kinds():
            out = union(a, b)
            assert out.kind in (
                "finite_set",
                "threshold_group",
                "cofinite_blacklist",
                "signature_witness",
                "external_check_only",
                "conditional_universal",
                "unsupported",
                "AND",
                "OR",
            )


def test_negate_total_over_all_kinds():
    for a in _all_kinds():
        out = negate(a)
        # Only constraint: never raises, returns a CapabilityExpr.
        assert isinstance(out, CapabilityExpr)
