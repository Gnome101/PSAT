"""Predicate-tree evaluator — the bridge from static stage to resolver.

Takes a ``PredicateTree`` (from ``services.static.contract_analysis_pipeline.
predicates.build_predicate_tree``) and produces a ``CapabilityExpr``
describing the principal set / capability shape that gates the
function. Recursive: AND/OR nodes compose via the closed combinators
in ``capabilities.py``.

Per v6 round-5 #3 fix, dispatch order is:
  1. kind == "unsupported"           → CapabilityExpr.unsupported(reason)
  2. authority_role ∈ {reentrancy, pause, business, time} →
     conditional_universal (anyone, with the side condition)
  3. caller_authority / delegated_authority — dispatch on leaf kind:
     - membership   → adapter.enumerate(set_descriptor) → finite_set
     - equality     → resolve operand → finite_set([address])
     - external_bool→ external_check_only
     - signature_auth → signature_witness
     - comparison   → conditional_universal (caller-priority comparisons
                       are exotic; mostly time-gates, already handled)

Adapters are pluggable: the caller passes an ``AdapterRegistry`` (week 5
deliverable). Without adapters, membership leaves return finite_set with
quality=lower_bound and empty members — the structural skeleton is
correct, just unfilled.
"""

from __future__ import annotations

from typing import Any, Protocol

from services.static.contract_analysis_pipeline.predicate_types import (
    LeafPredicate,
    PredicateTree,
    SetDescriptor,
)

from .capabilities import (
    CapabilityExpr,
    Condition,
    ExternalCheck,
    intersect,
    negate,
    union,
)

# ---------------------------------------------------------------------------
# Adapter protocol (placeholder — week-5 fully-typed registry replaces this)
# ---------------------------------------------------------------------------


class SetAdapter(Protocol):
    """Minimal adapter interface for week-4. The full SetAdapter
    Protocol (with EvaluationContext, matches/enumerate/membership)
    lands in week 5 alongside the AccessControl + Safe adapters."""

    def enumerate(self, descriptor: SetDescriptor, contract_address: str | None) -> CapabilityExpr: ...


class _NullAdapter:
    """Fallback when no real adapter is registered. Returns
    finite_set(empty, lower_bound) — the structural skeleton without
    a populated members list."""

    def enumerate(self, descriptor: SetDescriptor, contract_address: str | None) -> CapabilityExpr:
        return CapabilityExpr.finite_set(
            [],
            quality="lower_bound",
            confidence="partial",
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class EvaluationContext:
    """Resolver-side context for the simple (week-4) evaluator path.

    The full week-5 ``EvaluationContext`` lives in
    ``services.resolution.adapters`` and carries chain/RPC/repos.
    Use ``evaluate_tree_with_registry`` to dispatch via that fuller
    context.
    """

    def __init__(
        self,
        *,
        contract_address: str | None = None,
        adapter: SetAdapter | None = None,
        block: int | None = None,
        state_var_values: dict[str, str] | None = None,
    ) -> None:
        self.contract_address = contract_address
        self.adapter: SetAdapter = adapter or _NullAdapter()
        self.block = block
        # Persisted state-variable values keyed by storage-var name.
        # Used by ``_resolve_equality_principal`` to enumerate Ownable
        # _owner / authority / etc. into concrete addresses.
        self.state_var_values = state_var_values or {}


def evaluate_tree_with_registry(
    tree: PredicateTree | None,
    registry: Any,  # adapters.AdapterRegistry — typed loosely to avoid circular import
    ctx: Any,  # adapters.EvaluationContext
) -> CapabilityExpr:
    """Like ``evaluate_tree`` but routes membership leaves through the
    week-5 AdapterRegistry. The registry's ``enumerate(descriptor,
    ctx)`` returns a CapabilityExpr that may be a populated
    finite_set, threshold_group, external_check_only, or
    unsupported(no_adapter)."""

    class _RegistryBackedAdapter:
        def enumerate(self, descriptor, contract_address):  # noqa: ARG002
            return registry.enumerate(descriptor, ctx)

    legacy_ctx = EvaluationContext(
        contract_address=getattr(ctx, "contract_address", None),
        adapter=_RegistryBackedAdapter(),
        block=getattr(ctx, "block", None),
        state_var_values=getattr(ctx, "state_var_values", None),
    )
    return evaluate_tree(tree, legacy_ctx)


def evaluate_tree(
    tree: PredicateTree | None,
    ctx: EvaluationContext | None = None,
) -> CapabilityExpr:
    """Walk a PredicateTree and return its CapabilityExpr.

    None or empty tree → conditional_universal with no conditions
    (i.e., 'public' / no gating).

    AND / OR nodes recurse via closed combinators.

    LEAF nodes dispatch per the v6 order: unsupported first, then
    side-condition roles, then caller/delegated auth.
    """
    if ctx is None:
        ctx = EvaluationContext()
    if tree is None:
        return CapabilityExpr.conditional_universal(
            Condition(kind="business", description="no gating"),
        )
    op = tree.get("op")
    if op == "LEAF":
        leaf = tree.get("leaf")
        if leaf is None:
            return CapabilityExpr.unsupported("empty_leaf")
        return _evaluate_leaf(leaf, ctx)
    children = tree.get("children") or []
    if not children:
        return CapabilityExpr.unsupported("empty_branch")
    evaluated = [evaluate_tree(c, ctx) for c in children]
    if op == "AND":
        result = evaluated[0]
        for child in evaluated[1:]:
            result = intersect(result, child)
        return result
    if op == "OR":
        result = evaluated[0]
        for child in evaluated[1:]:
            result = union(result, child)
        return result
    return CapabilityExpr.unsupported(f"unknown_op_{op}")


# ---------------------------------------------------------------------------
# Per-leaf dispatch
# ---------------------------------------------------------------------------


def _evaluate_leaf(leaf: LeafPredicate, ctx: EvaluationContext) -> CapabilityExpr:
    # 0. unsupported is structural — check first (round-5 #3 fix).
    if leaf.get("kind") == "unsupported":
        return CapabilityExpr.unsupported(leaf.get("unsupported_reason") or "unsupported")

    # 1. Non-authority leaves go to side-conditions.
    role = leaf.get("authority_role")
    if role in ("reentrancy", "pause", "business", "time"):
        cond = _condition_from_leaf(leaf)
        return CapabilityExpr.conditional_universal(cond)

    # 2. caller_authority / delegated_authority — dispatch on kind.
    kind = leaf.get("kind")
    operator = leaf.get("operator")

    if kind == "membership":
        descriptor = leaf.get("set_descriptor")
        if descriptor is None:
            return CapabilityExpr.unsupported("membership_without_descriptor")
        cap = ctx.adapter.enumerate(descriptor, ctx.contract_address)
        if operator == "falsy":
            cap = negate(cap)
        return cap

    if kind == "equality":
        if operator in ("eq", "ne"):
            base = _resolve_equality_principal(leaf, ctx)
            return base if operator == "eq" else negate(base)
        return CapabilityExpr.unsupported(f"equality_op_{operator}_unsupported")

    if kind == "external_bool":
        return _resolve_external_bool(leaf)

    if kind == "signature_auth":
        signer = _resolve_signer_from_leaf(leaf)
        return CapabilityExpr.signature_witness(signer)

    if kind == "comparison":
        # Caller-authority comparisons are exotic; treat as conditional.
        cond = _condition_from_leaf(leaf)
        return CapabilityExpr.conditional_universal(cond)

    return CapabilityExpr.unsupported(f"unknown_leaf_kind_{kind}")


# ---------------------------------------------------------------------------
# Operand resolution helpers
# ---------------------------------------------------------------------------


def _resolve_equality_principal(
    leaf: LeafPredicate,
    ctx: EvaluationContext | None = None,
) -> CapabilityExpr:
    """``msg.sender == X`` — resolve X to a CapabilityExpr.

    Per v6 round-5 #2: when X is a function parameter, the result is
    conditional_universal(self_service) — anyone may call but only
    for their own data. State-var operands consult
    ``ctx.state_var_values`` (populated from ``controller_values``);
    when the value isn't there we emit the lower_bound placeholder so
    the FE can still render 'guarded by X' even without enumeration."""
    operands = leaf.get("operands") or []
    other = [op for op in operands if op["source"] not in ("msg_sender", "tx_origin", "signature_recovery")]
    if len(other) != 1:
        return CapabilityExpr.unsupported("equality_operand_ambiguous")
    op = other[0]

    src = op["source"]
    if src == "constant":
        val = op.get("constant_value")
        if isinstance(val, str) and val.startswith("0x") and len(val) == 42:
            return CapabilityExpr.finite_set([val])
        return CapabilityExpr.unsupported(f"equality_constant_non_address_{val}")

    if src == "state_variable":
        sv_name = op.get("state_variable_name")
        if ctx is not None and sv_name and sv_name in ctx.state_var_values:
            value = ctx.state_var_values[sv_name]
            if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
                return CapabilityExpr.finite_set(
                    [value],
                    quality="exact",
                    confidence="enumerable",
                )
        # Fallback: we know there's a guarding state-var but haven't
        # enumerated it yet (no ControllerValue row, or non-address
        # value). UI surfaces this as 'guarded but unresolved'.
        return CapabilityExpr.finite_set(
            [],
            quality="lower_bound",
            confidence="partial",
        )

    if src == "view_call":
        # Same as state_variable: resolved via adapter recursion.
        return CapabilityExpr.finite_set(
            [],
            quality="lower_bound",
            confidence="partial",
        )

    if src == "parameter":
        # Self-service: anyone, on their own data.
        cond = Condition(
            kind="self_service",
            description=f"caller acting on their own {op.get('parameter_name') or 'arg'}",
            parameter_index=op.get("parameter_index"),
            parameter_name=op.get("parameter_name"),
        )
        return CapabilityExpr.conditional_universal(cond)

    if src == "signature_recovery":
        # Already handled via signature_auth leaf kind, but defensive.
        return CapabilityExpr.signature_witness(CapabilityExpr.unsupported("signer_unresolved"))

    if src == "computed":
        return CapabilityExpr.unsupported(f"equality_operand_computed_{op.get('computed_kind')}")

    return CapabilityExpr.unsupported(f"equality_operand_source_{src}")


def _resolve_external_bool(leaf: LeafPredicate) -> CapabilityExpr:
    """``require(authority.canCall(...))`` — produces an
    external_check_only capability. The actual probe interface
    (call selector + target address) is deferred to the adapter
    registry in week 5."""
    check = ExternalCheck(
        target_address=None,
        target_call_selector=None,
        extra={"basis": list(leaf.get("basis", []))},
    )
    cap = CapabilityExpr.external_check_only(check)
    operator = leaf.get("operator")
    if operator == "falsy":
        cap = negate(cap)
    return cap


def _resolve_signer_from_leaf(leaf: LeafPredicate) -> CapabilityExpr:
    """For a signature_auth leaf, the principal is whoever signed.
    Find the operand that's NOT the signature_recovery source — that
    operand identifies the expected signer, which becomes a
    capability that the resolver-side check verifies the signature
    against."""
    operands = leaf.get("operands") or []
    signers = [op for op in operands if op["source"] != "signature_recovery"]
    if len(signers) != 1:
        return CapabilityExpr.unsupported("signature_signer_ambiguous")
    op = signers[0]

    if op["source"] == "state_variable":
        return CapabilityExpr.finite_set(
            [],
            quality="lower_bound",
            confidence="partial",
        )
    if op["source"] == "constant":
        val = op.get("constant_value")
        if isinstance(val, str) and val.startswith("0x") and len(val) == 42:
            return CapabilityExpr.finite_set([val])
    return CapabilityExpr.unsupported(f"signature_signer_source_{op['source']}")


def _condition_from_leaf(leaf: LeafPredicate) -> Condition:
    role = leaf.get("authority_role")
    kind: str = role if role in ("time", "pause", "reentrancy", "business") else "business"
    return Condition(
        kind=kind,  # type: ignore[arg-type]
        description=leaf.get("expression") or "",
    )
