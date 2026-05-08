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

from copy import deepcopy
from typing import Any, Protocol, cast

from eth_utils.crypto import keccak

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
    lands in week 5 alongside concrete adapters."""

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
        # Used by ``_resolve_equality_principal`` to enumerate state-variable
        # authority values into concrete addresses.
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
        # ``_outer_ctx`` exposes the full resolver ctx (session, event logs,
        # state_var_values, evaluation_stack, …) to leaf evaluators that
        # need cross-contract inlining. ``_registry`` is the AdapterRegistry
        # the recursive ``evaluate_tree_with_registry`` re-uses when it
        # spawns a child ctx for B's tree.
        _outer_ctx = ctx
        _registry = registry

        def enumerate(self, descriptor, contract_address):  # noqa: ARG002
            return registry.enumerate(descriptor, ctx)

    eval_ctx = EvaluationContext(
        contract_address=getattr(ctx, "contract_address", None),
        adapter=_RegistryBackedAdapter(),
        block=getattr(ctx, "block", None),
        state_var_values=getattr(ctx, "state_var_values", None),
    )
    return evaluate_tree(tree, eval_ctx)


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


def _has_caller_keyed_value_predicate(leaf: LeafPredicate) -> bool:
    """True iff ``leaf.set_descriptor`` carries a ``value_predicate``
    AND at least one ``key_sources`` entry is ``msg_sender`` (i.e. the
    threshold is keyed on the caller). Used to upgrade
    ``business``-flavored thresholds (PR D.1+) into finite-set
    enumerations when the adapter chain has data, while still letting
    pure-business thresholds (``amount > 1000``) fall through to
    ``conditional_universal``.
    """
    descriptor = leaf.get("set_descriptor") or {}
    if not descriptor.get("value_predicate"):
        return False
    keys = descriptor.get("key_sources") or []
    return any(k.get("source") in ("msg_sender", "tx_origin", "signature_recovery") for k in keys)


def _evaluate_leaf(leaf: LeafPredicate, ctx: EvaluationContext) -> CapabilityExpr:
    # 0. unsupported is structural — check first (round-5 #3 fix).
    if leaf.get("kind") == "unsupported":
        return CapabilityExpr.unsupported(leaf.get("unsupported_reason") or "unsupported")

    # 1. Non-authority leaves go to side-conditions — UNLESS the
    # descriptor carries a caller-keyed value_predicate (PR D.1+).
    # ``balances[msg.sender] < 10 revert`` is structurally a business
    # threshold but operationally an authority gate over the set of
    # callers whose latest mapping value satisfies the predicate.
    # When the adapter chain has data (durable indexer / on-demand
    # event replay / trace replay) we get a concrete finite_set;
    # otherwise the fallback path produces conditional_universal.
    role = leaf.get("authority_role")
    if role in ("reentrancy", "pause", "business", "time"):
        if _has_caller_keyed_value_predicate(leaf):
            descriptor = leaf.get("set_descriptor")
            if descriptor is not None:
                cap = ctx.adapter.enumerate(descriptor, ctx.contract_address)
                # Only return the enumerated capability when it has
                # at least one concrete member. Anything else
                # (``external_check_only``, ``unsupported``, empty
                # ``finite_set`` regardless of quality) means "no
                # useful data" — and a side-condition leaf's
                # description is more informative than an empty
                # principal list. Codex review #3 caught the
                # ``finite_set([], exact)`` case where a genuinely-
                # business predicate could silently lose its
                # description; gating on ``cap.members`` fixes it.
                if cap.kind == "finite_set" and cap.members:
                    return cap
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
        descriptor = leaf.get("set_descriptor")
        if descriptor is not None:
            # Cross-contract inlining: when the leaf records an exact
            # callee signature/selector, try evaluating the registry's
            # predicate_trees for that function under the original
            # caller's msg.sender. If it
            # produces a useful capability we use it; otherwise fall
            # through to the adapter-registry path which handles
            # generic event-indexed descriptors.
            inlined = _maybe_inline_cross_contract_call(leaf, descriptor, ctx)
            if inlined is not None:
                if operator == "falsy":
                    inlined = negate(inlined)
                return inlined
            if descriptor.get("kind") == "external_set":
                cap = _external_check_from_descriptor(leaf, descriptor, ctx)
            else:
                cap = ctx.adapter.enumerate(descriptor, ctx.contract_address)
                if cap.kind == "unsupported" and cap.unsupported_reason == "no_adapter":
                    cap = _external_check_from_descriptor(leaf, descriptor, ctx)
            if operator == "falsy":
                cap = negate(cap)
            return cap
        return _resolve_external_bool(leaf, ctx)

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


def _maybe_inline_cross_contract_call(
    leaf: LeafPredicate,
    descriptor: SetDescriptor,
    ctx: EvaluationContext,
) -> CapabilityExpr | None:
    """Try to resolve a delegated external-check leaf by
    evaluating the registry contract's predicate trees under the
    caller's context.

    The leaf must carry:
      * ``set_descriptor.authority_contract.address_source`` — pointing
        at the state-variable that holds the registry address.
      * ``set_descriptor.callee_signature`` or ``callee_selector`` — the
        exact registry function to inline.

    Returns:
      * a ``CapabilityExpr`` from re-evaluating B's tree under A's
        sender, OR
      * ``None`` if any precondition isn't met (no session, no
        state-var resolution, no Job for the registry, no
        predicate_trees artifact, no matching function tree, or the
        recursion guard fires) — caller falls through to the existing
        adapter path.

    The resolver carries an ``evaluation_stack`` set on the context to
    short-circuit cycles: ``(chain_id, address.lower(), function_signature)``
    is added before recursing and removed after. A repeat hit (e.g.
    A→B→A or B→B) returns ``CapabilityExpr.external_check_only`` so
    the leaf still surfaces as 'gated' even if we can't resolve.
    """
    callee_signature = descriptor.get("callee_signature")
    callee_selector = descriptor.get("callee_selector")
    if not isinstance(callee_signature, str):
        callee_signature = None
    if not isinstance(callee_selector, str):
        callee_selector = None
    if not callee_signature and not callee_selector:
        return None

    # session lives on the OUTER (adapters) context — pulled by the
    # registry-backed adapter wrapper. Fall back to None gracefully.
    outer_ctx = getattr(getattr(ctx, "adapter", None), "_outer_ctx", None)
    if outer_ctx is None:
        return None
    session = getattr(outer_ctx, "session", None)
    if session is None:
        return None

    authority_contract = descriptor.get("authority_contract") or {}
    address_source = authority_contract.get("address_source") or {}
    if address_source.get("source") != "state_variable":
        return None
    sv_name = address_source.get("state_variable_name")
    if not isinstance(sv_name, str) or not sv_name:
        return None
    state_vars = getattr(outer_ctx, "state_var_values", None) or {}
    registry_addr = state_vars.get(sv_name)
    if not isinstance(registry_addr, str) or not registry_addr.startswith("0x") or len(registry_addr) != 42:
        return None
    registry_addr = registry_addr.lower()

    chain_id = getattr(outer_ctx, "chain_id", 1)
    stack = outer_ctx.evaluation_stack if hasattr(outer_ctx, "evaluation_stack") else set()
    callee_identity = callee_signature or callee_selector or ""
    key = (chain_id, registry_addr, callee_identity)
    if key in stack:
        # Cycle: B's resolution depends on its own gate, or we've already
        # walked through this address+function in this evaluation tree.
        return CapabilityExpr.external_check_only(
            ExternalCheck(
                target_address=registry_addr,
                target_call_selector=callee_selector,
                extra={"basis": ["cycle_detected_in_cross_contract_inlining"]},
            )
        )

    # Look up the registry's semantic artifacts. If the registry address is
    # a proxy, predicate_trees live on its implementation child job.
    from db.queue import get_artifact
    from services.resolution.capability_resolver import find_analysis_job_for_address

    lookup = find_analysis_job_for_address(
        session,
        registry_addr,
        required_artifact="predicate_trees",
        completed_only=True,
    )
    if lookup is None:
        return None
    artifact = get_artifact(session, lookup.analysis_job.id, "predicate_trees")
    if not isinstance(artifact, dict):
        return None
    trees = artifact.get("trees")
    if not isinstance(trees, dict) or not trees:
        return None

    callee_tree = _tree_for_signature_or_selector(
        trees,
        callee_signature=callee_signature,
        callee_selector=callee_selector,
    )
    if callee_tree is None:
        return None

    callee_tree = _bind_callee_parameters(
        callee_tree,
        _callee_argument_operands(
            leaf,
            callee_signature=callee_signature,
            callee_selector=callee_selector,
        ),
    )

    # Build a child evaluation context targeting the registry. msg.sender
    # is preserved (the call passes through). state_var_values come
    # from B's own controller_values rows, not A's.
    from services.resolution.capability_resolver import _load_state_var_values

    state_var_values = _load_state_var_values(
        session,
        lookup.analysis_job.address or registry_addr,
        job_id=lookup.analysis_job.id,
    )
    if not state_var_values and lookup.runtime_job.id != lookup.analysis_job.id:
        state_var_values = _load_state_var_values(session, registry_addr, job_id=lookup.runtime_job.id)

    child_outer = type(outer_ctx)(
        chain_id=chain_id,
        rpc_url=getattr(outer_ctx, "rpc_url", None),
        block=getattr(outer_ctx, "block", None),
        finality_depth=getattr(outer_ctx, "finality_depth", 12),
        contract_address=registry_addr,
        event_log_repo=getattr(outer_ctx, "event_log_repo", None),
        bytecode=outer_ctx.bytecode,
        recursive_resolver=outer_ctx.recursive_resolver,
        state_var_values=state_var_values,
        session=session,
        evaluation_stack=stack | {key},
        meta=dict(outer_ctx.meta),
    )

    # Same registry-backed adapter pattern as evaluate_tree_with_registry,
    # just keyed on the child outer ctx.
    from services.resolution.adapters import AdapterRegistry as _Reg

    registry_adapters = (
        ctx.adapter._registry  # type: ignore[attr-defined]
        if hasattr(ctx.adapter, "_registry")
        else _Reg()
    )
    return evaluate_tree_with_registry(callee_tree, registry_adapters, child_outer)


def _callee_argument_operands(
    leaf: LeafPredicate,
    *,
    callee_signature: str | None,
    callee_selector: str | None,
) -> list[dict[str, Any]]:
    args: list[dict[str, Any]] = []
    for raw_operand in leaf.get("operands") or []:
        if not isinstance(raw_operand, dict):
            continue
        operand = cast(dict[str, Any], raw_operand)
        if _is_target_call_operand(operand, callee_signature=callee_signature, callee_selector=callee_selector):
            continue
        args.append(deepcopy(operand))
    return args


def _is_target_call_operand(
    operand: dict[str, Any],
    *,
    callee_signature: str | None,
    callee_selector: str | None,
) -> bool:
    if operand.get("source") != "external_call":
        return False
    op_sig = operand.get("callee_signature")
    if callee_signature and isinstance(op_sig, str) and op_sig == callee_signature:
        return True
    op_selector = operand.get("callee_selector")
    if callee_selector and isinstance(op_selector, str) and op_selector == callee_selector:
        return True
    op_callee = operand.get("callee")
    if callee_signature and isinstance(op_callee, str) and callee_signature.startswith(f"{op_callee}("):
        return True
    return False


def _bind_callee_parameters(tree: PredicateTree, call_args: list[dict[str, Any]]) -> PredicateTree:
    bound = _bind_value(deepcopy(tree), call_args)
    return cast(PredicateTree, bound) if isinstance(bound, dict) else tree


def _bind_value(value: Any, call_args: list[dict[str, Any]]) -> Any:
    if isinstance(value, list):
        return [_bind_value(item, call_args) for item in value]
    if not isinstance(value, dict):
        return value
    if value.get("source") == "parameter":
        idx = value.get("parameter_index")
        if isinstance(idx, int) and 0 <= idx < len(call_args):
            return deepcopy(call_args[idx])
    out = {k: _bind_value(v, call_args) for k, v in value.items()}
    leaf = out.get("leaf")
    if isinstance(leaf, dict):
        _promote_bound_caller_leaf(leaf)
    return out


def _promote_bound_caller_leaf(leaf: dict[str, Any]) -> None:
    if leaf.get("authority_role") != "business":
        return
    if leaf.get("kind") not in {"equality", "membership", "external_bool"}:
        return
    operands = leaf.get("operands") or []
    key_sources = (leaf.get("set_descriptor") or {}).get("key_sources") or []
    has_caller = any(_is_caller_source(item) for item in [*operands, *key_sources] if isinstance(item, dict))
    if has_caller:
        leaf["authority_role"] = "delegated_authority"
        leaf["references_msg_sender"] = True


def _is_caller_source(item: dict[str, Any]) -> bool:
    return item.get("source") in {"msg_sender", "tx_origin", "signature_recovery"}


def _tree_for_signature_or_selector(
    trees: dict[str, Any],
    *,
    callee_signature: str | None,
    callee_selector: str | None,
) -> PredicateTree | None:
    """Find a predicate tree by exact ABI signature or selector."""
    if callee_signature and callee_signature in trees:
        tree = trees[callee_signature]
        return cast(PredicateTree, tree) if isinstance(tree, dict) else None
    if callee_selector:
        for signature, tree in trees.items():
            if not isinstance(signature, str):
                continue
            if _selector_for_signature(signature) == callee_selector and isinstance(tree, dict):
                return cast(PredicateTree, tree)
    return None


def _selector_for_signature(signature: str) -> str | None:
    if "(" not in signature or not signature.endswith(")"):
        return None
    return "0x" + keccak(text=signature).hex()[:8]


def _resolve_external_bool(leaf: LeafPredicate, ctx: EvaluationContext | None = None) -> CapabilityExpr:
    """``require(authority.check(...))`` — produces an
    external_check_only capability."""
    selector = None
    for op in leaf.get("operands") or []:
        if op.get("source") == "external_call":
            raw = op.get("callee_selector")
            selector = raw if isinstance(raw, str) else selector
    check = ExternalCheck(
        target_address=None,
        target_call_selector=selector,
        extra={"basis": list(leaf.get("basis", []))},
    )
    cap = CapabilityExpr.external_check_only(check)
    operator = leaf.get("operator")
    if operator == "falsy":
        cap = negate(cap)
    return cap


def _external_check_from_descriptor(
    leaf: LeafPredicate,
    descriptor: SetDescriptor,
    ctx: EvaluationContext,
) -> CapabilityExpr:
    target_address = _target_address_from_descriptor(descriptor, ctx)
    selector = descriptor.get("callee_selector")
    check = ExternalCheck(
        target_address=target_address,
        target_call_selector=selector if isinstance(selector, str) else None,
        extra={
            "basis": list(leaf.get("basis", [])),
            "callee_function": descriptor.get("callee_function"),
            "callee_signature": descriptor.get("callee_signature"),
            "topic0": _first_hint_value(descriptor, "topic0"),
            "direction": _first_hint_value(descriptor, "direction"),
        },
    )
    return CapabilityExpr.external_check_only(check)


def _target_address_from_descriptor(descriptor: SetDescriptor, ctx: EvaluationContext) -> str | None:
    authority = descriptor.get("authority_contract") or {}
    raw = authority.get("address")
    if isinstance(raw, str) and raw.startswith("0x") and len(raw) == 42:
        return raw.lower()
    source = authority.get("address_source") or {}
    if source.get("source") == "state_variable":
        name = source.get("state_variable_name")
        value = ctx.state_var_values.get(name) if isinstance(name, str) else None
        if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
            return value.lower()
    return ctx.contract_address.lower() if ctx.contract_address else None


def _first_hint_value(descriptor: SetDescriptor, key: str) -> Any:
    hints = descriptor.get("enumeration_hint") or []
    for hint in hints:
        value = hint.get(key)
        if value is not None:
            return value
    return None


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
