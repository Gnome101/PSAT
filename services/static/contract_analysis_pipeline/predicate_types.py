"""Typed shapes for the predicate-based access analysis output.

These are the ``v2.0`` schema types — the static stage emits a
``PredicateTree`` per privileged function describing the structural
gates that admit it, and the resolver evaluates that tree. No
shape-name labels in the routing path; shape labels are diagnostic
only.

The plan is /tmp/psat-plans/generic-predicate-pipeline-v{4,5,6,7}.md.

These types live in their own module so the runtime predicate builder,
the schema, and the resolver can import them without circular deps.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

from typing_extensions import NotRequired

# ---------------------------------------------------------------------------
# Operand — where a value in a predicate originates.
# ---------------------------------------------------------------------------


OperandSource = Literal[
    "msg_sender",
    "tx_origin",
    "parameter",
    "state_variable",
    "constant",
    "view_call",  # internal call returning a value
    "external_call",  # high-level call to another contract
    "computed",  # arithmetic / hash / abi.encode result
    "block_context",  # block.timestamp / number / chainid / coinbase
    "signature_recovery",  # ecrecover / EIP-1271 isValidSignature output
    "top",  # provenance saturated (cycles, depth cap)
]


class Operand(TypedDict):
    source: OperandSource
    parameter_index: NotRequired[int | None]
    parameter_name: NotRequired[str | None]
    state_variable_name: NotRequired[str | None]
    callee: NotRequired[str | None]
    callee_args: NotRequired[list["Operand"]]
    constant_value: NotRequired[str | None]
    computed_kind: NotRequired[str | None]
    block_context_kind: NotRequired[str | None]


# ---------------------------------------------------------------------------
# SetDescriptor — instructions for a membership predicate.
# ---------------------------------------------------------------------------


SetKind = Literal[
    "mapping_membership",
    "array_contains",
    "external_set",
    "bitwise_role_flag",  # only inside unsupported leaves in v2.0
    "diamond_facet_acl",  # only inside unsupported leaves in v2.0
]


class AuthorityContract(TypedDict):
    address_source: Operand
    abi_hint: NotRequired[str | None]


RoleDomainSource = Literal[
    "compile_time_constants",
    "role_granted_history",
    "abi_declared",
    "manual_pinned",
]


class RoleDomain(TypedDict):
    parameter_index: int
    auto_seed_default_admin: bool
    sources: list[RoleDomainSource]
    recursive_role_admin_expansion: bool


class SelectorContext(TypedDict):
    selectors: list[str]


class EventHint(TypedDict):
    event_address: str
    topic0: str
    topics_to_keys: dict[int, int]
    data_to_keys: dict[int, int]
    direction: Literal["add", "remove"]
    key_value_taint: NotRequired[str | None]


class SetDescriptor(TypedDict):
    kind: SetKind
    storage_var: NotRequired[str | None]
    storage_slot: NotRequired[str | None]
    key_sources: list[Operand]
    truthy_value: NotRequired[str | None]
    enumeration_hint: NotRequired[list[EventHint]]
    authority_contract: NotRequired[AuthorityContract | None]
    role_domain: NotRequired[RoleDomain | None]
    selector_context: NotRequired[SelectorContext | None]


# ---------------------------------------------------------------------------
# LeafPredicate + PredicateTree.
# ---------------------------------------------------------------------------


LeafKind = Literal[
    "membership",
    "equality",
    "comparison",
    "external_bool",
    "signature_auth",
    "unsupported",
]

LeafOperator = Literal[
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "truthy",
    "falsy",
]

AuthorityRole = Literal[
    "caller_authority",
    "delegated_authority",
    "time",
    "reentrancy",
    "pause",
    "business",
]


Confidence = Literal["high", "medium", "low"]


class LeafPredicate(TypedDict):
    kind: LeafKind
    operator: LeafOperator
    authority_role: AuthorityRole
    confidence: NotRequired[Confidence]
    operands: list[Operand]
    set_descriptor: NotRequired[SetDescriptor | None]
    unsupported_reason: NotRequired[str | None]
    references_msg_sender: bool
    parameter_indices: list[int]
    expression: str
    basis: list[str]


PredicateOp = Literal["AND", "OR", "LEAF"]


class PredicateTree(TypedDict, total=False):
    op: PredicateOp
    children: list["PredicateTree"]
    leaf: LeafPredicate | None


# ---------------------------------------------------------------------------
# Helpers for constructing canonical predicate values.
# ---------------------------------------------------------------------------


def make_leaf_node(leaf: LeafPredicate) -> PredicateTree:
    tree: PredicateTree = {"op": "LEAF", "leaf": leaf}
    return tree


def make_and_node(children: list[PredicateTree]) -> PredicateTree:
    if len(children) == 1:
        return children[0]
    tree: PredicateTree = {"op": "AND", "children": children}
    return tree


def make_or_node(children: list[PredicateTree]) -> PredicateTree:
    if len(children) == 1:
        return children[0]
    tree: PredicateTree = {"op": "OR", "children": children}
    return tree


def operand(source: OperandSource, /, **kwargs: Any) -> Operand:
    """Construct an Operand with sensible defaults. Kwargs are merged
    into the typed dict; unknown keys raise (TypedDict total=False
    accepts NotRequired keys, but typos slip through — keep keys
    matching the type)."""
    payload: Operand = {"source": source}  # type: ignore[typeddict-item]
    payload.update(kwargs)  # type: ignore[typeddict-item]
    return payload
