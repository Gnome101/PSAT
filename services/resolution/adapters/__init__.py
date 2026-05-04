"""SetAdapter framework — week 5.

Adapters know how to read on-chain state for a particular standard
(OZ AccessControl, Gnosis Safe, DSAuth/Aragon, EIP-1271). They share
a uniform interface so the resolver's predicate evaluator can call
them without per-standard branches.

Per v4 plan + v5 round-3 #5/#7 fixes:
  - EvaluationContext carries chain, rpc, block, finality_depth,
    contract_meta, repo handles, recursive_resolver.
  - SetAdapter Protocol has classmethod matches(descriptor, ctx) → 0-100,
    enumerate(descriptor, ctx) → EnumerationResult,
    membership(descriptor, ctx, member) → Trit,
    supports_external_check_only() → bool.
  - AdapterRegistry orders adapters by registration; pick() returns
    the highest-scoring; ties broken by registration order; score 0
    from all adapters → unsupported(no_adapter).

Adapters are pluggable via a backend (the *Repo classes) so tests can
inject fake on-chain state without RPC. The real RPC integration is
done in a follow-up by wiring backends to ``services.resolution`` /
``utils.eth_rpc``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from ..capabilities import CapabilityExpr, Confidence

# Convenience re-export for adapters that need to construct caps.
__all__ = [
    "AdapterRegistry",
    "EnumerationResult",
    "EvaluationContext",
    "RoleGrantsRepo",
    "SafeRepo",
    "SetAdapter",
    "Trit",
]


# ---------------------------------------------------------------------------
# Trit
# ---------------------------------------------------------------------------


class Trit(Enum):
    YES = "yes"
    NO = "no"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# EnumerationResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnumerationResult:
    """Adapter output for enumerate()."""

    members: list[str] = field(default_factory=list)
    confidence: Confidence = "enumerable"
    partial_reason: str | None = None
    last_indexed_block: int | None = None


# ---------------------------------------------------------------------------
# Repo Protocols (backends)
# ---------------------------------------------------------------------------


class RoleGrantsRepo(Protocol):
    """Reads from the role_grants_events table (v4 plan §role_grants).
    Concrete implementation queries Postgres; fakes for tests provide
    in-memory data.

    For role-domain expansion (v3 round-3 #10 fix):
      - ``list_observed_roles`` returns every role bytes32 value
        ever seen in RoleGranted events for the contract. Used to
        seed the role-domain for parametric AC reads.
      - ``get_role_admin`` returns the admin role for a given role
        (the OZ AccessControl ``getRoleAdmin(role)`` semantic).
        Used to walk the admin chain to fixed point.
    """

    def members_for_role(
        self, *, chain_id: int, contract_address: str, role: bytes, block: int | None = None
    ) -> EnumerationResult: ...

    def has_member(self, *, chain_id: int, contract_address: str, role: bytes, member: str) -> Trit: ...

    def list_observed_roles(self, *, chain_id: int, contract_address: str) -> list[bytes]: ...

    def get_role_admin(
        self, *, chain_id: int, contract_address: str, role: bytes, block: int | None = None
    ) -> bytes | None: ...


class SafeRepo(Protocol):
    """Reads Safe owner+threshold from chain (or a fake)."""

    def get_owners_threshold(
        self, *, chain_id: int, contract_address: str, block: int | None = None
    ) -> tuple[list[str], int] | None: ...


class BytecodeRepo(Protocol):
    """Reads contract code metadata an adapter uses to score
    matches() — selectors present in the bytecode, declared events,
    inherited interfaces. Adapters can also inspect descriptor
    fields directly."""

    def has_selector(self, *, chain_id: int, contract_address: str, selector: str) -> bool: ...

    def declares_event(self, *, chain_id: int, contract_address: str, topic0: str) -> bool: ...


# ---------------------------------------------------------------------------
# EvaluationContext
# ---------------------------------------------------------------------------


@dataclass
class EvaluationContext:
    chain_id: int = 1
    rpc_url: str | None = None
    block: int | None = None
    finality_depth: int = 12
    contract_address: str | None = None
    role_grants: RoleGrantsRepo | None = None
    safe_repo: SafeRepo | None = None
    bytecode: BytecodeRepo | None = None
    recursive_resolver: Any = None
    # Free-form metadata bag for adapter-specific state; avoid using
    # for general-purpose data.
    meta: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# SetAdapter Protocol
# ---------------------------------------------------------------------------


SetDescriptor = dict  # forward-import-light alias; full type lives in predicate_types


class SetAdapter(Protocol):
    """Standard-ABI adapter for resolving a set descriptor's members."""

    @classmethod
    def matches(cls, descriptor: SetDescriptor, ctx: EvaluationContext) -> int:
        """Return 0-100. 0 means definitely not this adapter; 100
        means definitely yes. Ties at the registry level are broken
        by registration order. Score 0 from all → unsupported."""
        ...

    @classmethod
    def supports_external_check_only(cls) -> bool:
        """True iff the adapter can answer membership() against a
        live backend (used by the predicate evaluator to decide
        whether to emit external_check_only vs lower_bound finite_set
        when enumerate returns partial)."""
        ...

    def enumerate(self, descriptor: SetDescriptor, ctx: EvaluationContext) -> CapabilityExpr:
        """Populate the capability — finite_set for enumerable, or
        partial / external_check_only when the adapter can't fully
        list members."""
        ...


# ---------------------------------------------------------------------------
# AdapterRegistry
# ---------------------------------------------------------------------------


@dataclass
class AdapterRegistry:
    """Ordered list of adapters. ``pick()`` returns the highest-
    scoring adapter for a descriptor; on a tie, registration order
    wins. Score 0 from all → returns None and the caller emits
    unsupported(no_adapter)."""

    adapters: list[type[SetAdapter]] = field(default_factory=list)

    def register(self, adapter_cls: type[SetAdapter]) -> None:
        if adapter_cls in self.adapters:
            return
        self.adapters.append(adapter_cls)

    def pick(self, descriptor: SetDescriptor, ctx: EvaluationContext) -> type[SetAdapter] | None:
        best: tuple[int, type[SetAdapter]] | None = None
        for cls in self.adapters:
            score = cls.matches(descriptor, ctx)
            if score <= 0:
                continue
            if best is None or score > best[0]:
                best = (score, cls)
        return best[1] if best is not None else None

    def enumerate(self, descriptor: SetDescriptor, ctx: EvaluationContext) -> CapabilityExpr:
        adapter_cls = self.pick(descriptor, ctx)
        if adapter_cls is None:
            return CapabilityExpr.unsupported("no_adapter")
        adapter = adapter_cls()
        return adapter.enumerate(descriptor, ctx)
