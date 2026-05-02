"""OZ AccessControl adapter — week 5.

Detects mapping_membership descriptors that match the OZ
AccessControl shape and enumerates members via the role_grants
repo. Detection is structural per v4 plan §SetDescriptor + RoleDomain
(round-3 #7 fix on AC-shaped detection):

  - the descriptor is a 2-key mapping_membership (role + caller)
  - the descriptor's enumeration_hint references RoleGranted topic0,
    OR the bytecode repo confirms hasRole(bytes32,address) selector
    plus a successful getRoleAdmin call (the latter is checked at
    runtime; for the static-time matches() score we use bytecode
    hints if available).

Detection NEVER uses identifier names. Storage var name matching is
not a signal.
"""

from __future__ import annotations

from typing import Any

from ..capabilities import CapabilityExpr, Confidence
from . import EnumerationResult, EvaluationContext, SetAdapter, Trit


# RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)
ROLE_GRANTED_TOPIC0 = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d"
HAS_ROLE_SELECTOR = "0x91d14854"  # hasRole(bytes32,address)


class AccessControlAdapter:
    """OZ AccessControl + AccessControlEnumerable resolver. Reads
    role members via the role_grants repo (event-indexed) and falls
    back to direct hasRole probes for membership()."""

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        if descriptor.get("kind") != "mapping_membership":
            return 0
        keys = descriptor.get("key_sources") or []
        # AC shape: 2 keys (role + caller) where one key sources from
        # msg_sender / signature_recovery.
        if len(keys) != 2:
            return 0
        if not any(k.get("source") in ("msg_sender", "tx_origin", "signature_recovery") for k in keys):
            return 0
        # Strong signal: enumeration_hint contains RoleGranted topic.
        for hint in descriptor.get("enumeration_hint", []) or []:
            if hint.get("topic0") == ROLE_GRANTED_TOPIC0:
                return 95
        # Bytecode confirms hasRole selector — also strong.
        if ctx.bytecode is not None and ctx.contract_address is not None:
            try:
                if ctx.bytecode.has_selector(
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address,
                    selector=HAS_ROLE_SELECTOR,
                ):
                    return 80
            except Exception:
                pass
        # Weaker structural signal: 2-key mapping with caller as a key
        # but no AC-specific evidence. Score low so other adapters
        # (e.g., custom DSAuth-shaped) can claim if they score higher;
        # only AC's role_grants_history sources on the descriptor
        # would push this to "yes".
        if descriptor.get("role_domain") is not None:
            return 40
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True  # hasRole(role, addr) is the canonical probe.

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        if ctx.role_grants is None:
            # No backend wired — return placeholder.
            return CapabilityExpr.finite_set(
                [],
                quality="lower_bound",
                confidence="partial",
            )
        # Identify the role key (the non-caller key) and try to
        # determine its constant value if any.
        role_keys = self._role_key_constants(descriptor)
        if not role_keys:
            # Parametric role — enumerate per-role would require
            # role-domain expansion (week 6 deliverable). For now:
            # return a lower_bound finite_set of all members across
            # all roles, marked partial so the resolver knows it's
            # over-permissive.
            members = self._enumerate_all_roles(descriptor, ctx)
            return CapabilityExpr.finite_set(
                members,
                quality="lower_bound",
                confidence="partial",
            )
        # Concrete role(s) — enumerate per role.
        if ctx.contract_address is None:
            return CapabilityExpr.finite_set([], quality="lower_bound", confidence="partial")
        merged: list[str] = []
        worst_confidence: Confidence = "enumerable"
        last_block: int | None = None
        for role in role_keys:
            try:
                result = ctx.role_grants.members_for_role(
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address,
                    role=role,
                    block=ctx.block,
                )
            except Exception:
                continue
            merged.extend(result.members)
            if _confidence_lt(result.confidence, worst_confidence):
                worst_confidence = result.confidence
            last_block = result.last_indexed_block
        return CapabilityExpr.finite_set(
            merged,
            quality="exact" if worst_confidence == "enumerable" else "lower_bound",
            confidence=worst_confidence,
            last_indexed_block=last_block,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _role_key_constants(self, descriptor: dict) -> list[bytes]:
        """Pull constant-value role keys from the descriptor's
        key_sources. The role key is the non-caller key in a 2-key
        AC mapping. Returns its constant value(s) if statically
        known; empty list if parametric."""
        keys = descriptor.get("key_sources") or []
        roles: list[bytes] = []
        for k in keys:
            if k.get("source") in ("msg_sender", "tx_origin", "signature_recovery"):
                continue
            if k.get("source") == "constant":
                val = k.get("constant_value")
                role_bytes = _coerce_role_bytes(val)
                if role_bytes is not None:
                    roles.append(role_bytes)
        return roles

    def _enumerate_all_roles(self, descriptor: dict, ctx: EvaluationContext) -> list[str]:
        """For parametric role descriptors, attempt a per-role
        expansion via role-domain history (round-3 #10 fix). Without
        a populated role_grants repo this returns []; the week-6
        role-domain expansion will fully populate."""
        # Returns members from the DEFAULT_ADMIN_ROLE seed if
        # role_grants supports it; otherwise empty.
        try:
            result = ctx.role_grants.members_for_role(
                chain_id=ctx.chain_id,
                contract_address=ctx.contract_address or "",
                role=b"\x00" * 32,
                block=ctx.block,
            )
            return list(result.members)
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_CONFIDENCE_ORDER = {"enumerable": 2, "partial": 1, "check_only": 0}


def _confidence_lt(a: Confidence, b: Confidence) -> bool:
    return _CONFIDENCE_ORDER[a] < _CONFIDENCE_ORDER[b]


def _coerce_role_bytes(value: Any) -> bytes | None:
    """Convert a Slither Constant value (which may be a hex string,
    int, or bytes) into the 32-byte role identifier."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value if len(value) == 32 else value.rjust(32, b"\x00")
    if isinstance(value, int):
        return value.to_bytes(32, "big") if value >= 0 else None
    if isinstance(value, str):
        v = value.strip()
        if v.startswith("0x"):
            v = v[2:]
        try:
            data = bytes.fromhex(v)
        except ValueError:
            return None
        if len(data) > 32:
            return None
        return data.rjust(32, b"\x00")
    return None
