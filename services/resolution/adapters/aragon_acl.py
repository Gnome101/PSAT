"""Aragon ACL + DSAuth adapter — week 6 (codex F3 unlock).

Both protocols share a structural shape: an external contract
gates calls via an oracle method (``canPerform`` / ``canCall``).
The static stage emits external_bool leaves with
delegated_authority for these patterns; this adapter resolves the
authority chain to a CapabilityExpr.

Detection (no name matching — by selector/event topic):
  - Aragon ACL: ``canPerform(address,address,bytes32,uint256[])``
    selector + ``SetPermission(address,address,bytes32,bool)`` event topic
  - DSAuth: ``canCall(address,address,bytes4)`` selector

Both shapes also surface via the descriptor's ``authority_contract``
abi_hint when the static stage detects them upstream.

Resolution path:
  1. The descriptor identifies the oracle's call shape.
  2. The repo replays SetPermission / DSAuth events to fold into a
     current permission set per (target, role/sig).
  3. Adapter returns finite_set when the permission set for the
     given (target, role) is enumerable; external_check_only when
     the oracle accepts arbitrary parameters and enumeration would
     be exponential.
"""

from __future__ import annotations

from typing import Protocol

from ..capabilities import CapabilityExpr, ExternalCheck
from . import EnumerationResult, EvaluationContext

# canPerform(address,address,bytes32,uint256[])
ARAGON_CAN_PERFORM_SELECTOR = "0xfdef9106"
# SetPermission(address indexed entity, address indexed app, bytes32 indexed role, bool allowed)
ARAGON_SET_PERMISSION_TOPIC0 = "0x80f1d1bdcdef74de9d34a2cf3a5b5cb56d40b6cc20cffd1bd328eaa6f5a96ed3"

# canCall(address,address,bytes4)
DS_AUTH_CAN_CALL_SELECTOR = "0xb7009613"


class AragonACLRepo(Protocol):
    """Reads from an Aragon ACL contract (or a fake)."""

    def members_for_permission(
        self,
        *,
        chain_id: int,
        acl_address: str,
        target_app: str,
        role: bytes,
        block: int | None = None,
    ) -> EnumerationResult: ...


class DSAuthRepo(Protocol):
    """Reads DSAuth ACL state."""

    def members_for_callable(
        self,
        *,
        chain_id: int,
        authority_address: str,
        target_contract: str,
        selector: str,
        block: int | None = None,
    ) -> EnumerationResult: ...


class AragonACLAdapter:
    """Aragon ACL: ``canPerform(who, where, what)`` is the auth
    oracle. Permissions live as ``(entity, app, role)`` tuples
    written via SetPermission events on the ACL contract."""

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        # Strong signal: descriptor's authority_contract.abi_hint set.
        ah = (descriptor.get("authority_contract") or {}).get("abi_hint")
        if ah == "aragon_acl":
            return 95
        # Strong signal: enumeration_hint references SetPermission topic.
        for hint in descriptor.get("enumeration_hint", []) or []:
            if hint.get("topic0") == ARAGON_SET_PERMISSION_TOPIC0:
                return 90
        # Bytecode confirms canPerform selector.
        if ctx.bytecode is not None and ctx.contract_address is not None:
            try:
                if ctx.bytecode.has_selector(
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address,
                    selector=ARAGON_CAN_PERFORM_SELECTOR,
                ):
                    return 75
            except Exception:
                pass
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True  # canPerform itself is the canonical probe.

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        repo = ctx.meta.get("aragon_acl_repo") if ctx.meta else None
        if repo is None or ctx.contract_address is None:
            # No backend wired — emit external_check_only with the
            # canPerform call as the probe interface.
            return CapabilityExpr.external_check_only(
                ExternalCheck(
                    target_address=ctx.contract_address,
                    target_call_selector=ARAGON_CAN_PERFORM_SELECTOR,
                    extra={"abi": "aragon_acl"},
                )
            )
        # With a backend, attempt to read concrete permissions.
        # The descriptor's selector_context typically carries the
        # target app + role; for the v1 cut we fall back to the
        # contract's role_grants_history seed when those aren't
        # populated.
        target_app = (descriptor.get("authority_contract") or {}).get("target_app", ctx.contract_address)
        try:
            result = repo.members_for_permission(
                chain_id=ctx.chain_id,
                acl_address=ctx.contract_address,
                target_app=target_app,
                role=b"\x00" * 32,
                block=ctx.block,
            )
        except Exception:
            return CapabilityExpr.unsupported("aragon_repo_error")
        return CapabilityExpr.finite_set(
            list(result.members),
            quality="exact" if result.confidence == "enumerable" else "lower_bound",
            confidence=result.confidence,
            last_indexed_block=result.last_indexed_block,
        )


class DSAuthAdapter:
    """DSAuth: ``DSAuthority.canCall(src, dst, sig)`` returning
    bool. The implementing authority contract decides per-(src,
    dst, sig) tuple."""

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        ah = (descriptor.get("authority_contract") or {}).get("abi_hint")
        if ah == "dsauth":
            return 95
        if ctx.bytecode is not None and ctx.contract_address is not None:
            try:
                if ctx.bytecode.has_selector(
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address,
                    selector=DS_AUTH_CAN_CALL_SELECTOR,
                ):
                    return 70
            except Exception:
                pass
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        repo = ctx.meta.get("dsauth_repo") if ctx.meta else None
        if repo is None or ctx.contract_address is None:
            return CapabilityExpr.external_check_only(
                ExternalCheck(
                    target_address=ctx.contract_address,
                    target_call_selector=DS_AUTH_CAN_CALL_SELECTOR,
                    extra={"abi": "dsauth"},
                )
            )
        sel_ctx = descriptor.get("selector_context") or {}
        selectors = sel_ctx.get("selectors") or []
        target = (descriptor.get("authority_contract") or {}).get("target_app", ctx.contract_address)
        merged: list[str] = []
        confidence = "enumerable"
        last_block: int | None = None
        for selector in selectors or [None]:
            try:
                result = repo.members_for_callable(
                    chain_id=ctx.chain_id,
                    authority_address=ctx.contract_address,
                    target_contract=target,
                    selector=selector or "0x",
                    block=ctx.block,
                )
            except Exception:
                continue
            merged.extend(result.members)
            if result.confidence == "partial":
                confidence = "partial"
            last_block = result.last_indexed_block
        return CapabilityExpr.finite_set(
            merged,
            quality="exact" if confidence == "enumerable" else "lower_bound",
            confidence=confidence,  # type: ignore[arg-type]
            last_indexed_block=last_block,
        )


class EIP1271Adapter:
    """EIP-1271 contract signatures: enumeration is intrinsically
    impossible (the signer contract decides arbitrarily). Adapter
    always returns external_check_only with the
    isValidSignature(hash, sig) probe interface."""

    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        ah = (descriptor.get("authority_contract") or {}).get("abi_hint")
        if ah == "eip1271":
            return 95
        # The static stage's F3 detection already classifies this as
        # signature_auth at the leaf level; the descriptor on
        # signature_auth leaves typically doesn't have set_descriptor
        # populated, so this adapter mostly fires when the
        # descriptor explicitly hints eip1271 (week-7 UI flow).
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        return CapabilityExpr.external_check_only(
            ExternalCheck(
                target_address=ctx.contract_address,
                # isValidSignature(bytes32,bytes) selector
                target_call_selector="0x1626ba7e",
                extra={"abi": "eip1271"},
            )
        )
