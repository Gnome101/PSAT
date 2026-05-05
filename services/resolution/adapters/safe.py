"""Gnosis Safe adapter — week 5.

A Safe's authorization is M-of-N over its owner set. The capability
this adapter emits is ``threshold_group(M, owners)``.

Safe descriptors typically come from a different shape than
mapping_membership — they're produced by the Safe-detection branch
of the static stage (the ``execTransaction`` / ``getOwners``
signature pattern). For week-5 this adapter targets descriptors
with a ``meta.kind == "safe"`` hint or matching authority-contract
ABI hint.
"""

from __future__ import annotations

from ..capabilities import CapabilityExpr
from . import EvaluationContext


class SafeAdapter:
    @classmethod
    def matches(cls, descriptor: dict, ctx: EvaluationContext) -> int:
        # Safe descriptors are identified via the authority_contract
        # field's abi_hint OR via the bytecode having both
        # getOwners() and getThreshold() selectors. v6 round-2 #7 fix:
        # adapter scoring decisions live here, not in the static
        # descriptor.
        ah = (descriptor.get("authority_contract") or {}).get("abi_hint")
        if ah == "gnosis_safe":
            return 95
        if ctx.bytecode is not None and ctx.contract_address is not None:
            try:
                # getOwners() = 0xa0e67e2b, getThreshold() = 0xe75235b8
                if ctx.bytecode.has_selector(
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address,
                    selector="0xa0e67e2b",
                ) and ctx.bytecode.has_selector(
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address,
                    selector="0xe75235b8",
                ):
                    return 90
            except Exception:
                pass
        return 0

    @classmethod
    def supports_external_check_only(cls) -> bool:
        return True

    def enumerate(self, descriptor: dict, ctx: EvaluationContext) -> CapabilityExpr:
        if ctx.safe_repo is None or ctx.contract_address is None:
            return CapabilityExpr.unsupported("safe_no_backend")
        try:
            res = ctx.safe_repo.get_owners_threshold(
                chain_id=ctx.chain_id,
                contract_address=ctx.contract_address,
                block=ctx.block,
            )
        except Exception:
            return CapabilityExpr.unsupported("safe_repo_error")
        if res is None:
            return CapabilityExpr.unsupported("safe_state_unavailable")
        owners, threshold = res
        return CapabilityExpr.threshold_group(threshold, owners)
