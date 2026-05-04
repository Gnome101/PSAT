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

from typing import Any, Literal

from ..capabilities import CapabilityExpr, Confidence
from . import EvaluationContext

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
            return CapabilityExpr.finite_set(
                [],
                quality="lower_bound",
                confidence="partial",
            )
        # Cross-contract role registry shape: descriptor.authority_contract.
        # address_source points at the registry's storage var on the subject
        # contract (e.g. EtherFi's ``roleRegistry``). Resolve through
        # ctx.state_var_values; fall back to ctx.contract_address when the
        # descriptor doesn't carry a separate authority contract (canonical
        # OZ pattern where the subject IS the registry).
        lookup_address = self._resolve_lookup_address(descriptor, ctx)
        if lookup_address is None:
            return CapabilityExpr.finite_set([], quality="lower_bound", confidence="partial")

        # Build the role-domain for this descriptor. Per v3 round-3
        # #10 fix:
        #   - seed with DEFAULT_ADMIN_ROLE (bytes32(0)) by default
        #   - add concrete role constants from descriptor.key_sources
        #   - add observed-history roles from role_grants
        #   - walk getRoleAdmin to fixed point
        role_domain = self._expand_role_domain(descriptor, ctx)
        if not role_domain:
            return CapabilityExpr.finite_set([], quality="lower_bound", confidence="partial")

        # The descriptor either has concrete role constants (1+ roles)
        # or is parametric (role from a function arg). For concrete
        # roles, we know exactly which role to enumerate. For
        # parametric, we expose the per-role expansion (every role in
        # the domain → its members).
        is_parametric = not self._role_key_constants(descriptor)
        merged: list[str] = []
        worst_confidence: Confidence = "enumerable"
        last_block: int | None = None
        for role in role_domain:
            try:
                result = ctx.role_grants.members_for_role(
                    chain_id=ctx.chain_id,
                    contract_address=lookup_address,
                    role=role,
                    block=ctx.block,
                )
            except Exception:
                continue
            merged.extend(result.members)
            if _confidence_lt(result.confidence, worst_confidence):
                worst_confidence = result.confidence
            last_block = result.last_indexed_block

        # Concrete role(s) → exact enumeration when confidence allows.
        # Parametric → lower_bound + partial (the union across roles
        # is over-permissive vs the runtime role argument; the UI
        # exposes a per-role expansion, but the capability emitted
        # here is the conservative union).
        quality: Literal["exact", "lower_bound"] = (
            "exact" if (worst_confidence == "enumerable" and not is_parametric) else "lower_bound"
        )
        confidence = worst_confidence if not is_parametric else "partial"
        return CapabilityExpr.finite_set(
            merged,
            quality=quality,
            confidence=confidence,
            last_indexed_block=last_block,
        )

    def _resolve_lookup_address(self, descriptor: dict, ctx: EvaluationContext) -> str | None:
        """Pick the contract address used for the role_grants lookup.

        Canonical OZ AccessControl: the subject contract IS the registry, so
        the lookup uses ``ctx.contract_address``. Cross-contract registries
        (EtherFi's RoleRegistry pattern) carry an
        ``authority_contract.address_source`` pointing at a state variable
        holding the registry address; we resolve that via
        ``ctx.state_var_values`` (populated by the resolver from
        ``controller_values``). Returns None when neither path produces a
        usable address — caller emits the lower_bound placeholder."""
        authority_contract = descriptor.get("authority_contract")
        if isinstance(authority_contract, dict):
            address_source = authority_contract.get("address_source") or {}
            if address_source.get("source") == "state_variable":
                sv_name = address_source.get("state_variable_name")
                if sv_name and ctx.state_var_values:
                    value = ctx.state_var_values.get(sv_name)
                    if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
                        return value.lower()
            elif address_source.get("source") == "constant":
                value = address_source.get("constant_value")
                if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
                    return value.lower()
        # Fall back to the subject contract for the canonical OZ shape.
        return ctx.contract_address

    def _expand_role_domain(self, descriptor: dict, ctx: EvaluationContext) -> set[bytes]:
        """Expand the role domain per v3 round-3 #10 fix:
        1. Seed DEFAULT_ADMIN_ROLE (bytes32(0)) when descriptor's
           role_domain.auto_seed_default_admin is True (default).
        2. Add concrete role constants from descriptor.key_sources.
        3. Add observed-history roles via list_observed_roles.
        4. Walk getRoleAdmin to fixed point (cap depth 6).
        """
        rd_spec = descriptor.get("role_domain") or {}
        domain: set[bytes] = set()

        # 1. Seed default admin if enabled (default True for AC-shaped).
        if rd_spec.get("auto_seed_default_admin", True):
            domain.add(b"\x00" * 32)

        # 2. Concrete role constants from key_sources.
        for role in self._role_key_constants(descriptor):
            domain.add(role)

        # 3. Observed-history roles.
        if rd_spec.get("auto_seed_default_admin", True) or "role_granted_history" in (rd_spec.get("sources") or []):
            try:
                observed = ctx.role_grants.list_observed_roles(  # type: ignore[union-attr]
                    chain_id=ctx.chain_id,
                    contract_address=ctx.contract_address or "",
                )
            except Exception:
                observed = []
            for r in observed:
                if isinstance(r, bytes) and len(r) == 32:
                    domain.add(r)

        # 4. Recursive role-admin expansion to fixed point (cap 6).
        if rd_spec.get("recursive_role_admin_expansion", True):
            for _ in range(6):
                added = False
                for role in list(domain):
                    try:
                        admin = ctx.role_grants.get_role_admin(  # type: ignore[union-attr]
                            chain_id=ctx.chain_id,
                            contract_address=ctx.contract_address or "",
                            role=role,
                            block=ctx.block,
                        )
                    except Exception:
                        admin = None
                    if admin is not None and isinstance(admin, bytes) and len(admin) == 32 and admin not in domain:
                        domain.add(admin)
                        added = True
                if not added:
                    break

        return domain

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _role_key_constants(self, descriptor: dict) -> list[bytes]:
        """Pull statically-known role keys from the descriptor's
        key_sources. The role key is the non-caller key in a 2-key
        AC mapping. Returns its byte values; empty list if parametric.

        Three sources qualify:
          * ``constant`` — the literal bytes32 value embedded in the
            descriptor.
          * ``external_call`` — the role was passed in as the result of
            calling a role-constant getter (Solidity's auto-generated
            getter for ``bytes32 public constant PROTOCOL_PAUSER =
            keccak256("PROTOCOL_PAUSER")``). The callee name is the
            role identifier; we hash it via the canonical OZ convention
            (``keccak256(name)``). When the contract defines roles via
            a different scheme the hash misses and we fall back to the
            existing observed-history expansion.
          * ``state_variable`` — same idea but the static analyzer
            traced through a stored copy of the constant. Use the var
            name as the seed string."""
        keys = descriptor.get("key_sources") or []
        roles: list[bytes] = []
        for k in keys:
            src = k.get("source")
            if src in ("msg_sender", "tx_origin", "signature_recovery"):
                continue
            if src == "constant":
                val = k.get("constant_value")
                role_bytes = _coerce_role_bytes(val)
                if role_bytes is not None:
                    roles.append(role_bytes)
                continue
            # OZ convention: role constants are keccak256(NAME). Try the
            # callee / state-variable name; the lookup either hits the
            # right grant rows or returns no members and we fall back
            # to lower_bound. No false positives because the role hash
            # has to match exactly.
            seed = None
            if src == "external_call":
                seed = k.get("callee")
            elif src == "state_variable":
                seed = k.get("state_variable_name")
            if seed:
                roles.append(_keccak_role(seed))
        return roles


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_CONFIDENCE_ORDER = {"enumerable": 2, "partial": 1, "check_only": 0}


def _confidence_lt(a: Confidence, b: Confidence) -> bool:
    return _CONFIDENCE_ORDER[a] < _CONFIDENCE_ORDER[b]


def _keccak_role(name: str) -> bytes:
    """Hash a role-constant name using the OZ convention
    (``keccak256(name)``). Used to recover the bytes32 identifier when
    the descriptor's key_source is the role-constant getter rather than
    an inlined constant_value (e.g. ``PROTOCOL_PAUSER`` was looked up
    via ``contract.PROTOCOL_PAUSER()`` in the source IR)."""
    from eth_utils.crypto import keccak

    return keccak(text=name)


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
