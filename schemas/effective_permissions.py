"""Typed schemas for resolved effective-permission outputs."""

from __future__ import annotations

from typing import Literal, TypedDict


ResolvedAddressType = Literal["zero", "eoa", "safe", "timelock", "proxy_admin", "contract", "unknown"]


class ResolvedPrincipal(TypedDict, total=False):
    address: str
    resolved_type: ResolvedAddressType
    details: dict[str, object]
    source_contract: str
    source_controller_id: str


class AuthorityRoleGrant(TypedDict):
    role: int
    principals: list[ResolvedPrincipal]


class EffectiveFunctionPermission(TypedDict):
    function: str
    abi_signature: str
    selector: str
    direct_owner: ResolvedPrincipal | None
    authority_public: bool
    authority_roles: list[AuthorityRoleGrant]
    effect_targets: list[str]
    effect_labels: list[str]
    action_summary: str
    notes: list[str]


class EffectivePermissions(TypedDict):
    schema_version: str
    contract_address: str
    contract_name: str
    authority_contract: str | None
    artifacts: dict[str, str]
    functions: list[EffectiveFunctionPermission]
