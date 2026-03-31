"""Typed schemas for resolved effective-permission outputs."""

from __future__ import annotations

from typing import Literal, TypedDict

from typing_extensions import NotRequired

ResolvedAddressType = Literal["zero", "eoa", "safe", "timelock", "proxy_admin", "contract", "unknown"]
PrincipalResolutionStatus = Literal[
    "complete",
    "missing_hypersync_token",
    "missing_policy_state",
    "no_authority",
    "no_authority_snapshot",
    "no_policy_tracking",
]


class PrincipalResolution(TypedDict):
    status: PrincipalResolutionStatus
    reason: str


class ResolvedPrincipal(TypedDict):
    address: str
    resolved_type: ResolvedAddressType
    details: dict[str, object]
    source_contract: NotRequired[str]
    source_controller_id: NotRequired[str]


class AuthorityRoleGrant(TypedDict):
    role: int
    principals: list[ResolvedPrincipal]


class ResolvedControllerGrant(TypedDict):
    controller_id: str
    label: str
    source: str
    kind: str
    principals: list[ResolvedPrincipal]
    notes: list[str]


class EffectiveFunctionPermission(TypedDict):
    function: str
    abi_signature: str
    selector: str
    direct_owner: ResolvedPrincipal | None
    authority_public: bool
    authority_roles: list[AuthorityRoleGrant]
    controllers: list[ResolvedControllerGrant]
    effect_targets: list[str]
    effect_labels: list[str]
    action_summary: str
    notes: list[str]


class EffectivePermissions(TypedDict):
    schema_version: str
    contract_address: str
    contract_name: str
    authority_contract: str | None
    principal_resolution: PrincipalResolution
    artifacts: dict[str, str]
    functions: list[EffectiveFunctionPermission]
