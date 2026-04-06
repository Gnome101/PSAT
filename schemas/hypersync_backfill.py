"""Typed schemas for HyperSync backfill artifacts."""

from __future__ import annotations

from typing import TypedDict


class PolicyEventRecord(TypedDict):
    schema_version: str
    contract_address: str
    contract_name: str
    policy_id: str
    policy_label: str
    event_signature: str
    block_number: int
    tx_hash: str | None
    log_index: int | None
    decoded_fields: dict[str, object]


class PublicCapabilityStateEntry(TypedDict):
    target: str
    function_sig: str
    enabled: bool
    last_updated_block: int
    tx_hash: str | None


class RoleCapabilityStateEntry(TypedDict):
    role: int
    target: str
    function_sig: str
    enabled: bool
    last_updated_block: int
    tx_hash: str | None


class UserRoleStateEntry(TypedDict):
    user: str
    role: int
    enabled: bool
    last_updated_block: int
    tx_hash: str | None


class PolicyStateSnapshot(TypedDict):
    schema_version: str
    contract_address: str
    contract_name: str
    source: str
    event_count: int
    public_capabilities: list[PublicCapabilityStateEntry]
    role_capabilities: list[RoleCapabilityStateEntry]
    user_roles: list[UserRoleStateEntry]
