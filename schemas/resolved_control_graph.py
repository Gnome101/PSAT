"""Typed schemas for recursive control-resolution artifacts."""

from __future__ import annotations

from typing import Literal, TypedDict

from .control_tracking import ResolvedControllerType

ResolvedNodeType = Literal["contract", "principal"]
ResolvedEdgeRelation = Literal[
    "controller_value",
    "role_principal",
    "safe_owner",
    "timelock_owner",
    "proxy_admin_owner",
    # Phase 3: mapping_member — edge from an access-control-like
    # contract to each current member of a mapping-backed allowlist
    # (MakerDAO wards, OZ-style whitelist, custom "approved" set).
    # Populated by the resolver after replaying the writer events
    # discovered in the static stage.
    "mapping_member",
]


class ResolvedGraphNode(TypedDict):
    id: str
    address: str
    node_type: ResolvedNodeType
    resolved_type: ResolvedControllerType
    label: str
    contract_name: str | None
    depth: int
    analyzed: bool
    details: dict[str, object]
    artifacts: dict[str, str]


class ResolvedGraphEdge(TypedDict):
    from_id: str
    to_id: str
    relation: ResolvedEdgeRelation
    label: str
    source_controller_id: str | None
    notes: list[str]


class ResolvedControlGraph(TypedDict):
    schema_version: str
    root_contract_address: str
    max_depth: int
    nodes: list[ResolvedGraphNode]
    edges: list[ResolvedGraphEdge]
