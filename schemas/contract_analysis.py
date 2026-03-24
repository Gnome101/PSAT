"""Typed schemas for contract analysis output."""

from __future__ import annotations

from typing import Literal, TypedDict

ControlModel = Literal["ownable", "access_control", "auth", "governance", "custom", "unknown"]
RiskLevel = Literal["low", "medium", "high", "unknown"]
UpgradeabilityPattern = Literal["uups", "transparent", "beacon", "custom", "none", "unknown"]
TimelockPattern = Literal["oz_timelock", "governor_timelock", "custom", "none", "unknown"]
CurrentHoldersStatus = Literal["unknown_static_only"]
ControllerTrackingMode = Literal["event_plus_state", "state_only", "manual_review"]
ControllerKind = Literal[
    "state_variable",
    "mapping_membership",
    "external_contract",
    "role_identifier",
    "unknown",
]
GuardKind = Literal[
    "caller_equals_storage",
    "caller_in_mapping",
    "external_authority_check",
    "role_membership_check",
    "unknown",
]
SinkKind = Literal["state_write", "contract_creation", "external_call", "delegatecall", "selfdestruct"]


class Evidence(TypedDict, total=False):
    file: str
    line: int
    detail: str


class Subject(TypedDict):
    address: str
    name: str
    compiler_version: str
    source_verified: bool


class AnalysisStatus(TypedDict):
    static_analysis_completed: bool
    slither_completed: bool
    errors: list[str]


class Summary(TypedDict):
    control_model: ControlModel
    is_upgradeable: bool
    is_pausable: bool
    has_timelock: bool
    static_risk_level: RiskLevel
    standards: list[str]
    is_factory: bool
    is_nft: bool


class ContractClassification(TypedDict):
    standards: list[str]
    is_erc20: bool
    is_erc721: bool
    is_erc1155: bool
    is_nft: bool
    is_factory: bool
    factory_functions: list[str]
    evidence: list[Evidence]


class RoleDefinition(TypedDict):
    role: str
    declared_in: str
    evidence: list[Evidence]


class PrivilegedFunction(TypedDict):
    contract: str
    function: str
    visibility: str
    guards: list[str]
    guard_kinds: list[GuardKind]
    controller_refs: list[str]
    sink_ids: list[str]
    effects: list[str]
    effect_targets: list[str]
    effect_labels: list[str]
    action_summary: str


class CurrentHolders(TypedDict):
    status: CurrentHoldersStatus


class AccessControlAnalysis(TypedDict):
    pattern: ControlModel
    owner_variables: list[str]
    admin_variables: list[str]
    role_definitions: list[RoleDefinition]
    privileged_functions: list[PrivilegedFunction]
    current_holders: CurrentHolders


class UpgradeabilityAnalysis(TypedDict):
    is_upgradeable: bool
    is_upgradeable_proxy: bool
    pattern: UpgradeabilityPattern
    upgradeable_version: str | None
    implementation_slots: list[str]
    admin_paths: list[str]
    evidence: list[Evidence]


class PausabilityAnalysis(TypedDict):
    is_pausable: bool
    pause_functions: list[str]
    unpause_functions: list[str]
    gating_modifiers: list[str]
    pause_variables: list[str]
    authorized_roles: list[str]
    evidence: list[Evidence]


class TimelockAnalysis(TypedDict):
    has_timelock: bool
    pattern: TimelockPattern
    delay_variables: list[str]
    queue_execute_functions: list[str]
    authorized_roles: list[str]
    evidence: list[Evidence]


class AuditAlignment(TypedDict):
    status: str
    bytecode_match: str
    notes: list[str]


class SlitherFinding(TypedDict):
    check: str
    impact: str
    confidence: str
    description: str


class SlitherSummary(TypedDict):
    detector_counts: dict[str, int]
    key_findings: list[SlitherFinding]


class TrackingHint(TypedDict):
    kind: str
    label: str
    source: str


class AssociatedEventInput(TypedDict):
    name: str
    type: str
    indexed: bool


class AssociatedEvent(TypedDict):
    name: str
    signature: str
    topic0: str
    inputs: list[AssociatedEventInput]


class ControllerWriterFunction(TypedDict):
    contract: str
    function: str
    visibility: str
    writes: list[str]
    associated_events: list[AssociatedEvent]
    evidence: list[Evidence]


class ControllerTrackingTarget(TypedDict):
    controller_id: str
    label: str
    source: str
    kind: ControllerKind
    tracking_mode: ControllerTrackingMode
    writer_functions: list[ControllerWriterFunction]
    associated_events: list[AssociatedEvent]
    polling_sources: list[str]
    notes: list[str]


class PolicyTrackingTarget(TypedDict):
    policy_id: str
    label: str
    policy_function: str
    tracked_state_targets: list[str]
    writer_functions: list[ControllerWriterFunction]
    associated_events: list[AssociatedEvent]
    notes: list[str]


class ControllerRef(TypedDict):
    id: str
    kind: ControllerKind
    label: str
    source: str
    evidence: list[Evidence]


class GuardRecord(TypedDict):
    id: str
    contract: str
    function: str
    kind: GuardKind
    controller_ids: list[str]
    evidence: list[Evidence]
    details: list[str]


class SinkRecord(TypedDict):
    id: str
    contract: str
    function: str
    kind: SinkKind
    target: str
    node_id: int
    guarded_by: list[str]
    effects: list[str]
    evidence: list[Evidence]


class PermissionGraph(TypedDict):
    controllers: list[ControllerRef]
    guards: list[GuardRecord]
    sinks: list[SinkRecord]


class ContractAnalysis(TypedDict):
    schema_version: str
    subject: Subject
    analysis_status: AnalysisStatus
    summary: Summary
    permission_graph: PermissionGraph
    contract_classification: ContractClassification
    access_control: AccessControlAnalysis
    upgradeability: UpgradeabilityAnalysis
    pausability: PausabilityAnalysis
    timelock: TimelockAnalysis
    audit_alignment: AuditAlignment
    slither: SlitherSummary
    tracking_hints: list[TrackingHint]
    controller_tracking: list[ControllerTrackingTarget]
    policy_tracking: list[PolicyTrackingTarget]
