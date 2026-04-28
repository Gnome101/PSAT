"""Typed schemas for contract analysis output."""

from __future__ import annotations

from typing import Literal, TypedDict

from typing_extensions import NotRequired

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
    "singleton_slot",
    "external_policy",
    "computed",
    "unknown",
]
GuardKind = Literal[
    "caller_equals_storage",
    "caller_in_mapping",
    "external_authority_check",
    "role_membership_check",
    "caller_via_helper_function",
    "unknown",
]
SinkKind = Literal["state_write", "contract_creation", "external_call", "delegatecall", "selfdestruct"]
ControllerReadStrategy = Literal["getter_call", "storage_slot", "mapping_lookup", "event_reconstruction", "unknown"]
ControllerConfidence = Literal["exact", "high", "medium", "low", "unknown"]


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


CallerSinkKind = Literal[
    "caller_equals",
    "caller_in_mapping",
    "caller_external_call",
    "caller_internal_call",
    "caller_signature",
    "caller_merkle",
    "caller_unknown",
]


class CallerSink(TypedDict, total=False):
    """One place where msg.sender reaches a gating predicate.

    Emitted by `caller_reach_analysis` in a closed vocabulary that
    covers every way a Solidity function can decide "is msg.sender
    allowed to call this?". Kind-specific fields are marked optional
    (TypedDict total=False) — each kind uses a different subset, but
    every record carries `kind`, `evidence`, and `revert_on_mismatch`.

    The point of this type is to replace the eleven pattern-specific
    guard detectors with one analyzer that emits structured records
    any downstream stage can consume uniformly.
    """

    kind: CallerSinkKind
    evidence: Evidence
    # True when the sink blocks the call on failure (sits inside a
    # require/assert, or an `if (...) revert` that rejects the
    # non-matching branch). False when the read of msg.sender is
    # observational and doesn't gate execution.
    revert_on_mismatch: bool

    # caller_equals: msg.sender == target_state_var OR msg.sender == constant
    target_state_var: str
    target_type: str
    constant_value: str

    # caller_in_mapping: mapping[msg.sender] <predicate>
    mapping_name: str
    mapping_predicate: str  # e.g. "== 1", "> 0", "!= 0"

    # caller_external_call: X.method(..., msg.sender, ...)
    external_target_state_var: str
    external_method: str
    external_role_args: list[str]

    # caller_internal_call: _helper(..., msg.sender, ...)
    internal_callee: str

    # caller_signature / caller_merkle
    signature_source_var: str
    merkle_root_var: str


class ExternalCallGuard(TypedDict):
    """A guard that dispatches to an external contract to make the check.

    Captures enough of the AST-level call to let a later stage (resolution
    or policy) resolve the guard to a concrete role without heuristics.
    Two patterns dominate:

    - **Pattern A** (Renzo family): `roleManager.onlyDepositWithdrawPauser
      (msg.sender)` — the role is encoded in the method name. The policy
      stage resolves it via the authority contract's `method_to_role` map.
    - **Pattern B** (ether.fi LP, OZ AccessControl consumers):
      `roleRegistry.hasRole(PROTOCOL_PAUSER, msg.sender)` — the role is a
      state/constant passed as an argument. The policy stage reads it
      directly off `role_args` without needing the method->role lookup.
    """

    # "modifier" when the call sits inside a modifier body that gates the
    # function, "inline" when the require(X.check(...)) lives in the
    # function body itself.
    kind: Literal["modifier", "inline"]
    # State variable the call is dispatched on (e.g. "roleManager"). Empty
    # string when the call target is an expression we couldn't trace back
    # to a state var (e.g. a local, a library, a parameter).
    target_state_var: str
    # Declared type of the state variable (e.g. "IRoleManager"). Empty
    # when slither doesn't carry a resolvable type.
    target_type: str
    # Method name on the target (e.g. "onlyDepositWithdrawPauser" for
    # pattern A, "hasRole" for pattern B). Non-empty — we drop records
    # that lack it.
    method: str
    # Name of the modifier that carried this call, if kind == "modifier".
    modifier_name: NotRequired[str]
    # True when msg.sender (or an alias) appears in the arguments — the
    # signal that this is an auth check rather than an arbitrary call.
    sender_in_args: bool
    # Role-like UPPER_SNAKE identifiers that appear as non-sender
    # arguments to the call. For `hasRole(PROTOCOL_PAUSER, msg.sender)`
    # this captures `["PROTOCOL_PAUSER"]`. Empty for Renzo-pattern calls
    # where the role isn't an argument — those go through method_to_role.
    role_args: NotRequired[list[str]]


class PrivilegedFunction(TypedDict):
    contract: str
    function: str
    visibility: str
    guards: list[str]
    guard_kinds: list[GuardKind]
    controller_refs: list[str]
    controller_ids: NotRequired[list[str]]
    sink_ids: list[str]
    effects: list[str]
    effect_targets: list[str]
    effect_labels: list[str]
    action_summary: str
    # Structured form of external-contract guards, consumed by the
    # policy stage for cross-contract role resolution.
    external_call_guards: NotRequired[list[ExternalCallGuard]]
    sinks: NotRequired[list["CallerSink"]]


class CurrentHolders(TypedDict):
    status: CurrentHoldersStatus


class AccessControlAnalysis(TypedDict):
    pattern: ControlModel
    owner_variables: list[str]
    admin_variables: list[str]
    role_definitions: list[RoleDefinition]
    privileged_functions: list[PrivilegedFunction]
    current_holders: CurrentHolders
    # method_name -> [role_constant, ...]. The policy stage matches a
    # guard's method name to resolve `X.method(msg.sender)` to a role.
    method_to_role: NotRequired[dict[str, list[str]]]
    # WriterEventSpec entries (shape in mapping_events.py). Kept as
    # list[dict] because TypedDict can't forward-ref a sibling module.
    mapping_writer_events: NotRequired[list[dict]]


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


class ControllerReadSpec(TypedDict):
    strategy: ControllerReadStrategy
    target: str
    contract_source: NotRequired[str]


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
    read_spec: ControllerReadSpec | None
    confidence: ControllerConfidence | None
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
    read_spec: ControllerReadSpec | None
    confidence: ControllerConfidence | None
    evidence: list[Evidence]


class GuardRecord(TypedDict):
    id: str
    contract: str
    function: str
    kind: GuardKind
    confidence: NotRequired[ControllerConfidence]
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
