"""Deterministic controller tracking metadata for event-first, polling-backed monitoring."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from eth_utils.crypto import keccak

from schemas.contract_analysis import (
    AccessControlAnalysis,
    AssociatedEvent,
    AssociatedEventInput,
    ControllerTrackingTarget,
    ControllerWriterFunction,
    Evidence,
    PermissionGraph,
    PolicyTrackingTarget,
)

from .shared import (
    _contract_functions,
    _declaring_contract_name,
    _looks_like_role_identifier_name,
    _source_evidence,
)


def _unit_key(unit) -> str:
    return (
        getattr(unit, "canonical_name", None)
        or getattr(unit, "full_name", None)
        or getattr(unit, "name", str(id(unit)))
    )


def _abi_type(type_obj) -> str:
    if type_obj is None:
        return "unknown"

    type_name = type(type_obj).__name__
    if type_name == "ElementaryType":
        return str(type_obj)
    if type_name == "UserDefinedType":
        underlying = getattr(type_obj, "type", None)
        underlying_name = type(underlying).__name__
        if underlying_name == "Contract":
            return "address"
        if underlying_name == "Enum":
            return "uint8"
        return str(type_obj)
    if type_name == "ArrayType":
        base = _abi_type(getattr(type_obj, "type", None))
        length = getattr(type_obj, "length", None)
        if length is None:
            return f"{base}[]"
        return f"{base}[{length}]"
    return str(type_obj)


def _event_signature(event_decl) -> str:
    arg_types = [_abi_type(getattr(elem, "type", None)) for elem in getattr(event_decl, "elems", [])]
    return f"{event_decl.name}({','.join(arg_types)})"


def _event_inputs(event_decl) -> list[AssociatedEventInput]:
    return [
        {
            "name": getattr(elem, "name", "") or "",
            "type": _abi_type(getattr(elem, "type", None)),
            "indexed": bool(getattr(elem, "indexed", False)),
        }
        for elem in getattr(event_decl, "elems", [])
    ]


def _event_reference(event_decl) -> AssociatedEvent:
    signature = _event_signature(event_decl)
    return {
        "name": event_decl.name,
        "signature": signature,
        "topic0": "0x" + keccak(text=signature).hex(),
        "inputs": _event_inputs(event_decl),
    }


def _event_index(contract) -> dict[str, list]:
    by_name: dict[str, list] = {}
    for current in [contract, *getattr(contract, "inheritance", [])]:
        events = getattr(current, "events", []) or getattr(current, "events_declared", [])
        for event_decl in events:
            by_name.setdefault(event_decl.name, []).append(event_decl)
    return by_name


def _ir_argument_types(arguments: Iterable) -> list[str]:
    return [_abi_type(getattr(argument, "type", None)) for argument in arguments]


def _resolve_event_refs(event_name: str, arguments: list, event_index: dict[str, list]) -> list[AssociatedEvent]:
    emitted_types = _ir_argument_types(arguments)
    matches = [
        event_decl
        for event_decl in event_index.get(event_name, [])
        if [_abi_type(getattr(elem, "type", None)) for elem in getattr(event_decl, "elems", [])] == emitted_types
    ]
    if not matches:
        matches = [
            event_decl
            for event_decl in event_index.get(event_name, [])
            if len(getattr(event_decl, "elems", []) or []) == len(arguments)
        ] or event_index.get(event_name, [])
    deduped: dict[str, AssociatedEvent] = {}
    for event_decl in matches:
        event_ref = _event_reference(event_decl)
        deduped[event_ref["signature"]] = event_ref
    return sorted(deduped.values(), key=lambda item: item["signature"])


def _collect_events(
    unit, project_dir: Path, event_index: dict[str, list], seen: set[str]
) -> list[tuple[AssociatedEvent, Evidence]]:
    key = _unit_key(unit)
    if key in seen:
        return []
    seen.add(key)

    events: list[tuple[AssociatedEvent, Evidence]] = []
    for node in getattr(unit, "nodes", []):
        for ir in getattr(node, "irs", []) or []:
            if type(ir).__name__ == "EventCall":
                event_name = getattr(ir, "name", None) or ""
                if event_name:
                    event_refs = _resolve_event_refs(event_name, list(getattr(ir, "arguments", []) or []), event_index)
                    evidence = _source_evidence(node, project_dir, detail=f"emit {event_name}")
                    events.extend((event_ref, evidence) for event_ref in event_refs)
            if type(ir).__name__ != "InternalCall":
                continue
            callee = getattr(ir, "function", None)
            if callee is None:
                continue
            events.extend(_collect_events(callee, project_dir, event_index, seen))
    return events


def _dedupe_event_refs(events: list[tuple[AssociatedEvent, Evidence]]) -> list[AssociatedEvent]:
    deduped: dict[str, AssociatedEvent] = {}
    for event_ref, _ in events:
        deduped[event_ref["signature"]] = event_ref
    return sorted(deduped.values(), key=lambda item: item["signature"])


def _functions_by_signature(contract) -> dict[str, object]:
    return {
        getattr(function, "full_name", getattr(function, "name", "")): function
        for function in _contract_functions(contract)
    }


def _role_identifier_authority_sources(
    permission_graph: PermissionGraph,
    access_control: AccessControlAnalysis,
) -> dict[str, str]:
    kinds_by_source: dict[str, set[str]] = {}
    for controller in permission_graph.get("controllers", []):
        source = str(controller.get("source") or "")
        kind = str(controller.get("kind") or "")
        if not source or not kind:
            continue
        kinds_by_source.setdefault(source, set()).add(kind)

    role_to_authority: dict[str, str | None] = {}
    for privileged in access_control.get("privileged_functions", []):
        refs = [str(ref) for ref in privileged.get("controller_refs", []) if isinstance(ref, str)]
        role_refs = [ref for ref in refs if "role_identifier" in kinds_by_source.get(ref, set())]
        authority_refs = [ref for ref in refs if "external_contract" in kinds_by_source.get(ref, set())]
        unique_authorities = sorted(set(authority_refs))
        if len(unique_authorities) != 1:
            continue
        authority_source = unique_authorities[0]
        for role_ref in role_refs:
            existing = role_to_authority.get(role_ref)
            if existing is None:
                role_to_authority[role_ref] = authority_source
            elif existing != authority_source:
                role_to_authority[role_ref] = None

    return {role: source for role, source in role_to_authority.items() if source}


def _writer_records_for_targets(
    contract,
    project_dir: Path,
    permission_graph: PermissionGraph,
    target_sources: Iterable[str],
    event_lookup: dict[str, list],
) -> tuple[list[ControllerWriterFunction], list[AssociatedEvent]]:
    target_set = set(target_sources)
    functions_by_signature = _functions_by_signature(contract)
    writer_targets_by_signature: dict[str, set[str]] = {}
    for sink in permission_graph["sinks"]:
        if (
            sink["kind"] != "state_write"
            or sink["target"] not in target_set
            or sink["function"].startswith("constructor(")
        ):
            continue
        writer_targets_by_signature.setdefault(sink["function"], set()).add(sink["target"])

    writer_functions: list[ControllerWriterFunction] = []
    aggregated_events: dict[str, AssociatedEvent] = {}
    for signature in sorted(writer_targets_by_signature):
        function = functions_by_signature.get(signature)
        if function is None:
            continue

        event_records = _collect_events(function, project_dir, event_lookup, set())
        event_refs = _dedupe_event_refs(event_records)
        for event_ref in event_refs:
            aggregated_events[event_ref["signature"]] = event_ref
        writer_functions.append(
            {
                "contract": _declaring_contract_name(function, contract.name),
                "function": signature,
                "visibility": getattr(function, "visibility", "unknown"),
                "writes": sorted(writer_targets_by_signature[signature]),
                "associated_events": event_refs,
                "evidence": [
                    _source_evidence(
                        function,
                        project_dir,
                        detail=f"writes tracked state {', '.join(sorted(writer_targets_by_signature[signature]))}",
                    )
                ],
            }
        )

    associated_events = sorted(aggregated_events.values(), key=lambda item: item["signature"])
    return writer_functions, associated_events


def build_controller_tracking(
    contract, project_dir: Path, permission_graph: PermissionGraph, access_control: AccessControlAnalysis | None = None
) -> list[ControllerTrackingTarget]:
    """Build event-first tracking metadata for mutable controllers discovered in the permission graph."""
    event_lookup = _event_index(contract)
    role_authority_sources = _role_identifier_authority_sources(permission_graph, access_control or {})

    tracking_targets: list[ControllerTrackingTarget] = []
    for controller in permission_graph["controllers"]:
        controller_kind = controller["kind"]
        controller_source = controller["source"]

        if controller_kind == "role_identifier":
            read_spec = {"strategy": "getter_call", "target": controller_source}
            authority_source = role_authority_sources.get(controller_source)
            if authority_source:
                read_spec["contract_source"] = authority_source
            tracking_targets.append(
                {
                    "controller_id": controller["id"],
                    "label": controller["label"],
                    "source": controller_source,
                    "kind": controller_kind,
                    "read_spec": read_spec,
                    "confidence": controller.get("confidence"),
                    "tracking_mode": "state_only",
                    "writer_functions": [],
                    "associated_events": [],
                    "polling_sources": [controller_source],
                    "notes": [
                        "Resolve the role identifier via eth_call and expand current "
                        "members through the authority adapter when supported."
                    ],
                }
            )
            continue

        if controller_kind not in {"state_variable", "external_contract"}:
            tracking_targets.append(
                {
                    "controller_id": controller["id"],
                    "label": controller["label"],
                    "source": controller_source,
                    "kind": controller_kind,
                    "read_spec": controller.get("read_spec"),
                    "confidence": controller.get("confidence"),
                    "tracking_mode": "manual_review",
                    "writer_functions": [],
                    "associated_events": [],
                    "polling_sources": [controller_source],
                    "notes": [
                        "Controller kind is not directly reducible to a mutable "
                        "singleton state value; manual review or specialized "
                        "resolution is required."
                    ],
                }
            )
            continue

        writer_functions, associated_events = _writer_records_for_targets(
            contract,
            project_dir,
            permission_graph,
            [controller_source],
            event_lookup,
        )
        if associated_events:
            tracking_mode = "event_plus_state"
            notes = [
                "Monitor associated events for low-latency detection and confirm "
                "the resulting controller state with RPC reads."
            ]
        else:
            tracking_mode = "state_only"
            notes = [
                "No deterministically associated post-deploy events were found "
                "for this controller state; rely on periodic RPC reads and "
                "reconciliation."
            ]
            if not writer_functions:
                notes.append(
                    "No post-deploy writer functions were found from static "
                    "analysis; continue polling the current value and reanalyze "
                    "on implementation changes."
                )

        tracking_targets.append(
            {
                "controller_id": controller["id"],
                "label": controller["label"],
                "source": controller_source,
                "kind": controller_kind,
                "read_spec": controller.get("read_spec"),
                "confidence": controller.get("confidence"),
                "tracking_mode": tracking_mode,
                "writer_functions": writer_functions,
                "associated_events": associated_events,
                "polling_sources": [controller_source],
                "notes": notes,
            }
        )

    existing_ids = {target["controller_id"] for target in tracking_targets}
    existing_kinds_by_source: dict[str, set[str]] = {}
    for target in tracking_targets:
        existing_kinds_by_source.setdefault(target["source"], set()).add(target["kind"])
    if access_control:
        for privileged in access_control.get("privileged_functions", []):
            for ref in privileged.get("controller_refs", []):
                if ref in {"role", "_role"}:
                    continue
                kind = "role_identifier" if _looks_like_role_identifier_name(ref) else "state_variable"
                controller_id = f"{kind}:{ref}"
                if controller_id in existing_ids:
                    continue
                if kind == "state_variable" and "role_identifier" in existing_kinds_by_source.get(ref, set()):
                    continue
                read_spec = None
                if kind == "role_identifier":
                    read_spec = {"strategy": "getter_call", "target": ref}
                    authority_source = role_authority_sources.get(ref)
                    if authority_source:
                        read_spec["contract_source"] = authority_source
                tracking_targets.append(
                    {
                        "controller_id": controller_id,
                        "label": ref,
                        "source": ref,
                        "kind": kind,  # type: ignore[typeddict-item]
                        "read_spec": read_spec,
                        "confidence": None,
                        "tracking_mode": "state_only",
                        "writer_functions": [],
                        "associated_events": [],
                        "polling_sources": [ref],
                        "notes": [
                            "Synthesized from semantic access-control inference because "
                            "the permission graph did not expose a runtime controller "
                            "source directly."
                        ],
                    }
                )
                existing_ids.add(controller_id)
                existing_kinds_by_source.setdefault(ref, set()).add(kind)

    return sorted(tracking_targets, key=lambda item: item["label"])


def build_policy_tracking(contract, project_dir: Path, permission_graph: PermissionGraph) -> list[PolicyTrackingTarget]:
    """Build event-driven tracking metadata for table-backed authorization policies like canCall."""
    event_lookup = _event_index(contract)
    policy_targets: list[PolicyTrackingTarget] = []

    for function in _contract_functions(contract):
        if getattr(function, "full_name", "") != "canCall(address,address,bytes4)":
            continue

        tracked_state_targets = sorted(
            {
                getattr(variable, "name", "")
                for variable in getattr(function, "state_variables_read", [])
                if getattr(variable, "name", "")
            }
        )
        if not tracked_state_targets:
            continue

        writer_functions, associated_events = _writer_records_for_targets(
            contract,
            project_dir,
            permission_graph,
            tracked_state_targets,
            event_lookup,
        )
        if not writer_functions or not associated_events:
            continue

        policy_targets.append(
            {
                "policy_id": "canCall_policy",
                "label": "canCall policy",
                "policy_function": getattr(function, "full_name", getattr(function, "name", "canCall")),
                "tracked_state_targets": tracked_state_targets,
                "writer_functions": writer_functions,
                "associated_events": associated_events,
                "notes": [
                    "Track authorization-policy mutations through emitted events; "
                    "the underlying table-backed state is non-enumerable for "
                    "generic polling."
                ],
            }
        )

    return sorted(policy_targets, key=lambda item: item["label"])
