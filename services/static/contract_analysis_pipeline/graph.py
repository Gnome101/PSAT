"""Permission graph extraction for contract analysis."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

from schemas.contract_analysis import ControllerRef, GuardRecord, PermissionGraph, SinkRecord

from .shared import (
    _declaring_contract_name,
    _dedupe_strings,
    _entry_points,
    _function_effects,
    _is_mapping_variable,
    _node_contains_require_or_assert,
    _source_evidence,
)


def _effects_for_sink(function, variable_name: str) -> list[str]:
    effects = _function_effects(function)
    if effects:
        return effects
    return [f"writes:{variable_name}"]


def _controller_id(kind: str, source: str) -> str:
    return f"{kind}:{source}"


def _guard_id(function_name: str, node_id: int | str, kind: str, controller_ids: list[str]) -> str:
    controller_suffix = ",".join(sorted(controller_ids))
    return f"{function_name}:node{node_id}:{kind}:{controller_suffix}"


def _sink_id(function_name: str, node_id: int | str, kind: str, target: str) -> str:
    return f"{function_name}:node{node_id}:{kind}:{target}"


def _node_type_name(node) -> str:
    return getattr(getattr(node, "type", None), "name", "")


def _variable_name(item) -> str:
    if item is None:
        return ""
    return getattr(item, "name", None) or str(item)


def _type_name(item) -> str:
    if item is None:
        return ""
    return str(getattr(item, "type", None) or "")


def _is_bytes32_typed(item) -> bool:
    return _type_name(item) == "bytes32"


def _is_bool_typed(item) -> bool:
    return _type_name(item) == "bool"


def _lower_camel(value: str) -> str:
    if not value:
        return value
    return value[0].lower() + value[1:]


def _helper_getter_target(source: str, suffix: str) -> str:
    explicit = {
        "owner": "owner",
        "authority": "authority",
        "governance": "getGovernance",
        "adminExecutor": "getAdminExecutor",
        "emergencyActivationCommittee": "getEmergencyActivationCommittee",
        "emergencyExecutionCommittee": "getEmergencyExecutionCommittee",
        "emergencyGovernance": "getEmergencyGovernance",
    }
    return explicit.get(source, f"get{suffix}")


def _resolve_state_source(item, state_aliases: dict[str, str]) -> str | None:
    if item is None:
        return None
    if type(item).__name__ == "StateVariable":
        return getattr(item, "name", None)
    return state_aliases.get(_variable_name(item))


def _looks_like_external_authority_call(function_name: str, destination_name: str) -> bool:
    function_lower = function_name.lower()
    destination_lower = destination_name.lower()
    if any(
        token in function_lower
        for token in (
            "cancall",
            "haspermission",
            "canperform",
            "hasrole",
            "checkrole",
            "authorize",
            "authoriz",
            "isauthorized",
            "ispermitted",
            "caninvoke",
        )
    ):
        return True
    return function_lower.startswith("can") and any(
        token in destination_lower for token in ("authority", "auth", "acl", "kernel", "accessmanager")
    )


def _looks_like_external_helper_guard_call(ir, caller_aliases: set[str], destination_name: str | None) -> bool:
    if type(ir).__name__ != "HighLevelCall":
        return False
    if not destination_name:
        return False
    arguments = list(getattr(ir, "arguments", []) or [])
    if not any(_variable_name(argument) in caller_aliases for argument in arguments):
        return False
    function = getattr(ir, "function", None)
    if not function:
        return False
    is_view_like = bool(getattr(function, "view", False) or getattr(function, "pure", False))
    if not is_view_like:
        return False
    return getattr(ir, "lvalue", None) is None


def _looks_like_policy_helper_call(arguments: list[Any], caller_aliases: set[str]) -> bool:
    has_caller = any(_variable_name(argument) in caller_aliases for argument in arguments)
    if not has_caller:
        return False
    non_caller = [argument for argument in arguments if _variable_name(argument) not in caller_aliases]
    has_address = any(_type_name(argument) == "address" for argument in non_caller)
    has_bytes4 = any(_type_name(argument) == "bytes4" for argument in non_caller)
    return has_address and has_bytes4


def _resolve_role_source(item, role_aliases: dict[str, str]) -> str | None:
    name = _variable_name(item)
    if not name:
        return None
    if name in role_aliases:
        return role_aliases[name]
    if not _is_bytes32_typed(item):
        return None
    if name.startswith("TMP_"):
        return None
    return name


def _role_sources_from_arguments(
    arguments: list[Any],
    caller_aliases: set[str],
    role_aliases: dict[str, str],
) -> list[str]:
    sources: list[str] = []
    for argument in arguments:
        if _variable_name(argument) in caller_aliases:
            continue
        source = _resolve_role_source(argument, role_aliases)
        if source and source not in sources:
            sources.append(source)
    return sources


def _update_role_aliases_for_ir(ir, role_aliases: dict[str, str]) -> None:
    lvalue_name = _variable_name(getattr(ir, "lvalue", None))
    if type(ir).__name__ == "Assignment":
        reads = list(getattr(ir, "read", []) or [])
        if lvalue_name and len(reads) == 1:
            source = _resolve_role_source(reads[0], role_aliases)
            if source:
                role_aliases[lvalue_name] = source
        return

    if type(ir).__name__ not in {"HighLevelCall", "InternalCall", "LibraryCall"}:
        return
    if not lvalue_name or not _is_bytes32_typed(getattr(ir, "lvalue", None)):
        return

    arguments = list(getattr(ir, "arguments", []) or [])
    function_name = getattr(ir, "function_name", None) or getattr(getattr(ir, "function", None), "name", None)
    if not arguments and function_name:
        role_aliases[lvalue_name] = str(function_name)
        return

    if len(arguments) == 1:
        source = _resolve_role_source(arguments[0], role_aliases)
        if source:
            role_aliases[lvalue_name] = source


def _updated_role_aliases(node, role_aliases: dict[str, str]) -> dict[str, str]:
    aliases = dict(role_aliases)
    for ir in getattr(node, "irs", []) or []:
        _update_role_aliases_for_ir(ir, aliases)
    return aliases


def _updated_state_aliases(node, state_aliases: dict[str, str]) -> dict[str, str]:
    aliases = dict(state_aliases)
    for ir in getattr(node, "irs", []):
        if type(ir).__name__ != "Assignment":
            continue
        lvalue_name = _variable_name(getattr(ir, "lvalue", None))
        reads = list(getattr(ir, "read", []) or [])
        if not lvalue_name or len(reads) != 1:
            continue
        source = reads[0]
        source_name = None
        if type(source).__name__ == "StateVariable":
            source_name = getattr(source, "name", None)
        else:
            source_name = state_aliases.get(_variable_name(source))
        if source_name:
            aliases[lvalue_name] = source_name
    return aliases


def _propagated_caller_aliases(ir, caller_aliases: set[str]) -> set[str]:
    aliases = set(caller_aliases)
    callee = getattr(ir, "function", None)
    if callee is None:
        return aliases

    parameters = list(getattr(callee, "parameters", []) or [])
    arguments = list(getattr(ir, "arguments", []) or [])
    for parameter, argument in zip(parameters, arguments):
        if _variable_name(argument) in caller_aliases and getattr(parameter, "name", ""):
            aliases.add(parameter.name)
    return aliases


def _propagated_role_aliases(ir, role_aliases: dict[str, str]) -> dict[str, str]:
    aliases = dict(role_aliases)
    callee = getattr(ir, "function", None)
    if callee is None:
        return aliases

    parameters = list(getattr(callee, "parameters", []) or [])
    arguments = list(getattr(ir, "arguments", []) or [])
    for parameter, argument in zip(parameters, arguments):
        if not getattr(parameter, "name", ""):
            continue
        source = _resolve_role_source(argument, role_aliases)
        if source:
            aliases[parameter.name] = source
    return aliases


def _binary_reads_caller_and_variable(node, variable_name: str) -> bool:
    for ir in getattr(node, "irs", []):
        if type(ir).__name__ != "Binary":
            continue
        reads = {str(item) for item in getattr(ir, "read", [])}
        if "msg.sender" in reads and variable_name in reads:
            return True
    return False


def _direct_sink_candidates(function, unit, node, project_dir: Path) -> list[dict[str, Any]]:
    sinks: list[dict[str, Any]] = []
    is_conditional_node = _node_contains_require_or_assert(node) or _node_type_name(node) in {"IF", "IFLOOP"}

    for variable in getattr(node, "state_variables_written", []):
        if not getattr(variable, "name", ""):
            continue
        sinks.append(
            {
                "kind": "state_write",
                "target": variable.name,
                "effects": _effects_for_sink(function, variable.name),
                "evidence": [_source_evidence(node, project_dir, detail=f"state write to {variable.name}")],
            }
        )

    for ir in getattr(node, "irs", []):
        if type(ir).__name__ != "NewContract":
            if type(ir).__name__ == "HighLevelCall":
                if is_conditional_node:
                    continue
                destination = getattr(ir, "destination", None)
                destination_name = getattr(destination, "name", None) or str(destination) or "unknown"
                function_name = getattr(ir, "function_name", None) or "call"
                if unit is not function and function_name == "canCall":
                    continue
                target = f"{destination_name}.{function_name}"
                sinks.append(
                    {
                        "kind": "external_call",
                        "target": target,
                        "effects": ["privileged_external_call"],
                        "evidence": [_source_evidence(node, project_dir, detail=f"external call to {target}")],
                    }
                )
                continue
            if type(ir).__name__ == "LibraryCall":
                if is_conditional_node:
                    continue
                function_name = getattr(ir, "function_name", None) or "call"
                arguments = list(getattr(ir, "arguments", []) or [])
                destination_name = (
                    _variable_name(arguments[0]) if arguments else _variable_name(getattr(ir, "destination", None))
                )
                target = f"{destination_name}.{function_name}"
                sinks.append(
                    {
                        "kind": "external_call",
                        "target": target,
                        "effects": ["privileged_external_call"],
                        "evidence": [_source_evidence(node, project_dir, detail=f"external call to {target}")],
                    }
                )
                continue
            if type(ir).__name__ == "LowLevelCall":
                if is_conditional_node:
                    continue
                target = getattr(getattr(ir, "destination", None), "name", None) or str(
                    getattr(ir, "destination", None) or "unknown"
                )
                function_name = str(getattr(ir, "function_name", "") or "")
                if function_name == "delegatecall":
                    sinks.append(
                        {
                            "kind": "delegatecall",
                            "target": target,
                            "effects": ["delegatecall_control"],
                            "evidence": [_source_evidence(node, project_dir, detail=f"delegatecall to {target}")],
                        }
                    )
                continue
            if type(ir).__name__ == "SolidityCall":
                if is_conditional_node:
                    continue
                function_name = getattr(getattr(ir, "function", None), "name", "")
                if function_name.startswith("selfdestruct("):
                    sinks.append(
                        {
                            "kind": "selfdestruct",
                            "target": "selfdestruct",
                            "effects": ["selfdestruct_capability"],
                            "evidence": [_source_evidence(node, project_dir, detail="selfdestruct capability")],
                        }
                    )
                continue
            continue
        target = getattr(ir, "contract_name", None) or str(getattr(ir, "contract_created", "")) or "unknown"
        sinks.append(
            {
                "kind": "contract_creation",
                "target": str(target),
                "effects": ["factory_deployment"],
                "evidence": [_source_evidence(node, project_dir, detail=f"contract creation of {target}")],
            }
        )

    return sinks


def _collect_sink_instances(
    root_function,
    unit,
    project_dir: Path,
    seen: set[str],
    guard_contexts: list[tuple[Any, Any]] | None = None,
) -> list[dict[str, Any]]:
    instances: list[dict[str, Any]] = []
    guard_contexts = list(guard_contexts or [])

    for node in getattr(unit, "nodes", []):
        current_contexts = [*guard_contexts, (unit, node)]
        for sink in _direct_sink_candidates(root_function, unit, node, project_dir):
            instances.append(
                {
                    "sink": sink,
                    "sink_unit": unit,
                    "sink_node": node,
                    "guard_contexts": current_contexts,
                }
            )

        for ir in getattr(node, "irs", []):
            if type(ir).__name__ not in {"InternalCall", "LibraryCall"}:
                continue
            callee = getattr(ir, "function", None)
            if callee is None:
                continue
            callee_name = getattr(callee, "canonical_name", None) or getattr(callee, "full_name", None) or str(callee)
            if callee_name in seen:
                continue
            instances.extend(
                _collect_sink_instances(
                    root_function,
                    callee,
                    project_dir,
                    {callee_name, *seen},
                    current_contexts,
                )
            )

    return instances


def _index_reads_caller(node, caller_aliases: set[str]) -> bool:
    for ir in getattr(node, "irs", []):
        if type(ir).__name__ != "Index":
            continue
        if any(_variable_name(item) in caller_aliases for item in getattr(ir, "read", [])):
            return True
    return False


def _direct_structural_guards(
    node,
    project_dir: Path,
    caller_aliases: set[str],
    state_aliases: dict[str, str],
    role_aliases: dict[str, str],
) -> list[dict[str, Any]]:
    guards: list[dict[str, Any]] = []
    state_reads = [variable for variable in getattr(node, "state_variables_read", []) if getattr(variable, "name", "")]
    evidence = [_source_evidence(node, project_dir, detail=f"guard node {node.node_id}")]
    high_level_calls = [ir for ir in getattr(node, "irs", []) if type(ir).__name__ == "HighLevelCall"]
    has_external_helper_guard = any(
        _looks_like_external_helper_guard_call(
            ir,
            caller_aliases,
            _resolve_state_source(getattr(ir, "destination", None), state_aliases)
            or _variable_name(getattr(ir, "destination", None)),
        )
        for ir in high_level_calls
    )
    is_guard_source = (
        _node_contains_require_or_assert(node)
        or _node_type_name(node) in {"IF", "IFLOOP", "RETURN"}
        or has_external_helper_guard
    )
    if not is_guard_source:
        return guards

    local_role_aliases = dict(role_aliases)
    for ir in getattr(node, "irs", []):
        if type(ir).__name__ == "HighLevelCall":
            destination = getattr(ir, "destination", None)
            destination_name = _resolve_state_source(destination, state_aliases) or _variable_name(destination)
            function_name = getattr(ir, "function_name", None) or getattr(getattr(ir, "function", None), "name", None)
            arguments = list(getattr(ir, "arguments", []) or [])
            argument_names = {_variable_name(argument) for argument in arguments}
            role_sources = _role_sources_from_arguments(arguments, caller_aliases, local_role_aliases)
            if (
                destination_name
                and function_name
                and any(name in caller_aliases for name in argument_names)
                and (
                    _looks_like_external_authority_call(str(function_name), destination_name)
                    or _is_bool_typed(getattr(ir, "lvalue", None))
                    or _looks_like_external_helper_guard_call(ir, caller_aliases, destination_name)
                )
            ):
                guards.append(
                    {
                        "kind": "external_authority_check",
                        "controllers": [
                            {
                                "kind": "external_contract",
                                "label": destination_name,
                                "source": destination_name,
                                "evidence": [_source_evidence(destination or node, project_dir)],
                            }
                        ],
                        "evidence": evidence,
                        "details": [
                            f"node:{node.node_id}",
                            "external call guard",
                            *(
                                ["policy_like_args"]
                                if _looks_like_policy_helper_call(arguments, caller_aliases)
                                else []
                            ),
                        ],
                    }
                )
                if role_sources:
                    guards.append(
                        {
                            "kind": "role_membership_check",
                            "controllers": [
                                {
                                    "kind": "role_identifier",
                                    "label": source,
                                    "source": source,
                                    "evidence": [
                                        _source_evidence(
                                            argument,
                                            project_dir,
                                            detail=f"role source {source}",
                                        )
                                    ],
                                }
                                for source, argument in (
                                    (
                                        source,
                                        next(
                                            arg
                                            for arg in arguments
                                            if _resolve_role_source(arg, local_role_aliases) == source
                                        ),
                                    )
                                    for source in role_sources
                                )
                            ],
                            "evidence": evidence,
                            "details": [f"node:{node.node_id}", "external role guard"],
                        }
                    )
            _update_role_aliases_for_ir(ir, local_role_aliases)
            continue

        if type(ir).__name__ != "Binary":
            _update_role_aliases_for_ir(ir, local_role_aliases)
            continue

        left = getattr(ir, "variable_left", None)
        right = getattr(ir, "variable_right", None)
        left_name = _variable_name(left)
        right_name = _variable_name(right)
        left_state = _resolve_state_source(left, state_aliases) or (
            getattr(left, "name", None) if type(left).__name__ == "StateVariable" else None
        )
        right_state = _resolve_state_source(right, state_aliases) or (
            getattr(right, "name", None) if type(right).__name__ == "StateVariable" else None
        )

        if left_name in caller_aliases and right_state:
            guards.append(
                {
                    "kind": "caller_equals_storage",
                    "controllers": [
                        {
                            "kind": "state_variable",
                            "label": right_state,
                            "source": right_state,
                            "evidence": [_source_evidence(right or node, project_dir)],
                        }
                    ],
                    "evidence": evidence,
                    "details": [f"node:{node.node_id}", "msg.sender equality to storage-derived state"],
                }
            )
        elif right_name in caller_aliases and left_state:
            guards.append(
                {
                    "kind": "caller_equals_storage",
                    "controllers": [
                        {
                            "kind": "state_variable",
                            "label": left_state,
                            "source": left_state,
                            "evidence": [_source_evidence(left or node, project_dir)],
                        }
                    ],
                    "evidence": evidence,
                    "details": [f"node:{node.node_id}", "msg.sender equality to storage-derived state"],
                }
            )
        _update_role_aliases_for_ir(ir, local_role_aliases)

    mapping_reads = [variable for variable in state_reads if _is_mapping_variable(variable)]
    if mapping_reads and _index_reads_caller(node, caller_aliases):
        for variable in mapping_reads:
            guards.append(
                {
                    "kind": "caller_in_mapping",
                    "controllers": [
                        {
                            "kind": "mapping_membership",
                            "label": variable.name,
                            "source": variable.name,
                            "evidence": [_source_evidence(variable, project_dir)],
                        }
                    ],
                    "evidence": evidence,
                    "details": [f"node:{node.node_id}", "msg.sender indexed mapping check"],
                }
            )

    return guards


def _guards_from_internal_call(ir, project_dir: Path, seen: set[str], caller_aliases: set[str]) -> list[dict[str, Any]]:
    return _guards_from_internal_call_with_roles(ir, project_dir, seen, caller_aliases, {})


def _guards_from_internal_call_with_roles(
    ir,
    project_dir: Path,
    seen: set[str],
    caller_aliases: set[str],
    role_aliases: dict[str, str],
) -> list[dict[str, Any]]:
    guards: list[dict[str, Any]] = []
    callee = getattr(ir, "function", None)
    if callee is None:
        return guards

    callee_name = getattr(callee, "canonical_name", None) or getattr(callee, "full_name", None) or str(callee)
    if callee_name in seen:
        return guards

    helper_name = getattr(callee, "name", None) or ""
    helper_match = re.match(r"^checkCallerIs([A-Z][A-Za-z0-9_]*)$", helper_name)
    if helper_match:
        suffix = helper_match.group(1)
        source = _lower_camel(suffix)
        guards.append(
            {
                "kind": "caller_via_helper_function",
                "confidence": "high",
                "controllers": [
                    {
                        "kind": "state_variable",
                        "label": source,
                        "source": source,
                        "read_spec": {
                            "strategy": "getter_call",
                            "target": _helper_getter_target(source, suffix),
                        },
                        "confidence": "high",
                        "evidence": [_source_evidence(callee, project_dir, detail=f"helper {helper_name}")],
                    }
                ],
                "evidence": [_source_evidence(callee, project_dir, detail=f"via {callee_name}")],
                "details": [f"via:{callee_name}", f"getter:{_helper_getter_target(source, suffix)}"],
            }
        )

    arguments = [argument for argument in getattr(ir, "arguments", []) or [] if _variable_name(argument)]
    role_sources = _role_sources_from_arguments(arguments, caller_aliases, role_aliases)
    propagated_role_aliases = _propagated_role_aliases(ir, role_aliases)

    nested_seen = {callee_name, *seen}
    nested_guards = _structural_guards_for_unit(
        callee,
        project_dir,
        nested_seen,
        _propagated_caller_aliases(ir, caller_aliases),
        propagated_role_aliases,
    )
    nested_role_sources = {
        controller["source"]
        for guard in nested_guards
        if guard.get("kind") == "role_membership_check"
        for controller in guard.get("controllers", [])
        if controller.get("source")
    }
    callee_type = type(callee).__name__
    if (
        role_sources
        and (
            callee_type == "Modifier"
            or any(
                guard.get("kind") in {"external_authority_check", "role_membership_check"} for guard in nested_guards
            )
        )
        and not set(role_sources).issubset(nested_role_sources)
    ):
        guards.append(
            {
                "kind": "role_membership_check",
                "controllers": [
                    {
                        "kind": "role_identifier",
                        "label": source,
                        "source": source,
                        "evidence": [_source_evidence(argument, project_dir)],
                    }
                    for source, argument in (
                        (source, next(arg for arg in arguments if _resolve_role_source(arg, role_aliases) == source))
                        for source in role_sources
                    )
                ],
                "evidence": [_source_evidence(callee, project_dir, detail=f"via {callee_name}")],
                "details": [f"via:{callee_name}", "propagated role-scoped guard"],
            }
        )

    guards.extend(nested_guards)
    return guards


def _structural_guards_for_unit(
    unit,
    project_dir: Path,
    seen: set[str],
    caller_aliases: set[str],
    role_aliases: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    guards: list[dict[str, Any]] = []
    state_aliases: dict[str, str] = {}
    role_aliases = dict(role_aliases or {})
    for node in getattr(unit, "nodes", []):
        guards.extend(_direct_structural_guards(node, project_dir, caller_aliases, state_aliases, role_aliases))
        for ir in getattr(node, "irs", []):
            if type(ir).__name__ not in {"InternalCall", "LibraryCall"}:
                continue
            guards.extend(_guards_from_internal_call_with_roles(ir, project_dir, seen, caller_aliases, role_aliases))
        state_aliases = _updated_state_aliases(node, state_aliases)
        role_aliases = _updated_role_aliases(node, role_aliases)
    return guards


def _dedupe_guard_candidates(guards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen = set()
    for guard in guards:
        controller_sources = tuple(
            sorted(controller["source"] for controller in guard.get("controllers", []) if controller.get("source"))
        )
        key = (guard.get("kind"), controller_sources, tuple(sorted(guard.get("details", []))))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(guard)
    return deduped


def _guards_for_sink(function, sink_node, project_dir: Path) -> list[dict[str, Any]]:
    guards: list[dict[str, Any]] = []
    state_aliases: dict[str, str] = {}
    role_aliases: dict[str, str] = {}
    for node in sorted(sink_node.dominators, key=lambda item: item.node_id):
        node_type = getattr(node, "type", None)
        if node is sink_node or getattr(node_type, "name", None) == "ENTRYPOINT":
            continue
        guards.extend(_direct_structural_guards(node, project_dir, {"msg.sender"}, state_aliases, role_aliases))
        for ir in getattr(node, "irs", []):
            if type(ir).__name__ not in {"InternalCall", "LibraryCall"}:
                continue
            guards.extend(_guards_from_internal_call_with_roles(ir, project_dir, set(), {"msg.sender"}, role_aliases))
        state_aliases = _updated_state_aliases(node, state_aliases)
        role_aliases = _updated_role_aliases(node, role_aliases)
    return _dedupe_guard_candidates(guards)


def _guard_kind_label(kind: str) -> str:
    if kind == "caller_equals_storage":
        return "storage_guard"
    if kind == "caller_via_helper_function":
        return "helper_guard"
    if kind == "caller_in_mapping":
        return "mapping_guard"
    if kind == "external_authority_check":
        return "external_authority"
    if kind == "role_membership_check":
        return "role"
    return kind


def build_permission_graph(contract, project_dir: Path) -> PermissionGraph:
    controllers: dict[str, ControllerRef] = {}
    guards: dict[str, GuardRecord] = {}
    sinks: list[SinkRecord] = []

    for function in _entry_points(contract):
        sink_instances = _collect_sink_instances(
            function,
            function,
            project_dir,
            {getattr(function, "canonical_name", None) or getattr(function, "full_name", None) or function.name},
        )

        for instance in sink_instances:
            sink_candidate = instance["sink"]
            sink_node = instance["sink_node"]
            sink_unit = instance["sink_unit"]
            sink_token = sink_node.node_id
            if sink_unit is not function:
                sink_token = f"{getattr(sink_unit, 'name', 'internal')}@{sink_node.node_id}"
            guard_candidates: list[dict[str, Any]] = []
            for guard_unit, guard_node in instance["guard_contexts"]:
                guard_candidates.extend(_guards_for_sink(guard_unit, guard_node, project_dir))
            guard_candidates = _dedupe_guard_candidates(guard_candidates)

            guarded_by: list[str] = []
            for candidate in guard_candidates:
                controller_ids = []
                for controller in candidate.get("controllers", []):
                    controller_id = _controller_id(controller["kind"], controller["source"])
                    controllers.setdefault(
                        controller_id,
                        cast(
                            ControllerRef,
                            {
                                "id": controller_id,
                                "kind": controller["kind"],
                                "label": controller["label"],
                                "source": controller["source"],
                                "read_spec": controller.get("read_spec"),
                                "confidence": controller.get("confidence"),
                                "evidence": controller["evidence"],
                            },
                        ),
                    )
                    controller_ids.append(controller_id)

                guard_id = _guard_id(function.full_name, sink_token, candidate["kind"], controller_ids)
                guards.setdefault(
                    guard_id,
                    cast(
                        GuardRecord,
                        {
                            "id": guard_id,
                            "contract": _declaring_contract_name(function, contract.name),
                            "function": getattr(function, "full_name", function.name),
                            "kind": candidate["kind"],
                            "confidence": candidate.get("confidence"),
                            "controller_ids": sorted(controller_ids),
                            "evidence": candidate["evidence"],
                            "details": sorted(set(candidate.get("details", []))),
                        },
                    ),
                )
                guarded_by.append(guard_id)

            sinks.append(
                {
                    "id": _sink_id(function.full_name, sink_token, sink_candidate["kind"], sink_candidate["target"]),
                    "contract": _declaring_contract_name(function, contract.name),
                    "function": getattr(function, "full_name", function.name),
                    "kind": sink_candidate["kind"],
                    "target": sink_candidate["target"],
                    "node_id": sink_node.node_id,
                    "guarded_by": sorted(set(guarded_by)),
                    "effects": sink_candidate["effects"],
                    "evidence": sink_candidate["evidence"],
                }
            )

    return {
        "controllers": sorted(controllers.values(), key=lambda item: item["id"]),
        "guards": sorted(guards.values(), key=lambda item: item["id"]),
        "sinks": sorted(sinks, key=lambda item: item["id"]),
    }


def privileged_functions_from_graph(contract, permission_graph: PermissionGraph) -> dict[str, dict[str, Any]]:
    guards_by_id = {guard["id"]: guard for guard in permission_graph["guards"]}
    controllers_by_id = {controller["id"]: controller for controller in permission_graph["controllers"]}
    by_function: dict[str, dict[str, Any]] = {}

    for sink in permission_graph["sinks"]:
        entry = by_function.setdefault(
            sink["function"],
            {
                "contract": sink["contract"],
                "function": sink["function"],
                "visibility": "unknown",
                "guards": [],
                "guard_kinds": [],
                "controller_refs": [],
                "controller_ids": [],
                "sink_ids": [],
                "effects": [],
                "effect_targets": [],
                "sink_kinds": [],
            },
        )
        entry["sink_ids"].append(sink["id"])
        entry["effects"].extend(sink["effects"])
        entry["effect_targets"].append(sink["target"])
        entry["sink_kinds"].append(sink["kind"])

        for guard_id in sink["guarded_by"]:
            guard = guards_by_id.get(guard_id)
            if guard is None:
                continue
            entry["guard_kinds"].append(guard["kind"])
            entry["guards"].append(_guard_kind_label(guard["kind"]))
            entry["controller_ids"].extend(guard["controller_ids"])

            for controller_id in guard["controller_ids"]:
                controller = controllers_by_id.get(controller_id)
                if controller is None:
                    continue
                entry["controller_refs"].append(controller["source"])
                entry["guards"].append(controller["label"])

    functions_by_signature = {
        getattr(function, "full_name", function.name): function for function in _entry_points(contract)
    }
    for function_signature, entry in by_function.items():
        function = functions_by_signature.get(function_signature)
        if function is not None:
            entry["visibility"] = getattr(function, "visibility", "unknown")
        entry["guards"] = _dedupe_strings(entry["guards"])
        entry["guard_kinds"] = _dedupe_strings(entry["guard_kinds"])
        entry["controller_refs"] = _dedupe_strings(entry["controller_refs"])
        entry["controller_ids"] = sorted(set(entry["controller_ids"]))
        entry["sink_ids"] = sorted(set(entry["sink_ids"]))
        entry["effects"] = _dedupe_strings(entry["effects"])
        entry["effect_targets"] = _dedupe_strings(entry["effect_targets"])
        entry["sink_kinds"] = _dedupe_strings(entry["sink_kinds"])

    return by_function
