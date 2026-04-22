"""Canonical semantic guard summaries for privileged functions."""

from __future__ import annotations

from typing import Any

from schemas.contract_analysis import ContractAnalysis, ControllerRef, GuardRecord, PrivilegedFunction, SinkRecord


def _sinks_by_id(analysis: ContractAnalysis) -> dict[str, SinkRecord]:
    return {sink["id"]: sink for sink in analysis["permission_graph"]["sinks"]}


def _guards_by_id(analysis: ContractAnalysis) -> dict[str, GuardRecord]:
    return {guard["id"]: guard for guard in analysis["permission_graph"]["guards"]}


def _controllers_by_id(analysis: ContractAnalysis) -> dict[str, ControllerRef]:
    return {controller["id"]: controller for controller in analysis["permission_graph"]["controllers"]}


def _helper_name_from_effect_targets(effect_targets: list[str], authority_sources: set[str]) -> str | None:
    for target in effect_targets:
        if "." not in target:
            continue
        left, right = target.split(".", 1)
        if left in authority_sources:
            return right
    return None


def _authority_sources_from_effect_targets(effect_targets: list[str]) -> set[str]:
    sources: set[str] = set()
    for target in effect_targets:
        if "." not in target:
            continue
        left, _ = target.split(".", 1)
        if left:
            sources.add(left)
    return sources


def _guard_ids_for_privileged_function(
    privileged: PrivilegedFunction,
    *,
    sinks_by_id: dict[str, SinkRecord],
) -> list[str]:
    guard_ids: set[str] = set()
    for sink_id in privileged.get("sink_ids", []):
        sink = sinks_by_id.get(sink_id)
        if not sink:
            continue
        guard_ids.update(sink.get("guarded_by", []))
    return sorted(guard_ids)


def build_semantic_guards(analysis: ContractAnalysis) -> dict[str, Any]:
    sinks_by_id = _sinks_by_id(analysis)
    guards_by_id = _guards_by_id(analysis)
    controllers_by_id = _controllers_by_id(analysis)
    controller_kinds_by_source: dict[str, set[str]] = {}
    for controller in analysis["permission_graph"]["controllers"]:
        controller_kinds_by_source.setdefault(controller["source"], set()).add(controller["kind"])

    functions: list[dict[str, Any]] = []
    for privileged in analysis["access_control"]["privileged_functions"]:
        guard_ids = _guard_ids_for_privileged_function(privileged, sinks_by_id=sinks_by_id)
        predicates: list[dict[str, Any]] = []
        notes: list[str] = []
        authority_sources: set[str] = set()
        saw_policy_like_helper = False

        for guard_id in guard_ids:
            guard = guards_by_id.get(guard_id)
            if not guard:
                continue
            controllers = [controllers_by_id[cid] for cid in guard["controller_ids"] if cid in controllers_by_id]

            if guard["kind"] in {"caller_equals_storage", "caller_via_helper_function"}:
                for controller in controllers:
                    predicates.append(
                        {
                            "kind": "caller_equals_controller",
                            "controller_kind": controller["kind"],
                            "controller_label": controller["label"],
                            "controller_source": controller["source"],
                            "read_spec": controller.get("read_spec"),
                        }
                    )
                continue

            if guard["kind"] == "caller_in_mapping":
                for controller in controllers:
                    predicates.append(
                        {
                            "kind": "mapping_membership",
                            "controller_kind": controller["kind"],
                            "controller_label": controller["label"],
                            "controller_source": controller["source"],
                        }
                    )
                continue

            if guard["kind"] == "external_authority_check":
                for controller in controllers:
                    authority_sources.add(controller["source"])
                if "policy_like_args" in guard.get("details", []):
                    saw_policy_like_helper = True
                continue

            if guard["kind"] == "role_membership_check":
                for controller in controllers:
                    predicates.append(
                        {
                            "kind": "role_member",
                            "role_source": controller["source"],
                            "authority_source": None,
                            "read_spec": controller.get("read_spec"),
                        }
                    )

        if len(authority_sources) == 1:
            authority_source = next(iter(authority_sources))
            for predicate in predicates:
                if predicate["kind"] == "role_member" and predicate["authority_source"] is None:
                    predicate["authority_source"] = authority_source

        if not authority_sources:
            authority_sources = {
                ref
                for ref in privileged.get("controller_refs", [])
                if "external_contract" in controller_kinds_by_source.get(ref, set())
            }
        if not authority_sources:
            inferred_sources = _authority_sources_from_effect_targets(list(privileged.get("effect_targets", [])))
            authority_sources = {
                source for source in inferred_sources if source in set(privileged.get("controller_refs", []))
            }

        if authority_sources and not any(
            predicate["kind"] in {"caller_equals_controller", "mapping_membership", "role_member"}
            for predicate in predicates
        ):
            helper = _helper_name_from_effect_targets(list(privileged.get("effect_targets", [])), authority_sources)
            if saw_policy_like_helper:
                predicates.append(
                    {
                        "kind": "policy_check",
                        "authority_source": sorted(authority_sources),
                        "helper": helper,
                        "status": "unresolved",
                    }
                )
                notes.append(
                    "External policy-like helper was detected but not reduced to a canonical policy predicate."
                )
            else:
                predicates.append(
                    {
                        "kind": "external_helper",
                        "authority_source": sorted(authority_sources),
                        "helper": helper,
                        "status": "unresolved",
                    }
                )
                notes.append("External authority helper was detected but not reduced to a canonical caller predicate.")

        status = "resolved"
        if privileged.get("authority_public"):
            status = "public"
        elif not predicates:
            status = "unresolved"
            notes.append("No canonical semantic guard predicate was derived.")
        elif any(predicate.get("status") == "unresolved" for predicate in predicates):
            status = "partial"

        functions.append(
            {
                "function": privileged["function"],
                "status": status,
                "predicates": predicates,
                "guard_ids": guard_ids,
                "guard_kinds": list(privileged.get("guard_kinds", [])),
                "controller_refs": list(privileged.get("controller_refs", [])),
                "notes": notes,
            }
        )

    return {
        "schema_version": "0.1",
        "contract_address": analysis["subject"]["address"],
        "contract_name": analysis["subject"]["name"],
        "functions": functions,
    }
