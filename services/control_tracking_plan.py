"""Compile runtime control-tracking plans from structured contract analysis."""

from __future__ import annotations

import json
from pathlib import Path

from schemas.contract_analysis import ContractAnalysis
from schemas.control_tracking import ControlTrackingPlan, EventWatch, PollingFallback, TrackedController, TrackedPolicy


def build_control_tracking_plan(analysis: ContractAnalysis) -> ControlTrackingPlan:
    """Build an event-first, polling-backed watch plan from contract analysis output."""
    contract_address = analysis["subject"]["address"]
    contract_name = analysis["subject"]["name"]

    tracked_controllers: list[TrackedController] = []
    tracked_policies: list[TrackedPolicy] = []
    for target in analysis.get("controller_tracking", []):
        associated_events = list(target.get("associated_events", []))
        writer_functions = [item["function"] for item in target.get("writer_functions", [])]

        event_watch: EventWatch | None = None
        if associated_events:
            event_watch = {
                "transport": "wss_logs",
                "contract_address": contract_address,
                "events": associated_events,
                "writer_functions": writer_functions,
            }

        cadence = "state_only"
        if target["tracking_mode"] == "event_plus_state":
            cadence = "realtime_confirm"
        elif target["tracking_mode"] == "manual_review":
            cadence = "periodic_reconciliation"

        polling_fallback: PollingFallback = {
            "contract_address": contract_address,
            "polling_sources": list(target.get("polling_sources", [])),
            "cadence": cadence,
            "notes": list(target.get("notes", [])),
        }

        tracked_controllers.append(
            {
                "controller_id": target["controller_id"],
                "label": target["label"],
                "source": target["source"],
                "kind": target["kind"],
                "tracking_mode": target["tracking_mode"],
                "event_watch": event_watch,
                "polling_fallback": polling_fallback,
                "notes": list(target.get("notes", [])),
            }
        )

    for policy in analysis.get("policy_tracking", []):
        tracked_policies.append(
            {
                "policy_id": policy["policy_id"],
                "label": policy["label"],
                "policy_function": policy["policy_function"],
                "tracked_state_targets": list(policy.get("tracked_state_targets", [])),
                "event_watch": {
                    "transport": "wss_logs",
                    "contract_address": contract_address,
                    "events": list(policy.get("associated_events", [])),
                    "writer_functions": [item["function"] for item in policy.get("writer_functions", [])],
                },
                "notes": list(policy.get("notes", [])),
            }
        )

    return {
        "schema_version": "0.1",
        "contract_address": contract_address,
        "contract_name": contract_name,
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": sorted(tracked_controllers, key=lambda item: item["label"]),
        "tracked_policies": sorted(tracked_policies, key=lambda item: item["label"]),
    }


def build_control_tracking_plan_from_file(contract_analysis_path: Path) -> ControlTrackingPlan:
    """Load a contract_analysis.json file and build a runtime tracking plan."""
    analysis = json.loads(contract_analysis_path.read_text())
    return build_control_tracking_plan(analysis)


def write_control_tracking_plan(contract_analysis_path: Path, output_path: Path | None = None) -> Path:
    """Write a control_tracking_plan.json file next to contract_analysis.json by default."""
    plan = build_control_tracking_plan_from_file(contract_analysis_path)
    if output_path is None:
        output_path = contract_analysis_path.with_name("control_tracking_plan.json")
    output_path.write_text(json.dumps(plan, indent=2) + "\n")
    return output_path
