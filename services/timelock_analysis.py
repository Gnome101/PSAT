"""Slither AST + LLM analysis for timelock patterns."""

import json
import re
from pathlib import Path

from slither.slither import Slither
from slither.core.declarations.solidity_variables import SolidityVariableComposed
from slither.core.cfg.node import NodeType

from utils.nim import chat
from utils.source_loader import load_sources

# Known timelock base contracts
TIMELOCK_BASES = {
    "TimelockController", "TimelockControllerUpgradeable",
    "GovernorTimelockControl", "GovernorTimelockControlUpgradeable",
}

# ---------------------------------------------------------------------------
# Slither layer
# ---------------------------------------------------------------------------


def _slither_scan(project_dir: Path) -> dict:
    """Use Slither's Python API to detect timelock patterns."""
    sl = Slither(str(project_dir))

    timelock_contracts: list[str] = []
    delay_variables: list[str] = []
    time_storage_vars: set[str] = set()   # vars that store block.timestamp values
    scheduling_functions: list[dict] = []
    time_gated_functions: list[dict] = []
    delay_modifier_functions: list[dict] = []
    admin_functions_without_timelock: list[dict] = []

    for contract in sl.contracts:
        if contract.is_library:
            continue

        # --- Check inheritance for known timelock contracts ---
        for parent in contract.inheritance:
            if parent.name in TIMELOCK_BASES and parent.name not in timelock_contracts:
                timelock_contracts.append(parent.name)

        # --- Pass 1: find delay variables and time storage vars ---
        # Walk all functions to find where block.timestamp is written to storage
        for func in contract.functions:
            for node in func.nodes:
                has_ts = any(
                    isinstance(v, SolidityVariableComposed)
                    and v.name == "block.timestamp"
                    for v in node.solidity_variables_read
                )
                if not has_ts:
                    continue

                written = {v.name for v in node.state_variables_written}
                read_sv = {v.name for v in node.state_variables_read}

                if written:
                    # These vars hold timestamps (e.g. queuedTransactions[h] = block.timestamp + delay)
                    time_storage_vars.update(written)
                    # Vars read alongside block.timestamp in a write are delay/duration vars
                    for v in read_sv:
                        if v not in time_storage_vars and v not in delay_variables:
                            delay_variables.append(v)

        # --- Pass 2: classify each declared function ---
        for func in contract.functions_declared:
            if func.is_constructor or func.visibility in ("private", "internal"):
                continue

            is_view = func.view or func.pure
            if is_view:
                continue

            schedules = False
            is_time_gated = False
            modifies_delay = False
            timestamp_expressions: list[str] = []

            for node in func.nodes:
                has_ts = any(
                    isinstance(v, SolidityVariableComposed)
                    and v.name == "block.timestamp"
                    for v in node.solidity_variables_read
                )
                if has_ts:
                    written = {v.name for v in node.state_variables_written}
                    read_sv = {v.name for v in node.state_variables_read}
                    expr = str(node.expression)[:200]
                    timestamp_expressions.append(expr)

                    # Scheduling: writes block.timestamp to a time storage var
                    if written & time_storage_vars:
                        schedules = True

                    # Time-gating: reads block.timestamp in a require/if
                    # comparing against a time storage var
                    if (node.contains_require_or_assert() or node.type == NodeType.IF):
                        if read_sv & time_storage_vars:
                            is_time_gated = True

            # Check if function writes delay variables
            for sv in func.state_variables_written:
                if sv.name in delay_variables:
                    modifies_delay = True

            func_info = {
                "function": func.full_name,
                "contract": contract.name,
                "protected": func.is_protected(),
                "modifiers": [m.name for m in func.modifiers],
            }

            if schedules:
                func_info["timestamp_expressions"] = timestamp_expressions
                scheduling_functions.append(func_info)
            elif is_time_gated:
                func_info["timestamp_expressions"] = timestamp_expressions
                time_gated_functions.append(func_info)
            elif modifies_delay:
                delay_modifier_functions.append(func_info)
            elif func.is_protected():
                # Protected admin function with no timelock
                admin_functions_without_timelock.append(func_info)

    has_timelock = bool(
        timelock_contracts
        or (time_storage_vars and (scheduling_functions or time_gated_functions))
    )

    timelock_type = "none"
    if timelock_contracts:
        timelock_type = "openzeppelin"
    elif scheduling_functions and time_gated_functions:
        timelock_type = "custom"
    elif time_gated_functions:
        timelock_type = "delay_pattern"

    return {
        "has_timelock": has_timelock,
        "timelock_type": timelock_type,
        "timelock_contracts": timelock_contracts,
        "delay_variables": delay_variables,
        "time_storage_variables": sorted(time_storage_vars),
        "scheduling_functions": scheduling_functions,
        "time_gated_functions": time_gated_functions,
        "delay_modifier_functions": delay_modifier_functions,
        "admin_functions_without_timelock": admin_functions_without_timelock,
    }


# ---------------------------------------------------------------------------
# LLM layer
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a smart contract timelock analyst. You will receive:
1. Solidity source code
2. Structural findings from Slither static analysis about timelock patterns
3. Privilege analysis results (if available)

Produce a JSON object (no markdown fences) with these keys:
- "has_timelock": boolean
- "timelock_type": "openzeppelin" | "custom" | "delay_pattern" | "none"
- "timelock_parameters": {"min_delay": str, "max_delay": str, "configurable": boolean}
- "timelocked_functions": [{"function": str, "delay": str, "description": str}]
- "unprotected_admin_functions": [{"function": str, "risk_level": "high"|"medium"|"low", "reason": str}]
- "custom_patterns": list of custom timelock patterns Slither may have missed
- "adequacy_assessment": whether timelock durations are appropriate for the privilege level
- "summary": 2-3 sentence summary
"""


def _llm_enrich(source_code: str, slither_findings: dict,
                privilege_data: dict | None = None,
                model: str | None = None) -> dict:
    """Send source + Slither findings to LLM for enrichment."""
    privilege_section = ""
    if privilege_data:
        privilege_section = f"""
## Privilege Analysis
```json
{json.dumps(privilege_data, indent=2)}
```
"""

    user_message = f"""## Slither Structural Findings
```json
{json.dumps(slither_findings, indent=2)}
```
{privilege_section}
## Source Code
```solidity
{source_code}
```

Analyze the timelock patterns. Confirm or correct the Slither findings, identify custom timelock mechanisms missed, and assess whether admin functions have adequate time delays. Return ONLY valid JSON."""

    kwargs = {}
    if model:
        kwargs["model"] = model

    raw = chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        max_tokens=4096,
        **kwargs,
    )

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        json_match = re.search(r"```(?:json)?\s*\n(.*?)```", raw, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        return {"llm_raw": raw, "parse_error": True}


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

def _generate_markdown(results: dict) -> str:
    """Generate a human-readable markdown report."""
    lines = ["# Timelock Analysis\n"]

    lines.append(f"**Has Timelock:** {'Yes' if results.get('has_timelock') else 'No'}")
    lines.append(f"**Type:** {results.get('timelock_type', 'none')}\n")

    if results.get("delay_variables"):
        lines.append("## Delay Variables")
        for v in results["delay_variables"]:
            lines.append(f"- `{v}`")

    params = results.get("timelock_parameters", {})
    if params:
        lines.append("\n## Timelock Parameters")
        for k, v in params.items():
            lines.append(f"- **{k}:** {v}")

    lines.append("\n## Scheduling Functions")
    sched = results.get("scheduling_functions", [])
    if sched:
        for sf in sched:
            lines.append(f"- **{sf['function']}** ({sf['contract']})")
    else:
        lines.append("None detected.")

    lines.append("\n## Time-Gated Functions")
    gated = results.get("time_gated_functions", [])
    timelocked = results.get("timelocked_functions", [])
    if gated:
        for gf in gated:
            lines.append(f"- **{gf['function']}** ({gf['contract']})")
    elif timelocked:
        lines.append("| Function | Delay | Description |")
        lines.append("|----------|-------|-------------|")
        for tf in timelocked:
            lines.append(f"| {tf['function']} | {tf.get('delay', '?')} | {tf.get('description', '')} |")
    else:
        lines.append("No time-gated functions detected.")

    lines.append("\n## Admin Functions Without Timelock")
    no_tl = results.get("admin_functions_without_timelock", [])
    unprotected = results.get("unprotected_admin_functions", [])
    if no_tl:
        lines.append("| Function | Contract | Protected |")
        lines.append("|----------|----------|-----------|")
        for af in no_tl:
            lines.append(f"| {af['function']} | {af['contract']} | {af['protected']} |")
    elif unprotected:
        lines.append("| Function | Risk Level | Reason |")
        lines.append("|----------|------------|--------|")
        for uf in unprotected:
            lines.append(f"| {uf['function']} | {uf.get('risk_level', '?')} | {uf.get('reason', '')} |")
    else:
        lines.append("None detected.")

    adequacy = results.get("adequacy_assessment")
    if adequacy:
        lines.append(f"\n## Adequacy Assessment\n{adequacy}")

    summary = results.get("summary")
    if summary:
        lines.append(f"\n## Summary\n{summary}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def analyze_timelocks(project_dir: Path, model: str | None = None) -> dict:
    """Run Slither + LLM timelock analysis. Returns the combined results dict."""
    source_code = load_sources(project_dir)
    if not source_code.strip():
        return {"error": "No Solidity source files found"}

    # Phase 1: Slither structural analysis
    slither_findings = _slither_scan(project_dir)

    # Load privilege data if available
    priv_path = project_dir / "privilege_analysis.json"
    privilege_data = None
    if priv_path.exists():
        try:
            privilege_data = json.loads(priv_path.read_text())
        except json.JSONDecodeError:
            pass

    # Phase 2: LLM enrichment
    try:
        llm_findings = _llm_enrich(source_code, slither_findings,
                                   privilege_data=privilege_data, model=model)
    except Exception as exc:
        llm_findings = {"llm_error": str(exc)}

    # Merge
    results = {**slither_findings}
    if not llm_findings.get("parse_error") and not llm_findings.get("llm_error"):
        for key in ("has_timelock", "timelock_type"):
            if key in llm_findings:
                results[key] = llm_findings[key]
        for key in ("timelock_parameters", "timelocked_functions",
                     "unprotected_admin_functions", "custom_patterns",
                     "adequacy_assessment", "summary"):
            if key in llm_findings:
                results[key] = llm_findings[key]

    # Write outputs
    json_path = project_dir / "timelock_analysis.json"
    json_path.write_text(json.dumps(results, indent=2) + "\n")

    md_path = project_dir / "timelock_analysis.md"
    md_path.write_text(_generate_markdown(results))

    return results
