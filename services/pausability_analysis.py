"""Slither AST + LLM analysis for pausability patterns."""

import json
import re
from pathlib import Path

from slither.slither import Slither
from slither.core.cfg.node import NodeType

from utils.nim import chat
from utils.source_loader import load_sources

# Keywords that indicate a pause-related state variable
PAUSE_KEYWORDS = {"paused", "_paused", "isPaused", "is_paused"}

# Keywords that indicate a pause-related modifier
PAUSE_MODIFIER_KEYWORDS = {"whenNotPaused", "whenPaused"}

# Known pausable base contracts
PAUSABLE_BASES = {"Pausable", "PausableUpgradeable"}

# ---------------------------------------------------------------------------
# Slither layer
# ---------------------------------------------------------------------------


def _slither_scan(project_dir: Path) -> dict:
    """Use Slither's Python API to detect pausability patterns."""
    sl = Slither(str(project_dir))

    is_pausable = False
    pause_vars: set[str] = set()
    pause_modifiers: set[str] = set()
    pause_functions: dict[str, dict] = {}
    affected_functions: list[dict] = []
    inherits_pausable = False

    for contract in sl.contracts:
        if contract.is_library:
            continue

        # --- Check inheritance for Pausable ---
        for parent in contract.inheritance:
            if parent.name in PAUSABLE_BASES:
                inherits_pausable = True
                is_pausable = True

        # --- Find bool state variables that look pause-related ---
        for sv in contract.state_variables_declared:
            if str(sv.type) == "bool" and sv.name.lower().replace("_", "") in {
                k.lower().replace("_", "") for k in PAUSE_KEYWORDS
            }:
                pause_vars.add(sv.name)
                is_pausable = True

        # --- Collect pause-related modifiers ---
        for mod in contract.modifiers_declared:
            if mod.name in PAUSE_MODIFIER_KEYWORDS:
                pause_modifiers.add(mod.name)
                is_pausable = True

        # --- Find functions that write pause variables (pause/unpause funcs) ---
        for func in contract.functions_declared:
            for sv in func.state_variables_written:
                if sv.name in pause_vars:
                    modifiers = [m.name for m in func.modifiers]
                    is_protected = func.is_protected()
                    pause_functions[func.full_name] = {
                        "access_control": modifiers if modifiers else (
                            ["protected_inline"] if is_protected else ["unrestricted"]
                        ),
                        "contract": contract.name,
                    }

        # --- Find functions gated by pause ---
        for func in contract.functions_declared:
            if func.is_constructor or func.visibility in ("private", "internal"):
                continue

            # Check 1: modifier-based pause gating
            for mod in func.modifiers:
                if mod.name in PAUSE_MODIFIER_KEYWORDS:
                    affected_functions.append({
                        "function": func.full_name,
                        "contract": contract.name,
                        "guard_type": "modifier",
                        "modifier": mod.name,
                    })
                    is_pausable = True

            # Check 2: inline pause checks — function reads a pause var
            # in a require/assert/if node
            for node in func.nodes:
                sv_read = {v.name for v in node.state_variables_read}
                reads_pause = sv_read & pause_vars
                if reads_pause and (
                    node.contains_require_or_assert()
                    or node.type == NodeType.IF
                ):
                    affected_functions.append({
                        "function": func.full_name,
                        "contract": contract.name,
                        "guard_type": "inline",
                        "expression": str(node.expression)[:200],
                        "pause_variable": list(reads_pause),
                    })
                    is_pausable = True

            # Check 3: function calls an external hook that could
            # act as a pause gate (like BoringVault's BeforeTransferHook)
            for node in func.nodes:
                for ext_call in node.external_calls_as_expressions:
                    call_str = str(ext_call).lower()
                    if any(kw in call_str for kw in (
                        "beforetransfer", "pause", "hook",
                    )):
                        affected_functions.append({
                            "function": func.full_name,
                            "contract": contract.name,
                            "guard_type": "external_hook",
                            "expression": str(ext_call)[:200],
                        })
                        is_pausable = True

    return {
        "is_pausable": is_pausable,
        "inherits_pausable": inherits_pausable,
        "pause_state_variables": sorted(pause_vars),
        "pause_functions": pause_functions,
        "affected_functions": affected_functions,
    }


# ---------------------------------------------------------------------------
# LLM layer
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a smart contract pausability analyst. You will receive:
1. Solidity source code
2. Structural findings from Slither static analysis about pause patterns
3. Privilege analysis results (if available)

Produce a JSON object (no markdown fences) with these keys:
- "is_pausable": boolean
- "pause_mechanism": description of how pausing works
- "pause_functions": {"func_name": {"access_control": [str], "description": str}}
- "affected_functions": [{"function": str, "guard_type": str, "impact": str}]
- "custom_pause_patterns": list of custom pause mechanisms Slither may have missed
- "impact_summary": what gets blocked when the contract is paused
- "risk_assessment": assessment of pause-related risks (e.g. centralization)
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

Analyze the pausability patterns. Confirm or correct the Slither findings, identify any custom pause mechanisms missed, and assess the impact of pausing. Return ONLY valid JSON."""

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
    lines = ["# Pausability Analysis\n"]

    lines.append(f"**Pausable:** {'Yes' if results.get('is_pausable') else 'No'}\n")

    mechanism = results.get("pause_mechanism")
    if mechanism:
        lines.append(f"**Mechanism:** {mechanism}\n")

    lines.append("## Pause/Unpause Functions")
    pf = results.get("pause_functions", {})
    if pf:
        for name, info in pf.items():
            ac = ", ".join(info.get("access_control", []))
            desc = info.get("description", "")
            lines.append(f"- **{name}** — access: {ac}")
            if desc:
                lines.append(f"  - {desc}")
    else:
        lines.append("None detected.")

    lines.append("\n## Affected Functions")
    affected = results.get("affected_functions", [])
    if affected:
        lines.append("| Function | Guard Type | Details |")
        lines.append("|----------|-----------|---------|")
        for af in affected:
            guard = af.get("guard_type", "")
            detail = af.get("modifier", "") or af.get("expression", "") or af.get("impact", "")
            lines.append(f"| {af['function']} | {guard} | {detail} |")
    else:
        lines.append("No functions with pause guards detected.")

    impact = results.get("impact_summary")
    if impact:
        lines.append(f"\n## Impact Summary\n{impact}")

    risk = results.get("risk_assessment")
    if risk:
        lines.append(f"\n## Risk Assessment\n{risk}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def analyze_pausability(project_dir: Path, model: str | None = None) -> dict:
    """Run Slither + LLM pausability analysis. Returns the combined results dict."""
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
        for key in ("is_pausable", "pause_functions", "affected_functions"):
            if key in llm_findings:
                results[key] = llm_findings[key]
        for key in ("pause_mechanism", "custom_pause_patterns",
                     "impact_summary", "risk_assessment"):
            if key in llm_findings:
                results[key] = llm_findings[key]

    # Write outputs
    json_path = project_dir / "pausability_analysis.json"
    json_path.write_text(json.dumps(results, indent=2) + "\n")

    md_path = project_dir / "pausability_analysis.md"
    md_path.write_text(_generate_markdown(results))

    return results
