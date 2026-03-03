"""Slither AST + LLM analysis for access control and privilege patterns."""

import json
import re
from pathlib import Path

from slither.slither import Slither
from slither.core.declarations.solidity_variables import SolidityVariableComposed
from slither.core.cfg.node import NodeType

from utils.nim import chat
from utils.source_loader import load_sources

# Well-known access-control base contracts
KNOWN_AC_BASES = {
    "Ownable", "Ownable2Step", "AccessControl", "AccessControlEnumerable",
    "AccessControlDefaultAdminRules", "Auth",
}

# ---------------------------------------------------------------------------
# Slither layer
# ---------------------------------------------------------------------------


def _slither_scan(project_dir: Path) -> dict:
    """Use Slither's Python API to extract access control information."""
    sl = Slither(str(project_dir))

    ac_models: list[str] = []
    privileged_roles: list[str] = []
    role_constants: list[dict] = []
    owner_state_variables: list[str] = []
    modifier_declarations: list[dict] = []
    gated_functions: list[dict] = []
    ungated_state_changing: list[str] = []
    sender_checks: list[dict] = []

    for contract in sl.contracts:
        if contract.is_library:
            continue

        # --- Detect access control model from inheritance ---
        for parent in contract.inheritance:
            if parent.name in KNOWN_AC_BASES and parent.name not in ac_models:
                ac_models.append(parent.name)

        # --- Collect modifier declarations ---
        for mod in contract.modifiers_declared:
            modifier_declarations.append({
                "name": mod.name,
                "contract": contract.name,
            })

        # --- Detect role constants (bytes32 ... _ROLE = keccak256("...")) ---
        for sv in contract.state_variables_declared:
            name = sv.name
            if name.endswith("_ROLE") and str(sv.type) == "bytes32":
                # Try to get the string value from the expression
                role_string = name  # fallback
                if sv.expression:
                    expr_str = str(sv.expression)
                    m = re.search(r'keccak256\s*\(\s*["\']([^"\']+)', expr_str)
                    if m:
                        role_string = m.group(1)
                role_constants.append({
                    "constant_name": name,
                    "role_string": role_string,
                })
                if role_string not in privileged_roles:
                    privileged_roles.append(role_string)

            # --- Detect owner/admin state variables ---
            if str(sv.type) == "address" and any(
                kw in name.lower() for kw in ("owner", "admin", "governance", "operator")
            ):
                owner_state_variables.append(name)

        # --- Analyze functions ---
        for func in contract.functions_declared:
            if func.is_constructor or func.visibility in ("private", "internal"):
                continue

            is_view = func.view or func.pure
            is_protected = func.is_protected()
            modifiers = [m.name for m in func.modifiers]

            # --- Walk nodes for msg.sender comparisons ---
            func_sender_checks = []
            for node in func.nodes:
                has_sender = any(
                    isinstance(v, SolidityVariableComposed)
                    and v.name == "msg.sender"
                    for v in node.solidity_variables_read
                )
                if not has_sender:
                    continue
                is_gate = (
                    node.contains_require_or_assert()
                    or node.type == NodeType.IF
                )
                if not is_gate:
                    continue

                compared_to = [v.name for v in node.state_variables_read]
                func_sender_checks.append({
                    "expression": str(node.expression)[:200],
                    "compared_to": compared_to,
                })
                # Track privileged addresses
                for var_name in compared_to:
                    role_label = f"{var_name}"
                    if role_label not in privileged_roles:
                        privileged_roles.append(role_label)

            if func_sender_checks:
                sender_checks.append({
                    "function": func.name,
                    "contract": contract.name,
                    "checks": func_sender_checks,
                })

            # --- Classify as gated or ungated ---
            if is_protected:
                gate_info = {
                    "function": func.full_name,
                    "contract": contract.name,
                    "visibility": func.visibility,
                }
                if modifiers:
                    gate_info["modifiers"] = modifiers
                if func_sender_checks:
                    gate_info["inline_checks"] = [
                        c["compared_to"] for c in func_sender_checks
                    ]
                gated_functions.append(gate_info)
            elif not is_view and func.name not in (
                "receive", "fallback",
                # Standard ERC callbacks that are expected to be ungated
                "onERC721Received", "onERC1155Received",
                "onERC1155BatchReceived",
            ):
                ungated_state_changing.append(func.full_name)

    return {
        "access_control_model": ac_models or ["none_detected"],
        "privileged_roles": privileged_roles,
        "role_constants": role_constants,
        "owner_state_variables": owner_state_variables,
        "modifier_declarations": modifier_declarations,
        "gated_functions": gated_functions,
        "ungated_state_changing_functions": ungated_state_changing,
        "sender_checks": sender_checks,
    }


# ---------------------------------------------------------------------------
# LLM layer
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a smart contract access-control analyst. You will receive:
1. Solidity source code
2. Structural findings from Slither static analysis about access control patterns

Produce a JSON object (no markdown fences) with these keys:
- "access_control_model": list of access control patterns used (e.g. "Ownable", "AccessControl", "Auth", "custom")
- "privileged_roles": list of role names/descriptions
- "gated_functions": list of {"function": str, "modifiers": [str], "risk_level": "high"|"medium"|"low", "risk_reason": str}
- "ungated_state_changing_functions": list of {"function": str, "risk_level": str, "risk_reason": str}
- "custom_patterns": list of custom access control patterns Slither may have missed
- "summary": 2-3 sentence summary of the access control posture
"""


def _llm_enrich(source_code: str, slither_findings: dict, model: str | None = None) -> dict:
    """Send source + Slither findings to LLM for risk classification."""
    user_message = f"""## Slither Structural Findings
```json
{json.dumps(slither_findings, indent=2)}
```

## Source Code
```solidity
{source_code}
```

Analyze the access control patterns. Confirm or correct the Slither findings, identify any custom patterns missed, and classify each gated function's risk level. Return ONLY valid JSON."""

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
    lines = ["# Privilege & Access Control Analysis\n"]

    lines.append("## Access Control Model")
    for model in results.get("access_control_model", []):
        lines.append(f"- {model}")

    lines.append("\n## Privileged Roles")
    roles = results.get("privileged_roles", [])
    if roles:
        for role in roles:
            lines.append(f"- {role}")
    else:
        lines.append("- None detected")

    lines.append("\n## Gated Functions")
    gated = results.get("gated_functions", [])
    if gated:
        lines.append("| Function | Protection | Risk Level |")
        lines.append("|----------|-----------|------------|")
        for g in gated:
            mods = ", ".join(g.get("modifiers", []))
            inline = ", ".join(
                str(c) for c in g.get("inline_checks", [])
            )
            protection = mods or inline or "is_protected"
            risk = g.get("risk_level", "")
            lines.append(f"| {g['function']} | {protection} | {risk} |")
    else:
        lines.append("No gated functions detected.")

    lines.append("\n## Ungated State-Changing Functions")
    ungated = results.get("ungated_state_changing_functions", [])
    if ungated:
        for u in ungated:
            if isinstance(u, dict):
                lines.append(f"- **{u['function']}** — {u.get('risk_reason', '')}")
            else:
                lines.append(f"- {u}")
    else:
        lines.append("None detected.")

    summary = results.get("summary")
    if summary:
        lines.append(f"\n## Summary\n{summary}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def analyze_privileges(project_dir: Path, model: str | None = None) -> dict:
    """Run Slither + LLM privilege analysis. Returns the combined results dict."""
    source_code = load_sources(project_dir)
    if not source_code.strip():
        return {"error": "No Solidity source files found"}

    # Phase 1: Slither structural analysis
    slither_findings = _slither_scan(project_dir)

    # Phase 2: LLM enrichment (risk classification + catch custom patterns)
    try:
        llm_findings = _llm_enrich(source_code, slither_findings, model=model)
    except Exception as exc:
        llm_findings = {"llm_error": str(exc)}

    # Merge: Slither provides structure, LLM adds risk levels and narrative
    results = {**slither_findings}
    if not llm_findings.get("parse_error") and not llm_findings.get("llm_error"):
        for key in ("access_control_model", "privileged_roles", "gated_functions",
                     "ungated_state_changing_functions"):
            if key in llm_findings:
                results[key] = llm_findings[key]
        for key in ("custom_patterns", "summary"):
            if key in llm_findings:
                results[key] = llm_findings[key]

    # Write outputs
    json_path = project_dir / "privilege_analysis.json"
    json_path.write_text(json.dumps(results, indent=2) + "\n")

    md_path = project_dir / "privilege_analysis.md"
    md_path.write_text(_generate_markdown(results))

    return results
