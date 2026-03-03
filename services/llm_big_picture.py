"""LLM-powered big picture analysis of smart contract governance and ecosystem risk."""

import json
from pathlib import Path

from utils.nim import chat
from utils.source_loader import load_sources, load_slither_findings

SYSTEM_PROMPT = """\
You are a senior smart contract security researcher performing a holistic governance \
and ecosystem risk assessment. You will receive:
1. Solidity source code
2. Slither static analysis findings
3. Privilege / access control analysis (if available)
4. Pausability analysis (if available)
5. Timelock analysis (if available)
6. Dependency data — static and dynamic (if available)

Produce a comprehensive **Big Picture Analysis** covering:

## Ecosystem Role
What role does this contract play in the broader protocol? Is it a token, vault, \
governance module, bridge, oracle consumer, etc.?

## Cross-Contract Interaction Risks
Using the dependency data, assess risks from external calls and integrations. \
Identify trust assumptions about other contracts.

## Privileged User Threat Model
What happens if a privileged user (owner, admin, governance) acts maliciously or \
their key is compromised? Enumerate worst-case scenarios per role.

## Pause Impact Analysis
If the contract is pausable, what is the blast radius? Which user operations are \
blocked? Could pausing itself be weaponized?

## Timelock Adequacy
Are timelocks sufficient for the privilege level they protect? Are any critical \
admin functions missing timelock protection?

## Governance Risk Score
Rate the overall governance risk on a scale of 1-10 (1 = fully decentralized, \
10 = single point of failure). Justify the score.

## Key Findings & Recommendations
Prioritized list of governance and ecosystem risks with actionable recommendations.

Be specific — reference function names, roles, and contract addresses where possible.\
"""


def analyze_big_picture(project_dir: Path, model: str | None = None) -> str:
    """Run big picture LLM analysis consuming all prior outputs. Returns markdown text."""
    source_code = load_sources(project_dir)
    slither_findings = load_slither_findings(project_dir)

    # Load metadata
    meta_path = project_dir / "contract_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}

    # Load prior analysis outputs
    sections = []

    sections.append(f"""Contract: {meta.get('contract_name', 'Unknown')}
Address: {meta.get('address', 'Unknown')}
Compiler: {meta.get('compiler_version', 'Unknown')}""")

    sections.append(f"## Source Code\n```solidity\n{source_code}\n```")
    sections.append(f"## Slither Findings\n{slither_findings}")

    # Privilege analysis
    priv_path = project_dir / "privilege_analysis.json"
    if priv_path.exists():
        try:
            priv_data = json.loads(priv_path.read_text())
            sections.append(f"## Privilege Analysis\n```json\n{json.dumps(priv_data, indent=2)}\n```")
        except json.JSONDecodeError:
            pass

    # Pausability analysis
    pause_path = project_dir / "pausability_analysis.json"
    if pause_path.exists():
        try:
            pause_data = json.loads(pause_path.read_text())
            sections.append(f"## Pausability Analysis\n```json\n{json.dumps(pause_data, indent=2)}\n```")
        except json.JSONDecodeError:
            pass

    # Timelock analysis
    tl_path = project_dir / "timelock_analysis.json"
    if tl_path.exists():
        try:
            tl_data = json.loads(tl_path.read_text())
            sections.append(f"## Timelock Analysis\n```json\n{json.dumps(tl_data, indent=2)}\n```")
        except json.JSONDecodeError:
            pass

    # Static dependencies
    deps_path = project_dir / "dependencies.json"
    if deps_path.exists():
        try:
            deps_data = json.loads(deps_path.read_text())
            sections.append(f"## Static Dependencies\n```json\n{json.dumps(deps_data, indent=2)}\n```")
        except json.JSONDecodeError:
            pass

    # Dynamic dependencies
    dyn_path = project_dir / "dynamic_dependencies.json"
    if dyn_path.exists():
        try:
            dyn_data = json.loads(dyn_path.read_text())
            sections.append(f"## Dynamic Dependencies\n```json\n{json.dumps(dyn_data, indent=2)}\n```")
        except json.JSONDecodeError:
            pass

    user_message = "\n\n".join(sections)

    kwargs = {}
    if model:
        kwargs["model"] = model

    return chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        max_tokens=8192,
        **kwargs,
    )
