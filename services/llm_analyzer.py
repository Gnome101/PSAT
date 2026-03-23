"""LLM-powered smart contract flow analysis using NVIDIA NIM."""

import json
from pathlib import Path

from utils.nim import chat

SYSTEM_PROMPT = """\
You are a smart contract security analyst. You will be given:
1. The Solidity source code of a contract
2. Static analysis findings from Slither

Analyze the contract and provide:

## Contract Overview
Brief description of what the contract does.

## Flow Analysis
Walk through the key execution flows (e.g. token transfers, approvals, admin functions).
For each flow, describe the path through the code and what checks/modifiers guard it.

## Access Control
Who can do what? Map out the privilege levels and what each role controls.

## Risk Assessment
Based on the source code AND the Slither findings, identify the most critical risks.
Rank them by severity. Distinguish between real risks and false positives from Slither.

## Recommendations
Actionable suggestions to improve the contract's security.

Be specific — reference function names and line numbers.\
"""


def _load_sources(project_dir: Path) -> str:
    """Concatenate all .sol files in the project."""
    parts = []
    for sol_file in sorted(project_dir.rglob("src/**/*.sol")):
        rel = sol_file.relative_to(project_dir)
        content = sol_file.read_text()
        parts.append(f"// === {rel} ===\n{content}")
    return "\n\n".join(parts)


def _load_slither_findings(project_dir: Path) -> str:
    """Load slither results as a summary string."""
    json_path = project_dir / "slither_results.json"
    if not json_path.exists():
        return "No Slither results available."

    data = json.loads(json_path.read_text())
    detectors = data.get("results", {}).get("detectors", [])

    if not detectors:
        return "Slither found no issues."

    lines = []
    for d in detectors:
        impact = d.get("impact", "?")
        check = d.get("check", "?")
        desc = d.get("description", "").strip().split("\n")[0]
        lines.append(f"- [{impact}] {check}: {desc}")

    return "\n".join(lines)


def _load_structured_analysis(project_dir: Path) -> str:
    """Load structured contract analysis if present."""
    json_path = project_dir / "contract_analysis.json"
    if not json_path.exists():
        return "No structured contract analysis available."
    return json.dumps(json.loads(json_path.read_text()), indent=2)


def analyze_with_llm(project_dir: Path, model: str | None = None) -> str:
    """Run LLM flow analysis on a contract project. Returns the analysis text."""
    source_code = _load_sources(project_dir)
    slither_findings = _load_slither_findings(project_dir)
    structured_analysis = _load_structured_analysis(project_dir)

    # Load metadata
    meta_path = project_dir / "contract_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}

    user_message = f"""Contract: {meta.get('contract_name', 'Unknown')}
Address: {meta.get('address', 'Unknown')}
Compiler: {meta.get('compiler_version', 'Unknown')}

## Source Code
```solidity
{source_code}
```

## Slither Findings
{slither_findings}

## Structured Static Analysis
```json
{structured_analysis}
```
"""

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
