"""LLM-powered smart contract flow analysis using NVIDIA NIM."""

import json
from pathlib import Path

from utils.nim import chat
from utils.source_loader import load_sources, load_slither_findings

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


def analyze_with_llm(project_dir: Path, model: str | None = None) -> str:
    """Run LLM flow analysis on a contract project. Returns the analysis text."""
    source_code = load_sources(project_dir)
    slither_findings = load_slither_findings(project_dir)

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
