"""Run Echidna fuzzing + symbolic execution on smart contracts.

Extracts:
- Function-level pass/fail results from fuzzing
- Symbolic execution exploration data (branches, counterexamples)
- Coverage and corpus metrics
- Input constraints extracted from require() guards via Slither AST

Requires: echidna binary on PATH, slither-analyzer.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def is_available() -> bool:
    """Check if the echidna binary is on PATH."""
    return shutil.which("echidna") is not None


def write_config(
    project_dir: Path,
    contract_addr: str | None = None,
    constructor_args: str | None = None,
) -> Path:
    """Write an Echidna config YAML for fuzzing with symbolic execution."""
    lines = [
        "testMode: assertion",
        "symExec: true",
        "symExecTimeout: 120",
        "shrinkLimit: 0",
        "format: text",
    ]
    if contract_addr:
        lines.append(f'contractAddr: "{contract_addr}"')
    if constructor_args:
        lines.append(f'deployConst: "{constructor_args}"')
    config_path = project_dir / "echidna_config.yaml"
    config_path.write_text("\n".join(lines) + "\n")
    return config_path


def run_echidna(
    project_dir: Path,
    contract_path: str,
    contract_name: str | None = None,
    rpc_url: str | None = None,
    rpc_block: str | None = None,
    contract_addr: str | None = None,
    timeout: int = 300,
) -> dict:
    """Run Echidna and return parsed results.

    Args:
        project_dir: Foundry project root.
        contract_path: Relative path to the .sol file.
        contract_name: Contract name if multiple in file.
        rpc_url: RPC endpoint for chain state forking.
        rpc_block: Block number to fork from (default: latest).
        timeout: Max seconds for the echidna process.

    Returns dict with:
        results: per-function pass/fail from fuzzing
        exploration: per-function symbolic execution data
        constraints: per-function input bounds
        coverage: instruction coverage and corpus metrics
        error: error string if echidna failed, else None
        raw_output: truncated raw output for debugging
    """
    config_path = write_config(project_dir, contract_addr=contract_addr)

    cmd = [
        "echidna",
        contract_path,
        "--config",
        str(config_path.name),
    ]
    if contract_name:
        cmd.extend(["--contract", contract_name])

    # RPC forking: pass via env vars so echidna loads on-chain state
    env = None
    if rpc_url:
        import os
        env = os.environ.copy()
        env["ECHIDNA_RPC_URL"] = rpc_url
        if rpc_block:
            env["ECHIDNA_RPC_BLOCK"] = rpc_block

    logger.info("Running Echidna: %s (cwd=%s, rpc=%s)", " ".join(cmd), project_dir, rpc_url or "none")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {
            "results": {},
            "exploration": {},
            "constraints": {},
            "coverage": {},
            "error": f"Echidna timed out after {timeout}s",
            "raw_output": "",
        }

    raw_output = result.stdout + result.stderr

    if result.returncode != 0 and not raw_output.strip():
        return {
            "results": {},
            "exploration": {},
            "constraints": {},
            "coverage": {},
            "error": f"Echidna exited with code {result.returncode}: {result.stderr[:2000]}",
            "raw_output": raw_output[:50000],
        }

    parsed = parse_echidna_output(raw_output)
    parsed["raw_output"] = raw_output[:50000]
    parsed["error"] = None
    return parsed


def parse_echidna_output(output: str) -> dict:
    """Parse all useful data from echidna's text output.

    Extracts three categories:
    1. results: function pass/fail from fuzzing (e.g. "deposit(uint256): passing")
    2. exploration: symbolic execution data per function (branches, counterexamples)
    3. constraints: input bounds per function (PR #1409 format, empty on current release)
    4. coverage: global metrics (instructions, corpus, calls)
    """
    return {
        "results": _parse_function_results(output),
        "exploration": _parse_exploration(output),
        "constraints": _parse_constraints(output),
        "coverage": _parse_coverage(output),
    }


# ---------------------------------------------------------------------------
# Function results: "deposit(uint256): passing" / "prop_x: failed!❗️"
# ---------------------------------------------------------------------------


def _parse_function_results(output: str) -> dict[str, str]:
    """Extract function-level pass/fail results.

    Returns {"deposit(uint256)": "passing", "echidna_test": "failed", ...}
    """
    results: dict[str, str] = {}
    for match in re.finditer(
        r"^(\S+(?:\([^)]*\))?)\s*:\s*(passing|failed)",
        output,
        re.MULTILINE,
    ):
        fn_name = match.group(1)
        status = match.group(2)
        results[fn_name] = status
    return results


# ---------------------------------------------------------------------------
# Exploration: symbolic execution per function
# ---------------------------------------------------------------------------


def _parse_exploration(output: str) -> dict[str, dict]:
    """Extract symbolic execution exploration data per function.

    Echidna outputs blocks like:
        Exploring "deposit"
           Exploring call prefix 0xb6b55f25
           ... N branch(es) checked in call prefix 0xb6b55f25 ...
           Found M potential counterexample(s) in call prefix 0xb6b55f25

    Each function has a unique selector. We map selector→function from the
    first occurrence, then aggregate branches and counterexamples.

    Returns {
        "deposit": {"selector": "0xb6b55f25", "branches": 6, "counterexamples": 0},
        ...
    }
    """
    exploration: dict[str, dict] = {}
    selector_to_fn: dict[str, str] = {}
    last_fn: str | None = None

    for line in output.splitlines():
        # Track which function we're currently in
        fn_match = re.search(r'Exploring "(\w+)"', line)
        if fn_match:
            name: str = fn_match.group(1)
            last_fn = name
            if name not in exploration:
                exploration[name] = {"branches": 0, "counterexamples": 0}
            continue

        # Map selector to the current function
        sel_match = re.search(r"Exploring call prefix (0x[0-9a-f]+)", line)
        if sel_match and last_fn:
            selector = sel_match.group(1)
            if selector not in selector_to_fn:
                selector_to_fn[selector] = last_fn
                exploration[last_fn]["selector"] = selector
            continue

        # Aggregate branches
        br_match = re.search(
            r"(\d+) branch\(es\) checked in call prefix (0x[0-9a-f]+)", line
        )
        if br_match:
            branches = int(br_match.group(1))
            selector = br_match.group(2)
            fn_name = selector_to_fn.get(selector)
            if fn_name and fn_name in exploration:
                exploration[fn_name]["branches"] += branches
            continue

        # Aggregate counterexamples
        cx_match = re.search(
            r"Found (\d+) potential counterexample\(s\) in call prefix (0x[0-9a-f]+)",
            line,
        )
        if cx_match:
            count = int(cx_match.group(1))
            selector = cx_match.group(2)
            fn_name = selector_to_fn.get(selector)
            if fn_name and fn_name in exploration:
                exploration[fn_name]["counterexamples"] += count

    return exploration


# ---------------------------------------------------------------------------
# Coverage metrics
# ---------------------------------------------------------------------------


def _parse_coverage(output: str) -> dict:
    """Extract global coverage metrics."""
    coverage: dict[str, int] = {}

    m = re.search(r"Unique instructions:\s*(\d+)", output)
    if m:
        coverage["instructions"] = int(m.group(1))

    m = re.search(r"Corpus size:\s*(\d+)", output)
    if m:
        coverage["corpus_size"] = int(m.group(1))

    m = re.search(r"Total calls:\s*(\d+)", output)
    if m:
        coverage["total_calls"] = int(m.group(1))

    m = re.search(r"Unique codehashes:\s*(\d+)", output)
    if m:
        coverage["codehashes"] = int(m.group(1))

    return coverage


# ---------------------------------------------------------------------------
# Constraint discovery (PR #1409 format — empty on current release)
# ---------------------------------------------------------------------------


def _parse_constraints(output: str) -> dict:
    """Parse constraint bounds from echidna's symbolic execution output.

    The actual output format (crytic/echidna#1409) interleaves constraints
    with exploration logs. The function name comes from the preceding
    'Exploring "name"' line, and constraints appear after 'Constraints inferred:':

        Exploring "deposit"
           Exploring call prefix 0xb6b55f25
           ...
        Constraints inferred:
        arg1 <= 0xde0b6b3a7640000
        arg1 >= 0x2386f26fc10000

    Constraints may also appear after 'Constraints resolved:' with range syntax:
        arg0 in [0x64, 0xfd]
    """
    functions: dict[str, dict] = {}
    current_fn: str | None = None
    in_constraints = False

    for line in output.splitlines():
        stripped = line.strip()

        # Track function name from Exploring header
        fn_match = re.search(r'Exploring "(\w+)"', stripped)
        if fn_match:
            current_fn = fn_match.group(1)
            in_constraints = False
            continue

        # Also support "Analyzing function:" header (future format)
        fn_match2 = re.match(r"Analyzing function:\s*(.+)", stripped)
        if fn_match2:
            current_fn = fn_match2.group(1).strip()
            in_constraints = False
            continue

        if stripped.startswith("Constraints inferred") or stripped.startswith("Constraints resolved"):
            in_constraints = True
            continue

        if not in_constraints or not current_fn:
            continue

        # Try to parse a constraint line
        # Range: arg0 in [0x64, 0xfd]
        rm = re.match(
            r"(arg\d+)\s+in\s+\[(0x[0-9a-fA-F]+|\d+),\s*(0x[0-9a-fA-F]+|\d+)\]",
            stripped,
        )
        if rm:
            if current_fn not in functions:
                functions[current_fn] = []
            functions[current_fn].append(
                {"arg": rm.group(1), "lower": rm.group(2), "upper": rm.group(3)}
            )
            continue

        # Single bound: arg1 >= 0x2386f26fc10000
        bm = re.match(r"(arg\d+)\s*(<=|>=|==)\s*(0x[0-9a-fA-F]+|\d+)", stripped)
        if bm:
            if current_fn not in functions:
                functions[current_fn] = []
            arg, op, val = bm.group(1), bm.group(2), bm.group(3)
            if op == ">=":
                functions[current_fn].append({"arg": arg, "lower": val, "upper": None})
            elif op == "<=":
                functions[current_fn].append({"arg": arg, "lower": None, "upper": val})
            else:
                functions[current_fn].append({"arg": arg, "lower": val, "upper": val})
            continue

        # Any non-matching line exits constraint mode
        if stripped and not stripped.startswith("[") and not stripped.startswith("("):
            in_constraints = False

    # Deduplicate: echidna explores multiple paths and repeats constraints
    for fn_name in functions:
        seen: set[tuple] = set()
        unique: list[dict] = []
        for c in functions[fn_name]:
            key = (c["arg"], c.get("lower"), c.get("upper"))
            if key not in seen:
                seen.add(key)
                unique.append(c)
        functions[fn_name] = unique

    return functions


def find_main_contract(project_dir: Path, contract_name: str) -> str | None:
    """Find the .sol file containing the main contract.

    Searches src/, contracts/, and root for a file whose name matches
    the contract name or contains a 'contract <Name>' declaration.
    Returns a relative path from project_dir, or None.
    """
    for search_dir in ["src", "contracts", "."]:
        candidate = project_dir / search_dir / f"{contract_name}.sol"
        if candidate.exists():
            return str(candidate.relative_to(project_dir))

    for sol_file in sorted(project_dir.rglob("*.sol")):
        rel = sol_file.relative_to(project_dir)
        parts = rel.parts
        if parts and parts[0] in ("lib", "out", "node_modules", "cache"):
            continue
        try:
            content = sol_file.read_text(errors="ignore")
            if re.search(rf"\bcontract\s+{re.escape(contract_name)}\b", content):
                return str(rel)
        except OSError:
            continue

    return None
