"""Tests for Echidna analysis (services/static/echidna.py).

Runs the real echidna binary (built from constraint discovery branch)
against tests/contracts/Vault.sol and validates that symbolic execution
discovers input constraints from require() guards.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.echidna import is_available, run_echidna

pytestmark = pytest.mark.skipif(not is_available(), reason="echidna not installed")

VAULT_SOL = Path(__file__).parent / "contracts" / "Vault.sol"


def test_echidna_discovers_constraints(tmp_path: Path) -> None:
    """Run echidna against Vault.sol and verify it discovers require() bounds.

    The Vault contract has require() guards on deposit, swap, and withdraw.
    Echidna's symbolic execution should discover bounds on their arguments.
    Exact values are non-deterministic (depend on fuzzing seed and explored paths),
    so we verify structure and that bounds exist, not specific hex values.
    """
    shutil.copy(VAULT_SOL, tmp_path / "Vault.sol")

    result = run_echidna(tmp_path, "Vault.sol", contract_name="Vault", timeout=120)

    assert result["error"] is None, f"Echidna failed: {result['error']}"

    # --- Function results: all public functions fuzzed ---
    results = result["results"]
    assert "deposit(uint256)" in results
    assert "swap(uint256,uint256)" in results
    assert "withdraw(uint256)" in results
    for fn, status in results.items():
        assert status == "passing", f"{fn}: {status}"

    # --- Constraints: symbolic execution found bounds ---
    constraints = result["constraints"]
    assert len(constraints) > 0, (
        f"No constraints discovered. Is echidna built from the constraint discovery branch?\n"
        f"Output: {result['raw_output'][:500]}"
    )

    # deposit has require(amount >= MIN_DEPOSIT) and require(amount <= MAX_DEPOSIT)
    # so it should have both upper and lower bounds
    assert "deposit" in constraints, f"No deposit constraints. Found: {list(constraints)}"
    deposit_bounds = constraints["deposit"]
    has_lower = any(c.get("lower") for c in deposit_bounds)
    has_upper = any(c.get("upper") for c in deposit_bounds)
    assert has_lower, f"deposit should have a lower bound, got: {deposit_bounds}"
    assert has_upper, f"deposit should have an upper bound, got: {deposit_bounds}"

    # swap has require(amountIn >= 100), so should have a lower bound
    assert "swap" in constraints, f"No swap constraints. Found: {list(constraints)}"
    swap_lowers = [c["lower"] for c in constraints["swap"] if c.get("lower")]
    assert len(swap_lowers) > 0, f"swap should have a lower bound, got: {constraints['swap']}"

    # All constraint entries should have the right structure
    for fn_name, bounds in constraints.items():
        for bound in bounds:
            assert "arg" in bound, f"Missing 'arg' key in {fn_name} constraint: {bound}"
            assert "lower" in bound and "upper" in bound, (
                f"Missing lower/upper in {fn_name} constraint: {bound}"
            )

    # --- Coverage ---
    assert result["coverage"]["instructions"] > 0
    assert result["coverage"]["total_calls"] > 0
