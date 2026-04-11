"""Tests for Echidna analysis (services/static/echidna.py).

Runs the real echidna binary (built from constraint discovery branch)
against test contracts and validates that symbolic execution discovers
real input constraints from require() guards.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.echidna import is_available, run_echidna

pytestmark = pytest.mark.skipif(not is_available(), reason="echidna not installed")

CONTRACTS_DIR = Path(__file__).parent / "contracts"


def test_echidna_discovers_constraints(tmp_path: Path) -> None:
    """Run echidna against Vault.sol (fixed constants: MIN=0.01 ETH, MAX=1 ETH).
    Verify it discovers the require() bounds on deposit and swap."""
    shutil.copy(CONTRACTS_DIR / "Vault.sol", tmp_path / "Vault.sol")

    result = run_echidna(tmp_path, "Vault.sol", contract_name="Vault", timeout=120)
    assert result["error"] is None, f"Echidna failed: {result['error']}"

    # Functions fuzzed
    assert "deposit(uint256)" in result["results"]
    assert "swap(uint256,uint256)" in result["results"]

    # Constraints discovered
    constraints = result["constraints"]
    assert len(constraints) > 0, f"No constraints. Output: {result['raw_output'][:500]}"

    assert "deposit" in constraints
    deposit_bounds = constraints["deposit"]
    assert any(c.get("lower") for c in deposit_bounds), f"deposit needs a lower bound: {deposit_bounds}"
    assert any(c.get("upper") for c in deposit_bounds), f"deposit needs an upper bound: {deposit_bounds}"


def test_echidna_discovers_configurable_bounds(tmp_path: Path) -> None:
    """Run echidna against ConfigurableVault.sol (constructor defaults: min=100, max=5000).
    The bounds are storage variables, not constants — echidna must resolve them
    from the deployed state to discover arg1 >= 100 and arg1 <= 5000."""
    shutil.copy(CONTRACTS_DIR / "ConfigurableVault.sol", tmp_path / "ConfigurableVault.sol")

    result = run_echidna(tmp_path, "ConfigurableVault.sol", contract_name="ConfigurableVault", timeout=120)
    assert result["error"] is None, f"Echidna failed: {result['error']}"

    constraints = result["constraints"]
    assert "deposit" in constraints, f"No deposit constraints. Found: {list(constraints)}"

    # The constructor sets minDeposit=100 (0x64) and maxDeposit=5000 (0x1388).
    # Echidna should discover these from storage reads during symbolic execution.
    deposit_bounds = constraints["deposit"]
    all_lowers = [int(c["lower"], 16) for c in deposit_bounds if c.get("lower")]
    all_uppers = [int(c["upper"], 16) for c in deposit_bounds if c.get("upper")]

    # Must find the min bound (100) somewhere in lower bounds
    assert any(v == 100 for v in all_lowers), (
        f"Expected minDeposit=100 in lower bounds, got: {[hex(v) for v in all_lowers]}"
    )
    # Must find the max bound (5000) somewhere in upper bounds
    assert any(v == 5000 for v in all_uppers), (
        f"Expected maxDeposit=5000 in upper bounds, got: {[hex(v) for v in all_uppers]}"
    )
