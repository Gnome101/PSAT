"""Predicate-pipeline performance benchmark.

Measures p50/p95/p99 latency for two phases of the v2 stack:

  - ``build_predicate_tree`` per function — the static-stage cost
    (target: <5ms p99 per function, per #17 PERF).
  - ``evaluate_tree_with_registry`` per function — the resolver
    cost excluding RPC (target: <50ms p99 per function).

Inputs are inline Solidity fixtures covering the canonical
shapes the v2 stack handles. Adding a new fixture is a one-line
extension to ``_FIXTURES``.

Usage::

    uv run python scripts/bench_predicate_pipeline.py
    uv run python scripts/bench_predicate_pipeline.py --runs 200
    uv run python scripts/bench_predicate_pipeline.py --json
    uv run python scripts/bench_predicate_pipeline.py --filter ownable

Exit code:
  0 — every fixture's p99 stayed under target
  1 — at least one fixture missed the per-fixture target

No DB / no RPC — pure timing of the in-memory pipeline. Slither
parse cost is excluded from the per-function timings (parsed
once per fixture, factored out of the loop).
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import textwrap
import tempfile
import time
from pathlib import Path
from typing import Any

# Allow running as `python scripts/bench_predicate_pipeline.py` from repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

# Imported only at runtime under tempdir scaffolding so the bench
# script doesn't pull in Slither at import time when no DB / RPC
# is touched.
TARGET_BUILD_P99_MS = 5.0
TARGET_EVAL_P99_MS = 50.0


_FIXTURES: list[tuple[str, str]] = [
    (
        "oz_ownable",
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            uint256 public x;
            modifier onlyOwner() { require(msg.sender == ownerVar); _; }
            function setX(uint256 v) external onlyOwner { x = v; }
            function bumpX() external onlyOwner { x = x + 1; }
        }
        """,
    ),
    (
        "oz_ac_inline",
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(bytes32 => mapping(address => bool)) private _roles;
            bytes32 public constant MINTER = keccak256("MINTER");
            uint256 public x;
            function mint(uint256 v) external {
                require(_roles[MINTER][msg.sender]);
                x = v;
            }
        }
        """,
    ),
    (
        "oz_pausable",
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            bool public _paused;
            uint256 public x;
            modifier whenNotPaused() { require(!_paused); _; }
            function pause() external {
                require(msg.sender == ownerVar);
                _paused = true;
            }
            function transfer() external whenNotPaused { x = x + 1; }
        }
        """,
    ),
    (
        "maker_wards",
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => uint256) public wards;
            uint256 public x;
            function rely(address addr) external {
                require(wards[msg.sender] == 1);
                wards[addr] = 1;
            }
            function mint(uint256 v) external {
                require(wards[msg.sender] == 1);
                x = v;
            }
        }
        """,
    ),
    (
        "oz_ac_cross_fn",
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(bytes32 => mapping(address => bool)) private _roles;
            mapping(bytes32 => bytes32) private _roleAdmins;
            modifier onlyRole(bytes32 role) { _checkRole(role); _; }
            function _checkRole(bytes32 role) internal view {
                if (!_roles[role][msg.sender]) revert();
            }
            function getRoleAdmin(bytes32 role) public view returns (bytes32) {
                return _roleAdmins[role];
            }
            function grantRole(bytes32 role, address account)
                public onlyRole(getRoleAdmin(role))
            {
                _roles[role][account] = true;
            }
        }
        """,
    ),
]


def _percentiles(samples_ms: list[float]) -> dict[str, float]:
    if not samples_ms:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "n": 0}
    sorted_samples = sorted(samples_ms)
    return {
        "p50": statistics.median(sorted_samples),
        "p95": _pct(sorted_samples, 95),
        "p99": _pct(sorted_samples, 99),
        "n": len(sorted_samples),
        "min": sorted_samples[0],
        "max": sorted_samples[-1],
        "mean": statistics.mean(sorted_samples),
    }


def _pct(sorted_samples: list[float], pct: int) -> float:
    if not sorted_samples:
        return 0.0
    if len(sorted_samples) == 1:
        return sorted_samples[0]
    # Standard nearest-rank percentile.
    rank = max(1, int(round(pct / 100.0 * len(sorted_samples))))
    return sorted_samples[min(rank - 1, len(sorted_samples) - 1)]


def _bench_one_fixture(name: str, source: str, *, runs: int) -> dict[str, Any]:
    """Compile once, then time build_predicate_tree per function
    and evaluate_tree_with_registry per function across ``runs``
    iterations. Returns the per-fixture report dict."""
    from slither import Slither

    from services.resolution.adapters import AdapterRegistry, EvaluationContext
    from services.resolution.adapters.access_control import AccessControlAdapter
    from services.resolution.adapters.aragon_acl import (
        AragonACLAdapter,
        DSAuthAdapter,
        EIP1271Adapter,
    )
    from services.resolution.adapters.event_indexed import EventIndexedAdapter
    from services.resolution.adapters.safe import SafeAdapter
    from services.resolution.predicate_evaluator import evaluate_tree_with_registry
    from services.static.contract_analysis_pipeline.predicates import (
        build_predicate_tree,
    )

    # Compile once outside the loop.
    with tempfile.TemporaryDirectory() as td:
        sol_path = Path(td) / "C.sol"
        sol_path.write_text(textwrap.dedent(source).strip() + "\n")
        sl = Slither(str(sol_path))
        contract = next(c for c in sl.contracts if c.name == "C")
        functions = [
            fn
            for fn in contract.functions
            if getattr(fn, "visibility", None) in ("external", "public")
            and not getattr(fn, "is_constructor", False)
        ]

        registry = AdapterRegistry()
        for cls in (
            AccessControlAdapter,
            SafeAdapter,
            AragonACLAdapter,
            DSAuthAdapter,
            EIP1271Adapter,
            EventIndexedAdapter,
        ):
            registry.register(cls)
        ctx = EvaluationContext()

        build_samples: list[float] = []
        eval_samples: list[float] = []

        # First pass: build trees so eval has something to time.
        # Time both phases on the same iterations to amortize Slither
        # IR setup. We keep build + eval samples per (function, iter)
        # as separate distributions.
        for _ in range(runs):
            for fn in functions:
                t0 = time.perf_counter()
                tree = build_predicate_tree(fn)
                t1 = time.perf_counter()
                build_samples.append((t1 - t0) * 1000.0)
                if tree is not None:
                    e0 = time.perf_counter()
                    evaluate_tree_with_registry(tree, registry, ctx)
                    e1 = time.perf_counter()
                    eval_samples.append((e1 - e0) * 1000.0)

        return {
            "fixture": name,
            "function_count": len(functions),
            "build": _percentiles(build_samples),
            "eval": _percentiles(eval_samples),
        }


def run_benchmark(*, runs: int, fixture_filter: str | None = None) -> dict[str, Any]:
    fixtures = _FIXTURES
    if fixture_filter:
        needle = fixture_filter.lower()
        fixtures = [(n, s) for n, s in fixtures if needle in n.lower()]
        if not fixtures:
            return {"fixtures": [], "filter": fixture_filter, "summary": {}}
    reports = [_bench_one_fixture(n, s, runs=runs) for n, s in fixtures]

    # Roll up worst-case across all fixtures.
    all_build_p99 = [r["build"].get("p99", 0.0) for r in reports if r["build"]["n"]]
    all_eval_p99 = [r["eval"].get("p99", 0.0) for r in reports if r["eval"]["n"]]
    summary = {
        "build_p99_max_ms": max(all_build_p99) if all_build_p99 else 0.0,
        "eval_p99_max_ms": max(all_eval_p99) if all_eval_p99 else 0.0,
        "build_target_ms": TARGET_BUILD_P99_MS,
        "eval_target_ms": TARGET_EVAL_P99_MS,
        "build_meets_target": (max(all_build_p99) if all_build_p99 else 0.0) <= TARGET_BUILD_P99_MS,
        "eval_meets_target": (max(all_eval_p99) if all_eval_p99 else 0.0) <= TARGET_EVAL_P99_MS,
    }
    return {
        "runs_per_fixture": runs,
        "fixtures": reports,
        "summary": summary,
    }


def _format_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(
        f"Predicate-pipeline benchmark — {len(report['fixtures'])} fixtures, "
        f"{report['runs_per_fixture']} runs each"
    )
    lines.append("=" * 72)
    lines.append(f"  {'fixture':<24} {'fns':>4} {'build p99 ms':>14} {'eval p99 ms':>14}")
    lines.append(f"  {'-' * 24} {'-' * 4} {'-' * 14} {'-' * 14}")
    for r in report["fixtures"]:
        bp = r["build"].get("p99", 0.0)
        ep = r["eval"].get("p99", 0.0)
        lines.append(
            f"  {r['fixture']:<24} {r['function_count']:>4} "
            f"{bp:>14.3f} {ep:>14.3f}"
        )
    s = report["summary"]
    lines.append("")
    lines.append(
        f"Worst build p99: {s.get('build_p99_max_ms', 0.0):.3f} ms "
        f"(target ≤ {s.get('build_target_ms', 0.0):.1f} ms) — "
        f"{'OK' if s.get('build_meets_target') else 'MISS'}"
    )
    lines.append(
        f"Worst eval  p99: {s.get('eval_p99_max_ms', 0.0):.3f} ms "
        f"(target ≤ {s.get('eval_target_ms', 0.0):.1f} ms) — "
        f"{'OK' if s.get('eval_meets_target') else 'MISS'}"
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=50, help="Iterations per fixture (default 50)")
    parser.add_argument("--filter", default=None, help="Only run fixtures whose name contains this substring")
    parser.add_argument("--json", action="store_true", help="Emit JSON")
    args = parser.parse_args()

    report = run_benchmark(runs=args.runs, fixture_filter=args.filter)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(_format_text(report))

    s = report.get("summary", {})
    if not s.get("build_meets_target", True) or not s.get("eval_meets_target", True):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
