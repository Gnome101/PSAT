#!/usr/bin/env python3
"""Contrast the semantic pipeline with HEVM on an opaque external policy helper."""

from __future__ import annotations

import argparse
import json
import os
import platform
import stat
import subprocess
import sys
import tempfile
import textwrap
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.static import collect_contract_analysis  # noqa: E402

HEVM_LINUX_X86_64 = "https://github.com/argotorg/hevm/releases/download/release/0.57.0/hevm-x86_64-linux"

CONTRACT_SOURCE = textwrap.dedent(
    """\
    // SPDX-License-Identifier: MIT
    pragma solidity ^0.8.19;

    interface IPolicy {
        function q(address who, address target, bytes4 sig) external view;
    }

    contract OpaquePolicy is IPolicy {
        mapping(address => mapping(bytes4 => mapping(address => bool))) internal can;
        error Denied();

        function p(address target, bytes4 selector, address who) external {
            can[target][selector][who] = true;
        }

        function q(address who, address target, bytes4 sig) external view {
            if (!can[target][sig][who]) revert Denied();
        }
    }

    contract OpaqueExternalPolicyGuard {
        IPolicy public policy;
        bool public executed;

        constructor(IPolicy policy_) {
            policy = policy_;
        }

        function execute() external {
            policy.q(msg.sender, address(this), this.execute.selector);
            executed = true;
        }
    }
    """
)

TEST_SOURCE = textwrap.dedent(
    """\
    // SPDX-License-Identifier: MIT
    pragma solidity ^0.8.19;

    import {Test} from "forge-std/Test.sol";
    import {IPolicy, OpaquePolicy, OpaqueExternalPolicyGuard} from "../src/OpaqueExternalPolicyGuard.sol";

    contract OpaqueExternalPolicyGuardTest is Test {
        OpaquePolicy policy;
        OpaqueExternalPolicyGuard guarded;
        address internal constant ADMIN = address(0xBEEF);

        function setUp() public {
            policy = new OpaquePolicy();
            guarded = new OpaqueExternalPolicyGuard(policy);
            policy.p(address(guarded), guarded.execute.selector, ADMIN);
        }

        function prove_unauthorized_cannot_execute(address caller) public {
            vm.assume(caller != ADMIN);
            vm.prank(caller);
            try guarded.execute() {
                assertTrue(false);
            } catch {
                assertFalse(guarded.executed());
            }
        }

        function prove_authorized_can_execute() public {
            vm.prank(ADMIN);
            guarded.execute();
            assertTrue(guarded.executed());
        }
    }
    """
)


def _print_banner(title: str) -> None:
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}")


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)


def _shutil_which(name: str) -> str | None:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(path) / name
        try:
            if candidate.exists() and os.access(candidate, os.X_OK):
                return str(candidate)
        except PermissionError:
            continue
    return None


def _ensure_hevm_binary(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"hevm binary not found: {path}")
        return path

    on_path = _shutil_which("hevm")
    if on_path:
        return Path(on_path)

    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "linux" or machine not in {"x86_64", "amd64"}:
        raise RuntimeError("Auto-download is only wired for Linux x86_64 right now.")

    cache_dir = Path.home() / ".cache" / "psat" / "hevm"
    cache_dir.mkdir(parents=True, exist_ok=True)
    binary = cache_dir / "hevm-x86_64-linux"
    if not binary.exists():
        urllib.request.urlretrieve(HEVM_LINUX_X86_64, binary)
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return binary


def _write_static_metadata(project_dir: Path) -> None:
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": "0x1111111111111111111111111111111111111111",
                "contract_name": "OpaqueExternalPolicyGuard",
                "compiler_version": "v0.8.19+commit.7dd6d404",
            }
        )
        + "\n"
    )
    (project_dir / "slither_results.json").write_text(json.dumps({"results": {"detectors": []}}) + "\n")


def _make_project(root: Path) -> None:
    init = _run(["forge", "init", "--no-git", str(root)], cwd=ROOT)
    if init.returncode != 0:
        raise RuntimeError(f"forge init failed:\n{init.stdout}\n{init.stderr}")
    (root / "src" / "OpaqueExternalPolicyGuard.sol").write_text(CONTRACT_SOURCE)
    (root / "test" / "OpaqueExternalPolicyGuard.t.sol").write_text(TEST_SOURCE)
    _write_static_metadata(root)


def _semantic_summary(project_dir: Path) -> dict[str, object]:
    analysis = collect_contract_analysis(project_dir)
    privileged = next(
        (item for item in analysis["access_control"]["privileged_functions"] if item["function"] == "execute()"),
        None,
    )
    if privileged is None:
        return {
            "subject": analysis["subject"]["name"],
            "privileged_functions": analysis["access_control"]["privileged_functions"],
            "note": "Current semantic pipeline does not recover execute() as a canonical policy-guarded function here.",
        }
    return {
        "guard_kinds": privileged["guard_kinds"],
        "guards": privileged["guards"],
        "controller_refs": privileged["controller_refs"],
    }


def _run_hevm(project_dir: Path, hevm_bin: Path) -> subprocess.CompletedProcess[str]:
    build = _run(["forge", "build", "--ast"], cwd=project_dir)
    if build.returncode != 0:
        raise RuntimeError(f"forge build failed:\n{build.stdout}\n{build.stderr}")
    return _run([str(hevm_bin), "test", "--root", str(project_dir), "--solver", "z3", "--verb", "2"], cwd=project_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hevm-bin", help="Optional path to an existing hevm binary.")
    parser.add_argument("--keep-temp", action="store_true")
    args = parser.parse_args()

    hevm_bin = _ensure_hevm_binary(args.hevm_bin)
    project_dir = Path(tempfile.mkdtemp(prefix="psat_hevm_opaque_policy_"))
    try:
        _make_project(project_dir)
        _print_banner("Semantic Pipeline")
        print(json.dumps(_semantic_summary(project_dir), indent=2))
        print(
            "\nInterpretation:\n"
            "  The current semantic pipeline does not reduce `policy.q(msg.sender, address(this), selector)`\n"
            "  to a canonical policy-check predicate.\n"
        )

        _print_banner("HEVM Proof")
        result = _run_hevm(project_dir, hevm_bin)
        print(result.stdout.rstrip())
        if result.stderr.strip():
            print("\n[stderr]")
            print(result.stderr.rstrip())
        return result.returncode
    finally:
        if args.keep_temp:
            print(f"\nKept project at: {project_dir}")
        else:
            subprocess.run(["rm", "-rf", str(project_dir)], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
