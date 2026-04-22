#!/usr/bin/env python3
"""Contrast the current semantic pipeline with HEVM on an opaque external helper.

This script sets up one generic authorization scenario:

  - ``OpaqueExternalGuard.pause()`` calls ``gate.x(msg.sender)``
  - ``OpaqueGate.x(address who)`` reverts unless ``who == owner``

Semantically, the guard is:

  caller == owner(gate)

The current static/semantic pipeline does not reduce that helper call into the
canonical caller predicate today. HEVM can prove the behavior through symbolic
execution without depending on meaningful names.

What the script does:

  1. create a temporary Foundry project with the opaque-helper scenario
  2. run ``collect_contract_analysis`` and print the inferred guard metadata
  3. compile the project with ``forge build --ast``
  4. run ``hevm test`` against two proof cases and print the result

This is an explorer/demo tool, not part of the automated test suite.
"""

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

    interface IGate {
        function x(address who) external view;
    }

    contract OpaqueGate is IGate {
        address public owner;
        error Denied();

        constructor(address owner_) {
            owner = owner_;
        }

        function x(address who) external view {
            if (who != owner) revert Denied();
        }
    }

    contract OpaqueExternalGuard {
        IGate public gate;
        bool public paused;

        constructor(IGate gate_) {
            gate = gate_;
        }

        function pause() external {
            gate.x(msg.sender);
            paused = true;
        }
    }
    """
)

TEST_SOURCE = textwrap.dedent(
    """\
    // SPDX-License-Identifier: MIT
    pragma solidity ^0.8.19;

    import {Test} from "forge-std/Test.sol";
    import {OpaqueGate, OpaqueExternalGuard} from "../src/OpaqueExternalGuard.sol";

    contract OpaqueExternalGuardTest is Test {
        OpaqueGate gate;
        OpaqueExternalGuard guarded;
        address internal constant OWNER = address(0xBEEF);

        function setUp() public {
            gate = new OpaqueGate(OWNER);
            guarded = new OpaqueExternalGuard(gate);
        }

        function prove_only_owner_can_pause(address caller) public {
            vm.assume(caller != OWNER);
            vm.prank(caller);
            try guarded.pause() {
                assertTrue(false);
            } catch {
                assertFalse(guarded.paused());
            }
        }

        function prove_owner_can_pause() public {
            vm.prank(OWNER);
            guarded.pause();
            assertTrue(guarded.paused());
        }
    }
    """
)


def _print_banner(title: str) -> None:
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}")


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)


def _ensure_hevm_binary(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"hevm binary not found: {path}")
        return path

    on_path = shutil_which("hevm")
    if on_path:
        return Path(on_path)

    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "linux" or machine not in {"x86_64", "amd64"}:
        raise RuntimeError(
            "Auto-download is only wired for Linux x86_64 right now. Pass --hevm-bin explicitly on other platforms."
        )

    cache_dir = Path.home() / ".cache" / "psat" / "hevm"
    cache_dir.mkdir(parents=True, exist_ok=True)
    binary = cache_dir / "hevm-x86_64-linux"
    if not binary.exists():
        urllib.request.urlretrieve(HEVM_LINUX_X86_64, binary)
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return binary


def shutil_which(name: str) -> str | None:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(path) / name
        try:
            if candidate.exists() and os.access(candidate, os.X_OK):
                return str(candidate)
        except PermissionError:
            continue
    return None


def _write_static_metadata(project_dir: Path) -> None:
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": "0x1111111111111111111111111111111111111111",
                "contract_name": "OpaqueExternalGuard",
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

    (root / "src" / "OpaqueExternalGuard.sol").write_text(CONTRACT_SOURCE)
    (root / "test" / "OpaqueExternalGuard.t.sol").write_text(TEST_SOURCE)
    _write_static_metadata(root)


def _semantic_summary(project_dir: Path) -> dict[str, object]:
    analysis = collect_contract_analysis(project_dir)
    privileged = next(
        (item for item in analysis["access_control"]["privileged_functions"] if item["function"] == "pause()"),
        None,
    )
    tracked = [item for item in analysis["controller_tracking"] if item["label"] in {"gate", "owner", "BREAK_GLASS"}]
    if privileged is None:
        return {
            "subject": analysis["subject"]["name"],
            "privileged_functions": analysis["access_control"]["privileged_functions"],
            "tracked_controllers": tracked,
            "note": "Current semantic pipeline does not even recover pause() as a privileged function here.",
        }
    return {
        "guard_kinds": privileged["guard_kinds"],
        "guards": privileged["guards"],
        "controller_refs": privileged["controller_refs"],
        "tracked_controllers": tracked,
    }


def _run_hevm(project_dir: Path, hevm_bin: Path) -> subprocess.CompletedProcess[str]:
    build = _run(["forge", "build", "--ast"], cwd=project_dir)
    if build.returncode != 0:
        raise RuntimeError(f"forge build failed:\n{build.stdout}\n{build.stderr}")

    result = _run(
        [str(hevm_bin), "test", "--root", str(project_dir), "--solver", "z3", "--verb", "2"],
        cwd=project_dir,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hevm-bin", help="Optional path to an existing hevm binary.")
    parser.add_argument("--keep-temp", action="store_true", help="Keep the temporary Foundry project on disk.")
    args = parser.parse_args()

    hevm_bin = _ensure_hevm_binary(args.hevm_bin)
    project_dir = Path(tempfile.mkdtemp(prefix="psat_hevm_opaque_guard_"))
    try:
        _make_project(project_dir)

        _print_banner("Semantic Pipeline")
        summary = _semantic_summary(project_dir)
        print(json.dumps(summary, indent=2))
        print(
            "\nInterpretation:\n"
            "  The current semantic pipeline sees an external helper call through `gate`,\n"
            "  but it does not reduce that helper to the canonical predicate `caller == owner(gate)`.\n"
        )

        _print_banner("HEVM Proof")
        result = _run_hevm(project_dir, hevm_bin)
        print(result.stdout.rstrip())
        if result.stderr.strip():
            print("\n[stderr]")
            print(result.stderr.rstrip())

        if result.returncode != 0:
            print(f"\nHEVM exited with status {result.returncode}")
            return result.returncode

        print(
            "\nInterpretation:\n"
            "  HEVM proves both sides of the property on the same opaque-helper scenario:\n"
            "    - non-owner callers cannot pause\n"
            "    - the owner can pause\n"
            "  That is the semantic result we want the pipeline to recover.\n"
        )
        return 0
    finally:
        if args.keep_temp:
            print(f"\nKept project at: {project_dir}")
        else:
            subprocess.run(["rm", "-rf", str(project_dir)], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
