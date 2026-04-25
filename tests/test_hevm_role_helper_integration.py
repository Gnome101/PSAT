from __future__ import annotations

import json
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.policy.effective_permissions import build_effective_permissions
from services.resolution.tracking import build_control_snapshot
from services.resolution.tracking_plan import build_control_tracking_plan
from services.static import collect_contract_analysis
from services.static.contract_analysis_pipeline import build_semantic_guards
from services.static.hevm_guard_analysis import refine_semantic_guards_with_hevm

ANVIL_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
DEPLOYER = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"

_REQUIRED_BINARIES = ("anvil", "forge", "hevm")
_missing_binaries = [b for b in _REQUIRED_BINARIES if shutil.which(b) is None]
requires_foundry_and_hevm = pytest.mark.skipif(
    bool(_missing_binaries),
    reason=f"Missing required binaries: {', '.join(_missing_binaries)}",
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for anvil on port {port}")


def _run(cmd: list[str], *, cwd: Path) -> str:
    result = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
    return "\n".join(part for part in [result.stdout, result.stderr] if part)


def _deployed_address(output: str) -> str:
    match = re.search(r"Deployed to:\s*(0x[a-fA-F0-9]{40})", output)
    if not match:
        raise RuntimeError(f"Could not parse deployed address from output:\n{output}")
    return match.group(1)


@requires_foundry_and_hevm
def test_hevm_role_helper_refines_and_resolves_local_project():
    with tempfile.TemporaryDirectory(prefix="psat_hevm_role_e2e_") as tmp:
        root = Path(tmp)
        (root / "src").mkdir(parents=True)
        (root / "foundry.toml").write_text(
            '[profile.default]\nsrc = "src"\nout = "out"\nlibs = []\nsolc_version = "0.8.19"\n'
        )
        (root / "src" / "OpaqueExternalRoleGuard.sol").write_text(
            """// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

interface IAuth {
    function z(address who) external view;
}

    contract OpaqueRoleAuth is IAuth {
        bytes32 public constant BREAK_GLASS = keccak256("x");
        address[] internal breakGlassMembers;
        mapping(bytes32 => mapping(address => bool)) internal roles;
        error Denied();

        constructor(address pauser) {
            breakGlassMembers.push(pauser);
            roles[BREAK_GLASS][pauser] = true;
        }

    function roleHolders(bytes32 role) external view returns (address[] memory) {
        if (role == BREAK_GLASS) return breakGlassMembers;
        address[] memory empty;
        return empty;
    }

        function z(address who) external view {
            if (!roles[BREAK_GLASS][who]) revert Denied();
        }
    }

contract OpaqueExternalRoleGuard {
    IAuth public auth;
    bool public paused;

    constructor(IAuth auth_) {
        auth = auth_;
    }

    function pause() external {
        auth.z(msg.sender);
        paused = true;
    }
}
"""
        )

        port = _free_port()
        rpc_url = f"http://127.0.0.1:{port}"
        anvil = subprocess.Popen(
            ["anvil", "-p", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            _wait_for_port(port)

            auth_out = _run(
                [
                    "forge",
                    "create",
                    "src/OpaqueExternalRoleGuard.sol:OpaqueRoleAuth",
                    "--broadcast",
                    "--rpc-url",
                    rpc_url,
                    "--private-key",
                    ANVIL_KEY,
                    "--constructor-args",
                    DEPLOYER,
                ],
                cwd=root,
            )
            auth_address = _deployed_address(auth_out)

            guard_out = _run(
                [
                    "forge",
                    "create",
                    "src/OpaqueExternalRoleGuard.sol:OpaqueExternalRoleGuard",
                    "--broadcast",
                    "--rpc-url",
                    rpc_url,
                    "--private-key",
                    ANVIL_KEY,
                    "--constructor-args",
                    auth_address,
                ],
                cwd=root,
            )
            guard_address = _deployed_address(guard_out)

            (root / "contract_meta.json").write_text(
                json.dumps(
                    {
                        "address": guard_address,
                        "contract_name": "OpaqueExternalRoleGuard",
                        "compiler_version": "v0.8.19+commit.7dd6d404",
                    }
                )
                + "\n"
            )
            (root / "slither_results.json").write_text(json.dumps({"results": {"detectors": []}}) + "\n")

            analysis = collect_contract_analysis(root)
            semantic = build_semantic_guards(analysis)
            plan = build_control_tracking_plan(analysis)
            target_snapshot = build_control_snapshot(plan, rpc_url)

            merged_semantic, artifact = refine_semantic_guards_with_hevm(
                semantic,
                tracking_plan=cast(Any, plan),
                rpc_url=rpc_url,
                project_dir=root,
            )

            auth_snapshot = build_control_snapshot(
                {
                    "schema_version": "0.1",
                    "contract_address": auth_address,
                    "contract_name": "OpaqueRoleAuth",
                    "tracking_strategy": "event_first_with_polling_fallback",
                    "tracked_controllers": [
                        {
                            "controller_id": "role_identifier:BREAK_GLASS",
                            "label": "BREAK_GLASS",
                            "source": "BREAK_GLASS",
                            "kind": "role_identifier",
                            "read_spec": {"strategy": "getter_call", "target": "BREAK_GLASS"},
                            "tracking_mode": "state_only",
                            "event_watch": None,
                            "polling_fallback": {
                                "contract_address": auth_address,
                                "polling_sources": ["BREAK_GLASS"],
                                "cadence": "state_only",
                                "notes": [],
                            },
                            "notes": [],
                        }
                    ],
                    "tracked_policies": [],
                },
                rpc_url,
            )

            ep = build_effective_permissions(
                cast(Any, analysis),
                target_snapshot=cast(Any, target_snapshot),
                semantic_guards=merged_semantic,
                external_snapshots={
                    auth_address.lower(): {
                        "contract_name": "OpaqueRoleAuth",
                        "controller_values": auth_snapshot["controller_values"],
                    }
                },
            )

            pause = next(fn for fn in ep["functions"] if fn["function"] == "pause()")
            assert pause["controllers"] == [
                {
                    "controller_id": "role_identifier:BREAK_GLASS",
                    "label": "BREAK_GLASS",
                    "source": "BREAK_GLASS",
                    "kind": "role_identifier",
                    "principals": [
                        {
                            "address": DEPLOYER,
                            "resolved_type": "eoa",
                            "details": {"address": DEPLOYER},
                            "source_controller_id": "role_identifier:BREAK_GLASS",
                        }
                    ],
                    "notes": [],
                }
            ]
            attempts = artifact["functions"][0]["attempts"]
            assert any(
                attempt.get("proof") in {"role_helper", "target_role_membership_finite"}
                and attempt.get("status") == "proved"
                for attempt in attempts
            )
        finally:
            anvil.terminate()
            try:
                anvil.wait(timeout=5)
            except subprocess.TimeoutExpired:
                anvil.kill()
