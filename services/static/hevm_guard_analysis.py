"""HEVM-backed semantic guard refinement for unresolved external helper paths."""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import stat
import subprocess
import tempfile
import textwrap
import urllib.request
from pathlib import Path
from typing import Any

from eth_utils.crypto import keccak
from slither.slither import Slither

from services.discovery.fetch import fetch, parse_remappings, parse_sources
from services.resolution.tracking import build_control_snapshot
from services.static.contract_analysis_pipeline.shared import _select_subject_contract

HEVM_LINUX_X86_64 = (
    "https://github.com/argotorg/hevm/releases/download/release/0.57.0/hevm-x86_64-linux"
)

_SOLVER = os.getenv("PSAT_HEVM_SOLVER", "z3")
_TIMEOUT_SECONDS = int(os.getenv("PSAT_HEVM_TIMEOUT_SECONDS", "120"))
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _shutil_which(name: str) -> str | None:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(path) / name
        try:
            if candidate.exists() and os.access(candidate, os.X_OK):
                return str(candidate)
        except PermissionError:
            continue
    return None


def _ensure_hevm_binary(explicit: str | None = None) -> Path:
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
        raise RuntimeError(
            "Auto-download is only wired for Linux x86_64 right now. "
            "Set PSAT_HEVM_BIN to an existing binary on this platform."
        )

    cache_dir = Path.home() / ".cache" / "psat" / "hevm"
    cache_dir.mkdir(parents=True, exist_ok=True)
    binary = cache_dir / "hevm-x86_64-linux"
    if not binary.exists():
        urllib.request.urlretrieve(HEVM_LINUX_X86_64, binary)
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return binary


def _run(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=_TIMEOUT_SECONDS,
        check=False,
    )


def _canonical_function_name(signature: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", signature).strip("_") or "function"


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def _owner_helper_test_source(authority_address: str, helper_name: str) -> str:
    authority_literal = f"address(uint160({int(authority_address, 16)}))"
    return f"""// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

interface Vm {{
    function assume(bool) external;
}}

interface IAuthority {{
    function owner() external view returns (address);
    function {helper_name}(address account) external view;
}}

contract HevmOwnerHelperTest {{
    bool public IS_TEST = true;
    Vm internal constant VM = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    IAuthority internal constant AUTHORITY = IAuthority({authority_literal});

    function assertTrue(bool condition) internal pure {{
        require(condition, "assertTrue failed");
    }}

    function prove_non_owner_rejected(address caller) public {{
        address owner = AUTHORITY.owner();
        VM.assume(caller != owner);
        try AUTHORITY.{helper_name}(caller) {{
            assertTrue(false);
        }} catch {{}}
    }}

    function prove_owner_accepted() public {{
        address owner = AUTHORITY.owner();
        AUTHORITY.{helper_name}(owner);
        assertTrue(true);
    }}
}}
"""


def _role_helper_test_source(authority_address: str, helper_name: str, members: list[str]) -> str:
    authority_literal = f"address(uint160({int(authority_address, 16)}))"
    member_literals = [f"address(uint160({int(member, 16)}))" for member in members]
    assumptions = "\n        ".join(f"VM.assume(caller != {member});" for member in member_literals)
    member_tests = "\n".join(
        textwrap
        .dedent(
            f"""\
            function prove_member_{index}_accepted() public {{
                AUTHORITY.{helper_name}({member});
                assertTrue(true);
            }}
            """
        )
        .rstrip()
        for index, member in enumerate(member_literals)
    )
    return f"""// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

interface Vm {{
    function assume(bool) external;
}}

interface IAuthority {{
    function {helper_name}(address account) external view;
}}

contract HevmRoleHelperTest {{
    bool public IS_TEST = true;
    Vm internal constant VM = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    IAuthority internal constant AUTHORITY = IAuthority({authority_literal});

    function assertTrue(bool condition) internal pure {{
        require(condition, "assertTrue failed");
    }}

    function prove_non_member_rejected(address caller) public {{
        {assumptions}
        try AUTHORITY.{helper_name}(caller) {{
            assertTrue(false);
        }} catch {{}}
    }}

{member_tests}
}}
"""


def _target_role_membership_test_source(target_address: str, function_signature: str, members: list[str]) -> str:
    target_literal = f"address(uint160({int(target_address, 16)}))"
    member_literals = [f"address(uint160({int(member, 16)}))" for member in members]
    outsider = "address(uint160(0xdead))"
    member_tests = "\n".join(
        textwrap
        .dedent(
            f"""\
            function prove_member_{index}_accepted() public {{
                VM.prank({member});
                (bool ok,) = TARGET.call(hex"{_selector(function_signature)[2:]}");
                assertTrue(ok);
            }}
            """
        )
        .rstrip()
        for index, member in enumerate(member_literals)
    )
    return f"""// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

interface Vm {{
    function assume(bool) external;
    function prank(address) external;
}}

contract HevmTargetRoleHelperTest {{
    bool public IS_TEST = true;
    Vm internal constant VM = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    address internal constant TARGET = {target_literal};

    function assertTrue(bool condition) internal pure {{
        require(condition, "assertTrue failed");
    }}

    function assertFalse(bool condition) internal pure {{
        require(!condition, "assertFalse failed");
    }}

    function prove_fixed_non_member_rejected() public {{
        VM.prank({outsider});
        (bool ok,) = TARGET.call(hex"{_selector(function_signature)[2:]}");
        assertFalse(ok);
    }}

{member_tests}
}}
"""


def _resolve_authority_address(
    tracking_plan: dict[str, Any],
    rpc_url: str,
    authority_source: str,
) -> str | None:
    candidates = [
        controller
        for controller in tracking_plan.get("tracked_controllers", [])
        if controller.get("source") == authority_source
    ]
    if not candidates:
        return None
    mini_plan = {
        "schema_version": tracking_plan["schema_version"],
        "contract_address": tracking_plan["contract_address"],
        "contract_name": tracking_plan["contract_name"],
        "tracking_strategy": tracking_plan["tracking_strategy"],
        "tracked_controllers": candidates,
        "tracked_policies": [],
    }
    snapshot = build_control_snapshot(mini_plan, rpc_url)
    for controller in candidates:
        value = snapshot["controller_values"].get(controller["controller_id"], {}).get("value")
        if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
            return value
    return None


def _role_candidates_for_authority(
    tracking_plan: dict[str, Any],
    authority_source: str,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for controller in tracking_plan.get("tracked_controllers", []):
        if controller.get("kind") != "role_identifier":
            continue
        read_spec = controller.get("read_spec")
        if not isinstance(read_spec, dict):
            continue
        if read_spec.get("contract_source") != authority_source:
            continue
        candidates.append(controller)
    return candidates


def _resolve_role_candidate_snapshot(
    tracking_plan: dict[str, Any],
    rpc_url: str,
    authority_source: str,
    role_controller: dict[str, Any],
) -> dict[str, Any] | None:
    dependency_controllers = [
        controller
        for controller in tracking_plan.get("tracked_controllers", [])
        if controller.get("source") == authority_source
    ]
    mini_plan = {
        "schema_version": tracking_plan["schema_version"],
        "contract_address": tracking_plan["contract_address"],
        "contract_name": tracking_plan["contract_name"],
        "tracking_strategy": tracking_plan["tracking_strategy"],
        "tracked_controllers": [*dependency_controllers, role_controller],
        "tracked_policies": [],
    }
    snapshot = build_control_snapshot(mini_plan, rpc_url)
    return snapshot["controller_values"].get(role_controller["controller_id"])


def _make_temp_authority_project(result: dict, authority_address: str) -> Path:
    project_dir = Path(tempfile.mkdtemp(prefix=f"psat_hevm_auth_{authority_address[2:10]}_"))
    (project_dir / "src").mkdir(parents=True, exist_ok=True)
    (project_dir / "foundry.toml").write_text(
        '[profile.default]\nsrc = "src"\nout = "out"\nlibs = ["lib"]\nsolc_version = "0.8.19"\n'
    )
    remappings = parse_remappings(result)
    if remappings:
        (project_dir / "remappings.txt").write_text("\n".join(remappings) + "\n")
    for rel, content in parse_sources(result).items():
        path = project_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": authority_address,
                "contract_name": result.get("ContractName", ""),
                "compiler_version": result.get("CompilerVersion", ""),
            }
        )
        + "\n"
    )
    (project_dir / "slither_results.json").write_text(json.dumps({"results": {"detectors": []}}) + "\n")
    return project_dir


def _candidate_role_sources_from_authority_contract(authority_address: str, helper_name: str) -> list[str]:
    try:
        result = fetch(authority_address)
        project_dir = _make_temp_authority_project(result, authority_address)
    except Exception:
        return []

    try:
        slither = Slither(str(project_dir))
        subject = _select_subject_contract(slither, result.get("ContractName"))
        if subject is None:
            return []
        helper = next(
            (
                function
                for function in getattr(subject, "functions", [])
                if getattr(function, "name", "") == helper_name
            ),
            None,
        )
        if helper is None:
            return []
        sources = {
            getattr(variable, "name", "")
            for variable in getattr(helper, "all_state_variables_read", [])
            if getattr(variable, "name", "") and str(getattr(variable, "type", None) or "") == "bytes32"
        }
        return sorted(sources)
    except Exception:
        return []
    finally:
        shutil.rmtree(project_dir, ignore_errors=True)


def _candidate_role_sources_from_local_project(project_dir: Path, helper_name: str) -> list[str]:
    try:
        slither = Slither(str(project_dir))
    except Exception:
        return []

    sources: set[str] = set()
    for contract in slither.contracts:
        if getattr(contract, "is_interface", False) or getattr(contract, "is_library", False):
            continue
        for function in getattr(contract, "functions", []):
            if getattr(function, "name", "") != helper_name:
                continue
            parameters = list(getattr(function, "parameters", []) or [])
            if len(parameters) != 1 or str(getattr(parameters[0], "type", None) or "") != "address":
                continue
            reads = getattr(function, "all_state_variables_read", [])
            if callable(reads):
                reads = reads()
            candidates = list(reads or [])
            for node in getattr(function, "nodes", []) or []:
                candidates.extend(getattr(node, "state_variables_read", []) or [])
            for variable in candidates:
                name = getattr(variable, "name", "")
                if name and str(getattr(variable, "type", None) or "") == "bytes32":
                    sources.add(name)
    return sorted(sources)


def _role_snapshot_from_source(
    authority_address: str,
    role_source: str,
    rpc_url: str,
) -> dict[str, Any] | None:
    mini_plan = {
        "schema_version": "0.1",
        "contract_address": authority_address,
        "contract_name": authority_address,
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": f"role_identifier:{role_source}",
                "label": role_source,
                "source": role_source,
                "kind": "role_identifier",
                "read_spec": {"strategy": "getter_call", "target": role_source},
                "tracking_mode": "state_only",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": authority_address,
                    "polling_sources": [role_source],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            }
        ],
        "tracked_policies": [],
    }
    snapshot = build_control_snapshot(mini_plan, rpc_url)
    return snapshot["controller_values"].get(f"role_identifier:{role_source}")


def _prove_owner_helper(
    *,
    authority_address: str,
    helper_name: str,
    rpc_url: str,
    hevm_bin: Path,
) -> dict[str, Any]:
    project_dir = Path(tempfile.mkdtemp(prefix="psat_hevm_owner_helper_"))
    try:
        (project_dir / "test").mkdir(parents=True, exist_ok=True)
        (project_dir / "foundry.toml").write_text(
            '[profile.default]\nsrc = "src"\ntest = "test"\nout = "out"\nlibs = []\nsolc_version = "0.8.19"\n'
        )
        (project_dir / "test" / "HevmOwnerHelper.t.sol").write_text(
            _owner_helper_test_source(authority_address, helper_name)
        )

        build = _run(["forge", "build", "--ast"], cwd=project_dir)
        if build.returncode != 0:
            return {
                "status": "error",
                "reason": "forge_build_failed",
                "stdout": build.stdout,
                "stderr": build.stderr,
            }

        run = _run(
            [
                str(hevm_bin),
                "test",
                "--root",
                str(project_dir),
                "--rpc",
                rpc_url,
                "--solver",
                _SOLVER,
                "--verb",
                "2",
            ],
            cwd=project_dir,
        )
        output = "\n".join(part for part in [run.stdout, run.stderr] if part)
        normalized_output = _ANSI_ESCAPE_RE.sub("", output)
        owner_ok = "[PASS] prove_owner_accepted" in normalized_output
        reject_ok = "[PASS] prove_non_owner_rejected" in normalized_output
        if run.returncode == 0 and owner_ok and reject_ok:
            return {
                "status": "proved",
                "proof": "owner_helper",
                "stdout": run.stdout,
                "stderr": run.stderr,
            }
        return {
            "status": "failed",
            "reason": "proof_not_established",
            "stdout": run.stdout,
            "stderr": run.stderr,
        }
    finally:
        shutil.rmtree(project_dir, ignore_errors=True)


def _prove_role_helper(
    *,
    authority_address: str,
    helper_name: str,
    members: list[str],
    rpc_url: str,
    hevm_bin: Path,
) -> dict[str, Any]:
    if not members:
        return {"status": "skipped", "reason": "no_role_members"}

    project_dir = Path(tempfile.mkdtemp(prefix="psat_hevm_role_helper_"))
    try:
        (project_dir / "test").mkdir(parents=True, exist_ok=True)
        (project_dir / "foundry.toml").write_text(
            '[profile.default]\nsrc = "src"\ntest = "test"\nout = "out"\nlibs = []\nsolc_version = "0.8.19"\n'
        )
        (project_dir / "test" / "HevmRoleHelper.t.sol").write_text(
            _role_helper_test_source(authority_address, helper_name, members)
        )

        build = _run(["forge", "build", "--ast"], cwd=project_dir)
        if build.returncode != 0:
            return {
                "status": "error",
                "reason": "forge_build_failed",
                "stdout": build.stdout,
                "stderr": build.stderr,
            }

        run = _run(
            [
                str(hevm_bin),
                "test",
                "--root",
                str(project_dir),
                "--rpc",
                rpc_url,
                "--solver",
                _SOLVER,
                "--verb",
                "2",
            ],
            cwd=project_dir,
        )
        output = "\n".join(part for part in [run.stdout, run.stderr] if part)
        normalized_output = _ANSI_ESCAPE_RE.sub("", output)
        reject_ok = "[PASS] prove_non_member_rejected" in normalized_output
        members_ok = all(
            f"[PASS] prove_member_{index}_accepted" in normalized_output for index in range(len(members))
        )
        if run.returncode == 0 and reject_ok and members_ok:
            return {
                "status": "proved",
                "proof": "role_helper",
                "stdout": run.stdout,
                "stderr": run.stderr,
            }
        return {
            "status": "failed",
            "reason": "proof_not_established",
            "stdout": run.stdout,
            "stderr": run.stderr,
        }
    finally:
        shutil.rmtree(project_dir, ignore_errors=True)


def _prove_target_role_membership(
    *,
    target_address: str,
    function_signature: str,
    members: list[str],
    rpc_url: str,
    hevm_bin: Path,
) -> dict[str, Any]:
    if not members:
        return {"status": "skipped", "reason": "no_role_members"}
    if not function_signature.endswith("()"):
        return {"status": "skipped", "reason": "non_zero_arg_function_unsupported"}

    project_dir = Path(tempfile.mkdtemp(prefix="psat_hevm_target_role_"))
    try:
        (project_dir / "test").mkdir(parents=True, exist_ok=True)
        (project_dir / "foundry.toml").write_text(
            '[profile.default]\nsrc = "src"\ntest = "test"\nout = "out"\nlibs = []\nsolc_version = "0.8.19"\n'
        )
        (project_dir / "test" / "HevmTargetRoleHelper.t.sol").write_text(
            _target_role_membership_test_source(target_address, function_signature, members)
        )

        build = _run(["forge", "build", "--ast"], cwd=project_dir)
        if build.returncode != 0:
            return {
                "status": "error",
                "reason": "forge_build_failed",
                "stdout": build.stdout,
                "stderr": build.stderr,
            }

        run = _run(
            [
                str(hevm_bin),
                "test",
                "--root",
                str(project_dir),
                "--rpc",
                rpc_url,
                "--solver",
                _SOLVER,
                "--verb",
                "2",
            ],
            cwd=project_dir,
        )
        output = "\n".join(part for part in [run.stdout, run.stderr] if part)
        normalized_output = _ANSI_ESCAPE_RE.sub("", output)
        reject_ok = "[PASS] prove_fixed_non_member_rejected" in normalized_output
        members_ok = all(
            f"[PASS] prove_member_{index}_accepted" in normalized_output for index in range(len(members))
        )
        if run.returncode == 0 and reject_ok and members_ok:
            return {
                "status": "proved",
                "proof": "target_role_membership_finite",
                "stdout": run.stdout,
                "stderr": run.stderr,
            }
        return {
            "status": "failed",
            "reason": "proof_not_established",
            "stdout": run.stdout,
            "stderr": run.stderr,
        }
    finally:
        shutil.rmtree(project_dir, ignore_errors=True)


def _deepcopy_jsonish(value: Any) -> Any:
    return json.loads(json.dumps(value))


def refine_semantic_guards_with_hevm(
    semantic_guards: dict[str, Any],
    *,
    tracking_plan: dict[str, Any],
    rpc_url: str,
    project_dir: Path | None = None,
    hevm_bin: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    merged = _deepcopy_jsonish(semantic_guards)
    artifact = {
        "schema_version": "0.1",
        "contract_address": semantic_guards.get("contract_address"),
        "contract_name": semantic_guards.get("contract_name"),
        "functions": [],
    }

    if not rpc_url:
        artifact["status"] = "skipped"
        artifact["reason"] = "no_rpc_url"
        return merged, artifact

    try:
        resolved_hevm_bin = _ensure_hevm_binary(hevm_bin or os.getenv("PSAT_HEVM_BIN"))
    except Exception as exc:
        artifact["status"] = "skipped"
        artifact["reason"] = f"hevm_unavailable: {exc}"
        return merged, artifact

    for function in merged.get("functions", []):
        helper_predicates = [p for p in function.get("predicates", []) if p.get("kind") == "external_helper"]
        fn_entry = {"function": function["function"], "attempts": []}
        for predicate in helper_predicates:
            authority_sources = list(predicate.get("authority_source") or [])
            helper_name = predicate.get("helper")
            if len(authority_sources) != 1 or not isinstance(helper_name, str) or not helper_name:
                fn_entry["attempts"].append(
                    {
                        "status": "skipped",
                        "reason": "unsupported_helper_shape",
                        "predicate": predicate,
                    }
                )
                continue
            authority_source = authority_sources[0]
            authority_address = _resolve_authority_address(tracking_plan, rpc_url, authority_source)
            if not authority_address:
                fn_entry["attempts"].append(
                    {
                        "status": "skipped",
                        "reason": "authority_address_unresolved",
                        "predicate": predicate,
                    }
                )
                continue

            owner_proof = _prove_owner_helper(
                authority_address=authority_address,
                helper_name=helper_name,
                rpc_url=rpc_url,
                hevm_bin=resolved_hevm_bin,
            )
            fn_entry["attempts"].append(
                {
                    "authority_source": authority_source,
                    "authority_address": authority_address,
                    "helper": helper_name,
                    **owner_proof,
                }
            )
            if owner_proof["status"] == "proved":
                new_predicate = {
                    "kind": "caller_equals_controller",
                    "controller_kind": "state_variable",
                    "controller_label": "owner",
                    "controller_source": "owner",
                    "read_spec": {
                        "strategy": "getter_call",
                        "target": "owner",
                        "contract_source": authority_source,
                    },
                    "proven_by": "hevm_owner_helper",
                    "authority_source": authority_source,
                    "helper": helper_name,
                }
                function["predicates"] = [p for p in function.get("predicates", []) if p is not predicate]
                function["predicates"].append(new_predicate)
                function["status"] = "resolved"
                function.setdefault("notes", []).append(
                    f"Resolved external helper {authority_source}.{helper_name} via HEVM owner-helper proof."
                )
                continue

            role_attempts = []
            role_snapshots: list[tuple[str, dict[str, Any] | None]] = []
            tracked_role_controllers = _role_candidates_for_authority(tracking_plan, authority_source)
            for role_controller in tracked_role_controllers:
                role_snapshot = _resolve_role_candidate_snapshot(
                    tracking_plan,
                    rpc_url,
                    authority_source,
                    role_controller,
                )
                role_snapshots.append(
                    (role_controller["source"], role_snapshot if isinstance(role_snapshot, dict) else None)
                )

            if not tracked_role_controllers:
                local_role_sources = (
                    _candidate_role_sources_from_local_project(project_dir, helper_name) if project_dir else []
                )
                candidate_sources = local_role_sources or _candidate_role_sources_from_authority_contract(
                    authority_address, helper_name
                )
                for role_source in candidate_sources:
                    role_snapshots.append(
                        (role_source, _role_snapshot_from_source(authority_address, role_source, rpc_url))
                    )

            for role_source, role_snapshot in role_snapshots:
                if not isinstance(role_snapshot, dict):
                    role_attempts.append(
                        {
                            "role_source": role_source,
                            "status": "skipped",
                            "reason": "role_snapshot_unavailable",
                        }
                    )
                    continue
                details = role_snapshot.get("details", {})
                resolved_principals = details.get("resolved_principals", []) if isinstance(details, dict) else []
                members = [
                    str(principal.get("address", "")).lower()
                    for principal in resolved_principals
                    if isinstance(principal, dict) and str(principal.get("address", "")).startswith("0x")
                ]
                role_proof = _prove_role_helper(
                    authority_address=authority_address,
                    helper_name=helper_name,
                    members=members,
                    rpc_url=rpc_url,
                    hevm_bin=resolved_hevm_bin,
                )
                if role_proof["status"] != "proved":
                    role_proof = _prove_target_role_membership(
                        target_address=str(semantic_guards.get("contract_address", "")),
                        function_signature=function["function"],
                        members=members,
                        rpc_url=rpc_url,
                        hevm_bin=resolved_hevm_bin,
                    )
                role_attempts.append(
                    {
                        "role_source": role_source,
                        "role_id": role_snapshot.get("value"),
                        "member_count": len(members),
                        **role_proof,
                    }
                )

            fn_entry["attempts"].extend(
                {
                    "authority_source": authority_source,
                    "authority_address": authority_address,
                    "helper": helper_name,
                    **attempt,
                }
                for attempt in role_attempts
            )

            proved_roles = [attempt for attempt in role_attempts if attempt.get("status") == "proved"]
            if len(proved_roles) != 1:
                continue

            proved_role = proved_roles[0]
            new_predicate = {
                "kind": "role_member",
                "role_source": proved_role["role_source"],
                "authority_source": authority_source,
                "read_spec": {
                    "strategy": "getter_call",
                    "target": proved_role["role_source"],
                    "contract_source": authority_source,
                },
                "proven_by": "hevm_role_helper",
                "helper": helper_name,
            }
            function["predicates"] = [p for p in function.get("predicates", []) if p is not predicate]
            function["predicates"].append(new_predicate)
            function["status"] = "resolved"
            function.setdefault("notes", []).append(
                f"Resolved external helper {authority_source}.{helper_name} via HEVM role-helper proof "
                f"for {proved_role['role_source']}."
            )

        artifact["functions"].append(fn_entry)

    artifact["status"] = "ok"
    return merged, artifact
