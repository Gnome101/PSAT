from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import services.static.hevm_guard_analysis as hevm
from services.static.hevm_guard_analysis import refine_semantic_guards_with_hevm


def test_shutil_which_finds_executable(tmp_path, monkeypatch):
    tool = tmp_path / "hevm"
    tool.write_text("#!/bin/sh\nexit 0\n")
    tool.chmod(tool.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", str(tmp_path))

    assert hevm._shutil_which("hevm") == str(tool)


def test_ensure_hevm_binary_prefers_explicit_and_validates(tmp_path):
    binary = tmp_path / "hevm"
    binary.write_text("bin")

    assert hevm._ensure_hevm_binary(str(binary)) == binary.resolve()


def test_ensure_hevm_binary_downloads_when_needed(tmp_path, monkeypatch):
    monkeypatch.setattr(hevm, "_shutil_which", lambda _name: None)
    monkeypatch.setattr(hevm.platform, "system", lambda: "Linux")
    monkeypatch.setattr(hevm.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(hevm.Path, "home", lambda: tmp_path)

    def fake_urlretrieve(url, destination):
        Path(destination).write_text("downloaded")

    monkeypatch.setattr(hevm.urllib.request, "urlretrieve", fake_urlretrieve)

    resolved = hevm._ensure_hevm_binary()

    assert resolved.exists()
    assert os.access(resolved, os.X_OK)


def test_ensure_hevm_binary_rejects_unsupported_platform(monkeypatch):
    monkeypatch.setattr(hevm, "_shutil_which", lambda _name: None)
    monkeypatch.setattr(hevm.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(hevm.platform, "machine", lambda: "arm64")

    try:
        hevm._ensure_hevm_binary()
    except RuntimeError as exc:
        assert "Auto-download is only wired" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_canonical_function_name_and_selector_helpers():
    assert hevm._canonical_function_name("upgradeTo(address)") == "upgradeTo_address"
    assert hevm._selector("pause()").startswith("0x")
    assert len(hevm._selector("pause()")) == 10


def test_source_generators_include_expected_markers():
    owner_source = hevm._owner_helper_test_source(
        "0x1111111111111111111111111111111111111111",
        "onlyProtocolUpgrader",
    )
    assert "prove_non_owner_rejected" in owner_source
    assert "onlyProtocolUpgrader" in owner_source

    role_source = hevm._role_helper_test_source(
        "0x1111111111111111111111111111111111111111",
        "z",
        ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    )
    assert "prove_non_member_rejected" in role_source
    assert "prove_member_0_accepted" in role_source

    target_source = hevm._target_role_membership_test_source(
        "0x1111111111111111111111111111111111111111",
        "pause()",
        ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
    )
    assert "prove_fixed_non_member_rejected" in target_source
    assert hevm._selector("pause()")[2:] in target_source


def test_resolve_authority_address_and_role_candidate_helpers(monkeypatch):
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "state_variable:roleRegistry",
                "label": "roleRegistry",
                "source": "roleRegistry",
                "kind": "state_variable",
                "read_spec": {"strategy": "getter_call", "target": "roleRegistry"},
                "tracking_mode": "state_only",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["roleRegistry"],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            },
            {
                "controller_id": "role_identifier:BREAK_GLASS",
                "label": "BREAK_GLASS",
                "source": "BREAK_GLASS",
                "kind": "role_identifier",
                "read_spec": {"strategy": "getter_call", "target": "BREAK_GLASS", "contract_source": "auth"},
                "tracking_mode": "state_only",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x2222222222222222222222222222222222222222",
                    "polling_sources": ["BREAK_GLASS"],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            },
        ],
        "tracked_policies": [],
    }

    def fake_snapshot(plan, _rpc_url):
        controller_ids = [controller["controller_id"] for controller in plan["tracked_controllers"]]
        values = {}
        if "state_variable:roleRegistry" in controller_ids:
            values["state_variable:roleRegistry"] = {
                "source": "roleRegistry",
                "value": "0x2222222222222222222222222222222222222222",
                "block_number": 1,
                "observed_via": "call",
                "resolved_type": "contract",
                "details": {},
            }
        if "role_identifier:BREAK_GLASS" in controller_ids:
            values["role_identifier:BREAK_GLASS"] = {
                "source": "BREAK_GLASS",
                "value": "0x" + "11" * 32,
                "block_number": 1,
                "observed_via": "call",
                "resolved_type": "contract",
                "details": {"resolved_principals": [{"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
            }
        return {"controller_values": values}

    monkeypatch.setattr(hevm, "build_control_snapshot", fake_snapshot)

    assert (
        hevm._resolve_authority_address(tracking_plan, "https://rpc.example", "roleRegistry")
        == "0x2222222222222222222222222222222222222222"
    )
    candidates = hevm._role_candidates_for_authority(tracking_plan, "auth")
    assert [candidate["source"] for candidate in candidates] == ["BREAK_GLASS"]

    snapshot = hevm._resolve_role_candidate_snapshot(
        tracking_plan,
        "https://rpc.example",
        "auth",
        candidates[0],
    )
    assert snapshot is not None
    assert snapshot["value"] == "0x" + "11" * 32


def test_make_temp_authority_project_writes_remappings_and_sources(tmp_path, monkeypatch):
    monkeypatch.setattr(hevm.tempfile, "mkdtemp", lambda prefix: str(tmp_path / "authority"))
    result = {
        "ContractName": "Auth",
        "CompilerVersion": "v0.8.19+commit.7dd6d404",
        "SourceCode": "",
    }
    monkeypatch.setattr(hevm, "parse_remappings", lambda _result: ["@oz/=lib/openzeppelin-contracts/"])
    monkeypatch.setattr(
        hevm,
        "parse_sources",
        lambda _result: {"src/Auth.sol": "contract Auth {}", "src/Lib.sol": "library Lib {}"},
    )

    project_dir = hevm._make_temp_authority_project(result, "0x1111111111111111111111111111111111111111")

    assert (project_dir / "foundry.toml").exists()
    assert (project_dir / "remappings.txt").read_text().strip() == "@oz/=lib/openzeppelin-contracts/"
    assert (project_dir / "src" / "Auth.sol").read_text() == "contract Auth {}"
    assert (project_dir / "contract_meta.json").exists()


def test_candidate_role_sources_from_authority_contract_success(monkeypatch):
    class FakeVariable:
        def __init__(self, name, type_name):
            self.name = name
            self.type = type_name

    helper = type(
        "Helper",
        (),
        {
            "name": "z",
            "all_state_variables_read": [
                FakeVariable("BREAK_GLASS", "bytes32"),
                FakeVariable("owner", "address"),
            ],
        },
    )()
    subject = type("Subject", (), {"functions": [helper]})()

    monkeypatch.setattr(hevm, "fetch", lambda _address: {"ContractName": "Auth", "CompilerVersion": "v0.8.19"})
    monkeypatch.setattr(hevm, "_make_temp_authority_project", lambda _result, _address: Path("/tmp/fake-auth"))
    monkeypatch.setattr(hevm, "Slither", lambda _path: object())
    monkeypatch.setattr(hevm, "_select_subject_contract", lambda _slither, _name: subject)
    removed = []
    monkeypatch.setattr(hevm.shutil, "rmtree", lambda path, ignore_errors=True: removed.append((path, ignore_errors)))

    assert hevm._candidate_role_sources_from_authority_contract("0xabc", "z") == ["BREAK_GLASS"]
    assert removed


def test_candidate_role_sources_from_authority_contract_handles_fetch_failure(monkeypatch):
    monkeypatch.setattr(hevm, "fetch", lambda _address: (_ for _ in ()).throw(RuntimeError("boom")))
    assert hevm._candidate_role_sources_from_authority_contract("0xabc", "z") == []


def test_candidate_role_sources_from_local_project_success(monkeypatch, tmp_path):
    class FakeVariable:
        def __init__(self, name, type_name):
            self.name = name
            self.type = type_name

    class FakeFunction:
        def __init__(self):
            self.name = "z"
            self.parameters = [type("Parameter", (), {"type": "address"})()]
            self.all_state_variables_read = [FakeVariable("BREAK_GLASS", "bytes32")]
            self.nodes = [type("Node", (), {"state_variables_read": [FakeVariable("SHIELD", "bytes32")]})()]

    fake_contract = type("Contract", (), {"is_interface": False, "is_library": False, "functions": [FakeFunction()]})()
    monkeypatch.setattr(hevm, "Slither", lambda _path: type("SlitherResult", (), {"contracts": [fake_contract]})())

    assert hevm._candidate_role_sources_from_local_project(tmp_path, "z") == ["BREAK_GLASS", "SHIELD"]


def test_role_snapshot_from_source_builds_snapshot(monkeypatch):
    monkeypatch.setattr(
        hevm,
        "build_control_snapshot",
        lambda plan, _rpc: {
            "controller_values": {
                "role_identifier:BREAK_GLASS": {
                    "source": "BREAK_GLASS",
                    "value": "0x" + "11" * 32,
                    "block_number": 1,
                    "observed_via": "call",
                    "resolved_type": "contract",
                    "details": {},
                }
            }
        },
    )

    snapshot = hevm._role_snapshot_from_source(
        "0x1111111111111111111111111111111111111111",
        "BREAK_GLASS",
        "https://rpc.example",
    )
    assert snapshot is not None
    assert snapshot["source"] == "BREAK_GLASS"


def test_prove_owner_helper_handles_build_failure(monkeypatch):
    monkeypatch.setattr(hevm.tempfile, "mkdtemp", lambda prefix: "/tmp/psat_hevm_owner_helper_test")
    monkeypatch.setattr(
        hevm,
        "_run",
        lambda cmd, cwd: subprocess.CompletedProcess(cmd, 1, stdout="bad build", stderr="compile fail"),
    )
    monkeypatch.setattr(hevm.shutil, "rmtree", lambda *_args, **_kwargs: None)

    result = hevm._prove_owner_helper(
        authority_address="0x1111111111111111111111111111111111111111",
        helper_name="onlyProtocolUpgrader",
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )

    assert result["status"] == "error"
    assert result["reason"] == "forge_build_failed"


def test_prove_owner_helper_success_and_failure(monkeypatch):
    monkeypatch.setattr(hevm.tempfile, "mkdtemp", lambda prefix: "/tmp/psat_hevm_owner_helper_success")
    outputs = [
        subprocess.CompletedProcess(["forge"], 0, stdout="build ok", stderr=""),
        subprocess.CompletedProcess(
            ["hevm"],
            0,
            stdout="\x1b[32m[PASS] prove_owner_accepted\n[PASS] prove_non_owner_rejected\x1b[0m",
            stderr="",
        ),
    ]
    monkeypatch.setattr(hevm, "_run", lambda cmd, cwd: outputs.pop(0))
    monkeypatch.setattr(hevm.shutil, "rmtree", lambda *_args, **_kwargs: None)

    result = hevm._prove_owner_helper(
        authority_address="0x1111111111111111111111111111111111111111",
        helper_name="onlyProtocolUpgrader",
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )

    assert result["status"] == "proved"

    outputs = [
        subprocess.CompletedProcess(["forge"], 0, stdout="build ok", stderr=""),
        subprocess.CompletedProcess(["hevm"], 0, stdout="[PASS] prove_owner_accepted", stderr=""),
    ]
    monkeypatch.setattr(hevm, "_run", lambda cmd, cwd: outputs.pop(0))
    failure = hevm._prove_owner_helper(
        authority_address="0x1111111111111111111111111111111111111111",
        helper_name="onlyProtocolUpgrader",
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )
    assert failure["status"] == "failed"


def test_prove_role_and_target_role_membership_branches(monkeypatch):
    monkeypatch.setattr(hevm.tempfile, "mkdtemp", lambda prefix: "/tmp/psat_hevm_role_helper_test")
    monkeypatch.setattr(hevm.shutil, "rmtree", lambda *_args, **_kwargs: None)

    assert hevm._prove_role_helper(
        authority_address="0x1111111111111111111111111111111111111111",
        helper_name="z",
        members=[],
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )["reason"] == "no_role_members"

    outputs = [
        subprocess.CompletedProcess(["forge"], 0, stdout="build ok", stderr=""),
        subprocess.CompletedProcess(
            ["hevm"],
            0,
            stdout="[PASS] prove_non_member_rejected\n[PASS] prove_member_0_accepted",
            stderr="",
        ),
    ]
    monkeypatch.setattr(hevm, "_run", lambda cmd, cwd: outputs.pop(0))
    proved = hevm._prove_role_helper(
        authority_address="0x1111111111111111111111111111111111111111",
        helper_name="z",
        members=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )
    assert proved["status"] == "proved"

    assert hevm._prove_target_role_membership(
        target_address="0x1111111111111111111111111111111111111111",
        function_signature="upgradeTo(address)",
        members=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )["reason"] == "non_zero_arg_function_unsupported"

    outputs = [
        subprocess.CompletedProcess(["forge"], 0, stdout="build ok", stderr=""),
        subprocess.CompletedProcess(
            ["hevm"],
            0,
            stdout="[PASS] prove_fixed_non_member_rejected\n[PASS] prove_member_0_accepted",
            stderr="",
        ),
    ]
    monkeypatch.setattr(hevm, "_run", lambda cmd, cwd: outputs.pop(0))
    target_proved = hevm._prove_target_role_membership(
        target_address="0x1111111111111111111111111111111111111111",
        function_signature="pause()",
        members=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        rpc_url="https://rpc.example",
        hevm_bin=Path("/tmp/hevm"),
    )
    assert target_proved["status"] == "proved"


def test_refine_semantic_guards_with_hevm_skips_without_rpc():
    merged, artifact = refine_semantic_guards_with_hevm(
        {"contract_address": "0x1", "contract_name": "T", "functions": []},
        tracking_plan={"tracked_controllers": []},
        rpc_url="",
    )
    assert merged["functions"] == []
    assert artifact["status"] == "skipped"
    assert artifact["reason"] == "no_rpc_url"


def test_refine_semantic_guards_with_hevm_skips_when_hevm_missing(monkeypatch):
    monkeypatch.setattr(hevm, "_ensure_hevm_binary", lambda _explicit=None: (_ for _ in ()).throw(RuntimeError("nope")))
    _, artifact = refine_semantic_guards_with_hevm(
        {"contract_address": "0x1", "contract_name": "T", "functions": []},
        tracking_plan={"tracked_controllers": []},
        rpc_url="https://rpc.example",
    )
    assert artifact["status"] == "skipped"
    assert artifact["reason"].startswith("hevm_unavailable:")


def test_refine_semantic_guards_with_hevm_uses_target_role_fallback(monkeypatch):
    semantic_guards = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "pause()",
                "status": "partial",
                "predicates": [
                    {
                        "kind": "external_helper",
                        "authority_source": ["auth"],
                        "helper": "z",
                        "status": "unresolved",
                    }
                ],
                "guard_ids": [],
                "guard_kinds": [],
                "controller_refs": ["auth"],
                "notes": [],
            }
        ],
    }
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "role_identifier:BREAK_GLASS",
                "label": "BREAK_GLASS",
                "source": "BREAK_GLASS",
                "kind": "role_identifier",
                "read_spec": {"strategy": "getter_call", "target": "BREAK_GLASS", "contract_source": "auth"},
                "tracking_mode": "state_only",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["BREAK_GLASS"],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            }
        ],
        "tracked_policies": [],
    }

    monkeypatch.setattr(hevm, "_ensure_hevm_binary", lambda _explicit=None: Path("/tmp/hevm"))
    monkeypatch.setattr(
        hevm,
        "_resolve_authority_address",
        lambda _plan, _rpc, _source: "0x2222222222222222222222222222222222222222",
    )
    monkeypatch.setattr(
        hevm,
        "_prove_owner_helper",
        lambda **_kwargs: {"status": "failed", "reason": "proof_not_established", "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(
        hevm,
        "_resolve_role_candidate_snapshot",
        lambda _plan, _rpc, _source, _role_controller: {
            "value": "0x" + "11" * 32,
            "details": {"resolved_principals": [{"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
        },
    )
    monkeypatch.setattr(
        hevm,
        "_prove_role_helper",
        lambda **_kwargs: {"status": "failed", "reason": "proof_not_established", "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(
        hevm,
        "_prove_target_role_membership",
        lambda **_kwargs: {"status": "proved", "proof": "target_role_membership_finite", "stdout": "", "stderr": ""},
    )

    merged, artifact = refine_semantic_guards_with_hevm(
        semantic_guards,
        tracking_plan=tracking_plan,
        rpc_url="https://rpc.example",
    )

    assert merged["functions"][0]["status"] == "resolved"
    assert artifact["functions"][0]["attempts"][-1]["proof"] == "target_role_membership_finite"


def test_refine_semantic_guards_with_hevm_proves_owner_helper(monkeypatch):
    semantic_guards = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "upgradeTo(address)",
                "status": "partial",
                "predicates": [
                    {
                        "kind": "external_helper",
                        "authority_source": ["roleRegistry"],
                        "helper": "onlyProtocolUpgrader",
                        "status": "unresolved",
                    }
                ],
                "guard_ids": [],
                "guard_kinds": [],
                "controller_refs": ["roleRegistry"],
                "notes": [],
            }
        ],
    }
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [],
        "tracked_policies": [],
    }

    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._ensure_hevm_binary",
        lambda _explicit=None: Path("/tmp/hevm"),
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._resolve_authority_address",
        lambda _plan, _rpc, _source: "0x2222222222222222222222222222222222222222",
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_owner_helper",
        lambda **_kwargs: {"status": "proved", "proof": "owner_helper", "stdout": "", "stderr": ""},
    )

    merged, artifact = refine_semantic_guards_with_hevm(
        semantic_guards,
        tracking_plan=tracking_plan,
        rpc_url="https://rpc.example",
    )

    function = merged["functions"][0]
    assert function["status"] == "resolved"
    assert function["predicates"] == [
        {
            "kind": "caller_equals_controller",
            "controller_kind": "state_variable",
            "controller_label": "owner",
            "controller_source": "owner",
            "read_spec": {
                "strategy": "getter_call",
                "target": "owner",
                "contract_source": "roleRegistry",
            },
            "proven_by": "hevm_owner_helper",
            "authority_source": "roleRegistry",
            "helper": "onlyProtocolUpgrader",
        }
    ]
    assert artifact["status"] == "ok"
    assert artifact["functions"][0]["attempts"][0]["status"] == "proved"


def test_refine_semantic_guards_with_hevm_proves_role_helper(monkeypatch):
    semantic_guards = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "pause()",
                "status": "partial",
                "predicates": [
                    {
                        "kind": "external_helper",
                        "authority_source": ["auth"],
                        "helper": "z",
                        "status": "unresolved",
                    }
                ],
                "guard_ids": [],
                "guard_kinds": [],
                "controller_refs": ["auth"],
                "notes": [],
            }
        ],
    }
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [
            {
                "controller_id": "role_identifier:BREAK_GLASS",
                "label": "BREAK_GLASS",
                "source": "BREAK_GLASS",
                "kind": "role_identifier",
                "read_spec": {"strategy": "getter_call", "target": "BREAK_GLASS", "contract_source": "auth"},
                "tracking_mode": "state_only",
                "event_watch": None,
                "polling_fallback": {
                    "contract_address": "0x1111111111111111111111111111111111111111",
                    "polling_sources": ["BREAK_GLASS"],
                    "cadence": "state_only",
                    "notes": [],
                },
                "notes": [],
            }
        ],
        "tracked_policies": [],
    }

    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._ensure_hevm_binary",
        lambda _explicit=None: Path("/tmp/hevm"),
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._resolve_authority_address",
        lambda _plan, _rpc, _source: "0x2222222222222222222222222222222222222222",
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_owner_helper",
        lambda **_kwargs: {"status": "failed", "reason": "proof_not_established", "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._resolve_role_candidate_snapshot",
        lambda _plan, _rpc, _source, _role_controller: {
            "value": "0x" + "11" * 32,
            "details": {
                "resolved_principals": [
                    {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                    {"address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
                ]
            },
        },
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_role_helper",
        lambda **_kwargs: {"status": "proved", "proof": "role_helper", "stdout": "", "stderr": ""},
    )

    merged, artifact = refine_semantic_guards_with_hevm(
        semantic_guards,
        tracking_plan=tracking_plan,
        rpc_url="https://rpc.example",
    )

    function = merged["functions"][0]
    assert function["status"] == "resolved"
    assert function["predicates"] == [
        {
            "kind": "role_member",
            "role_source": "BREAK_GLASS",
            "authority_source": "auth",
            "read_spec": {
                "strategy": "getter_call",
                "target": "BREAK_GLASS",
                "contract_source": "auth",
            },
            "proven_by": "hevm_role_helper",
            "helper": "z",
        }
    ]
    statuses = [attempt["status"] for attempt in artifact["functions"][0]["attempts"]]
    assert "proved" in statuses


def test_refine_semantic_guards_with_hevm_discovers_role_source_from_authority(monkeypatch):
    semantic_guards = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "pause()",
                "status": "partial",
                "predicates": [
                    {
                        "kind": "external_helper",
                        "authority_source": ["auth"],
                        "helper": "z",
                        "status": "unresolved",
                    }
                ],
                "guard_ids": [],
                "guard_kinds": [],
                "controller_refs": ["auth"],
                "notes": [],
            }
        ],
    }
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [],
        "tracked_policies": [],
    }

    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._ensure_hevm_binary",
        lambda _explicit=None: Path("/tmp/hevm"),
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._resolve_authority_address",
        lambda _plan, _rpc, _source: "0x2222222222222222222222222222222222222222",
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_owner_helper",
        lambda **_kwargs: {"status": "failed", "reason": "proof_not_established", "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._candidate_role_sources_from_authority_contract",
        lambda _authority, _helper: ["BREAK_GLASS"],
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._role_snapshot_from_source",
        lambda _authority, _role_source, _rpc: {
            "value": "0x" + "11" * 32,
            "details": {
                "resolved_principals": [
                    {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                ]
            },
        },
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_role_helper",
        lambda **_kwargs: {"status": "proved", "proof": "role_helper", "stdout": "", "stderr": ""},
    )

    merged, artifact = refine_semantic_guards_with_hevm(
        semantic_guards,
        tracking_plan=tracking_plan,
        rpc_url="https://rpc.example",
    )

    function = merged["functions"][0]
    assert function["status"] == "resolved"
    assert function["predicates"] == [
        {
            "kind": "role_member",
            "role_source": "BREAK_GLASS",
            "authority_source": "auth",
            "read_spec": {
                "strategy": "getter_call",
                "target": "BREAK_GLASS",
                "contract_source": "auth",
            },
            "proven_by": "hevm_role_helper",
            "helper": "z",
        }
    ]
    statuses = [attempt["status"] for attempt in artifact["functions"][0]["attempts"]]
    assert "proved" in statuses


def test_refine_semantic_guards_with_hevm_prefers_local_project_role_sources(monkeypatch, tmp_path):
    semantic_guards = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "pause()",
                "status": "partial",
                "predicates": [
                    {
                        "kind": "external_helper",
                        "authority_source": ["auth"],
                        "helper": "z",
                        "status": "unresolved",
                    }
                ],
                "guard_ids": [],
                "guard_kinds": [],
                "controller_refs": ["auth"],
                "notes": [],
            }
        ],
    }
    tracking_plan = {
        "schema_version": "0.1",
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [],
        "tracked_policies": [],
    }

    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._ensure_hevm_binary",
        lambda _explicit=None: Path("/tmp/hevm"),
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._resolve_authority_address",
        lambda _plan, _rpc, _source: "0x2222222222222222222222222222222222222222",
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_owner_helper",
        lambda **_kwargs: {"status": "failed", "reason": "proof_not_established", "stdout": "", "stderr": ""},
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._candidate_role_sources_from_local_project",
        lambda _project, _helper: ["BREAK_GLASS"],
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._candidate_role_sources_from_authority_contract",
        lambda _authority, _helper: ["SHOULD_NOT_BE_USED"],
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._role_snapshot_from_source",
        lambda _authority, role_source, _rpc: {
            "value": "0x" + ("11" if role_source == "BREAK_GLASS" else "22") * 32,
            "details": {"resolved_principals": [{"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
        },
    )
    monkeypatch.setattr(
        "services.static.hevm_guard_analysis._prove_role_helper",
        lambda **_kwargs: {"status": "proved", "proof": "role_helper", "stdout": "", "stderr": ""},
    )

    merged, _ = refine_semantic_guards_with_hevm(
        semantic_guards,
        tracking_plan=tracking_plan,
        rpc_url="https://rpc.example",
        project_dir=tmp_path,
    )

    function = merged["functions"][0]
    assert function["predicates"][0]["role_source"] == "BREAK_GLASS"
