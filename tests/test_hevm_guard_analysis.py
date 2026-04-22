import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static.hevm_guard_analysis import refine_semantic_guards_with_hevm


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
