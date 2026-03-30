import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.policy.effective_permissions import build_effective_permissions, write_effective_permissions_from_files


def test_build_effective_permissions_resolves_roles_and_safe_details():
    target_analysis = {
        "subject": {
            "address": "0x1111111111111111111111111111111111111111",
            "name": "Target",
        },
        "access_control": {
            "privileged_functions": [
                {
                    "function": "manage(address,bytes,uint256)",
                    "controller_refs": ["authority", "owner"],
                    "effect_targets": ["target.functionCallWithValue"],
                    "effect_labels": ["arbitrary_external_call"],
                    "action_summary": "Executes arbitrary external calldata from the contract.",
                },
                {
                    "function": "setBeforeTransferHook(address)",
                    "controller_refs": ["authority", "owner"],
                    "effect_targets": ["hook"],
                    "effect_labels": ["hook_update"],
                    "action_summary": "Updates hook configuration that can affect later contract behavior.",
                },
            ]
        },
    }
    target_snapshot = {
        "contract_name": "Target",
        "controller_values": {
            "external_contract:authority": {
                "value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_type": "contract",
                "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            },
            "state_variable:owner": {
                "value": "0x0000000000000000000000000000000000000000",
                "resolved_type": "zero",
                "details": {"address": "0x0000000000000000000000000000000000000000"},
            },
        },
    }
    authority_snapshot = {
        "contract_name": "RolesAuthority",
        "controller_values": {
            "state_variable:owner": {
                "value": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "resolved_type": "safe",
                "details": {
                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "owners": [
                        "0x1111111111111111111111111111111111111111",
                        "0x2222222222222222222222222222222222222222",
                    ],
                    "threshold": 2,
                },
            }
        },
    }
    policy_state = {
        "public_capabilities": [],
        "role_capabilities": [
            {
                "role": 1,
                "target": "0x1111111111111111111111111111111111111111",
                "function_sig": "0xf6e715d0",
                "enabled": True,
            },
            {
                "role": 8,
                "target": "0x1111111111111111111111111111111111111111",
                "function_sig": "0x8929565f",
                "enabled": True,
            },
        ],
        "user_roles": [
            {
                "user": "0xcccccccccccccccccccccccccccccccccccccccc",
                "role": 1,
                "enabled": True,
            },
            {
                "user": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "role": 8,
                "enabled": True,
            },
        ],
    }

    payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        authority_snapshot=authority_snapshot,
        policy_state=policy_state,
    )

    assert payload["authority_contract"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert payload["principal_resolution"]["status"] == "complete"
    functions = {item["function"]: item for item in payload["functions"]}

    manage = functions["manage(address,bytes,uint256)"]
    assert manage["selector"] == "0xf6e715d0"
    assert manage["effect_labels"] == ["arbitrary_external_call"]
    assert manage["action_summary"] == "Executes arbitrary external calldata from the contract."
    assert manage["authority_roles"] == [
        {
            "role": 1,
            "principals": [
                {
                    "address": "0xcccccccccccccccccccccccccccccccccccccccc",
                    "resolved_type": "unknown",
                    "details": {},
                }
            ],
        }
    ]

    hook = functions["setBeforeTransferHook(address)"]
    assert hook["selector"] == "0x8929565f"
    assert hook["effect_targets"] == ["hook"]
    assert hook["authority_roles"] == [
        {
            "role": 8,
            "principals": [
                {
                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "resolved_type": "safe",
                    "details": {
                        "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "owners": [
                            "0x1111111111111111111111111111111111111111",
                            "0x2222222222222222222222222222222222222222",
                        ],
                        "threshold": 2,
                    },
                    "source_contract": "RolesAuthority",
                    "source_controller_id": "state_variable:owner",
                }
            ],
        }
    ]


def test_write_effective_permissions_from_files(tmp_path):
    target_analysis_path = tmp_path / "contract_analysis.json"
    target_snapshot_path = tmp_path / "control_snapshot.json"
    authority_snapshot_path = tmp_path / "authority_snapshot.json"
    policy_state_path = tmp_path / "policy_state.json"

    target_analysis_path.write_text(
        json.dumps(
            {
                "subject": {
                    "address": "0x1111111111111111111111111111111111111111",
                    "name": "Target",
                },
                "access_control": {
                    "privileged_functions": [
                        {
                            "function": "manage(address,bytes,uint256)",
                            "controller_refs": ["authority"],
                            "effect_targets": ["target.functionCallWithValue"],
                            "effect_labels": ["arbitrary_external_call"],
                            "action_summary": "Executes arbitrary external calldata from the contract.",
                        }
                    ]
                },
            }
        )
        + "\n"
    )
    target_snapshot_path.write_text(
        json.dumps(
            {
                "contract_name": "Target",
                "controller_values": {
                    "external_contract:authority": {
                        "value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "resolved_type": "contract",
                        "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                    }
                },
            }
        )
        + "\n"
    )
    authority_snapshot_path.write_text(json.dumps({"contract_name": "Authority", "controller_values": {}}) + "\n")
    policy_state_path.write_text(
        json.dumps(
            {
                "public_capabilities": [],
                "role_capabilities": [],
                "user_roles": [],
            }
        )
        + "\n"
    )

    written = write_effective_permissions_from_files(
        target_analysis_path,
        target_snapshot_path=target_snapshot_path,
        authority_snapshot_path=authority_snapshot_path,
        policy_state_path=policy_state_path,
    )

    payload = json.loads(written.read_text())
    assert written.name == "effective_permissions.json"
    assert payload["contract_name"] == "Target"
    assert payload["authority_contract"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert payload["principal_resolution"]["status"] == "complete"
    assert payload["functions"][0]["effect_labels"] == ["arbitrary_external_call"]


def test_build_effective_permissions_handles_vyper_dynarray_signatures():
    target_analysis = {
        "subject": {
            "address": "0x1111111111111111111111111111111111111111",
            "name": "GateSeal",
        },
        "access_control": {
            "privileged_functions": [
                {
                    "function": "seal(DynArray[address,MAX_SEALABLES])",
                    "controller_refs": ["SEALING_COMMITTEE"],
                    "effect_targets": [],
                    "effect_labels": ["external_contract_call", "pause_toggle"],
                    "action_summary": "Calls an external contract from the contract context.",
                }
            ]
        },
    }
    target_snapshot = {
        "contract_name": "GateSeal",
        "controller_values": {
            "state_variable:SEALING_COMMITTEE": {
                "source": "SEALING_COMMITTEE",
                "value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_type": "safe",
                "details": {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "owners": ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
                    "threshold": 1,
                },
            }
        },
    }

    payload = build_effective_permissions(target_analysis, target_snapshot=target_snapshot)
    function = payload["functions"][0]

    assert function["function"] == "seal(DynArray[address,MAX_SEALABLES])"
    assert function["selector"].startswith("0x")
    assert len(function["selector"]) == 10
    assert function["controllers"] == [
        {
            "controller_id": "state_variable:SEALING_COMMITTEE",
            "label": "SEALING_COMMITTEE",
            "source": "SEALING_COMMITTEE",
            "kind": "state_variable",
            "principals": [
                {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "resolved_type": "safe",
                    "details": {
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "owners": ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
                        "threshold": 1,
                    },
                    "source_contract": "GateSeal",
                    "source_controller_id": "state_variable:SEALING_COMMITTEE",
                }
            ],
            "notes": [],
        }
    ]


def test_build_effective_permissions_derives_role_registry_controller_from_effect_targets():
    target_analysis = {
        "subject": {
            "address": "0x1111111111111111111111111111111111111111",
            "name": "EtherFiAdmin",
        },
        "access_control": {
            "privileged_functions": [
                {
                    "function": "upgradeTo(address)",
                    "controller_refs": ["_authorizeUpgrade", "role"],
                    "effect_targets": ["roleRegistry.onlyProtocolUpgrader", "target"],
                    "effect_labels": ["delegatecall_execution", "implementation_update"],
                    "action_summary": "Calls an external contract from the contract context.",
                }
            ]
        },
    }
    target_snapshot = {
        "contract_name": "EtherFiAdmin",
        "controller_values": {
            "external_contract:roleRegistry": {
                "source": "roleRegistry",
                "value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_type": "contract",
                "details": {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "authority_kind": "access_control_like",
                },
            },
            "state_variable:roleRegistry": {
                "source": "roleRegistry",
                "value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_type": "contract",
                "details": {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "authority_kind": "access_control_like",
                },
            },
        },
    }

    payload = build_effective_permissions(target_analysis, target_snapshot=target_snapshot)
    function = payload["functions"][0]

    assert len(function["controllers"]) == 1
    controller = function["controllers"][0]
    assert controller["label"] == "roleRegistry"
    assert controller["source"] == "roleRegistry"
    assert controller["kind"] in {"external_contract", "state_variable"}
    assert controller["principals"] == [
        {
            "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "resolved_type": "contract",
            "details": {
                "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "authority_kind": "access_control_like",
            },
            "source_contract": "EtherFiAdmin",
            "source_controller_id": controller["controller_id"],
        }
    ]


def test_build_effective_permissions_includes_generic_controller_grants():
    target_analysis = {
        "subject": {
            "address": "0x1111111111111111111111111111111111111111",
            "name": "Target",
        },
        "access_control": {
            "privileged_functions": [
                {
                    "function": "pause()",
                    "controller_refs": ["governance"],
                    "effect_targets": ["paused"],
                    "effect_labels": ["pause_toggle"],
                    "action_summary": "Pauses the contract.",
                }
            ]
        },
    }
    target_snapshot = {
        "contract_name": "Target",
        "controller_values": {
            "state_variable:governance": {
                "source": "governance",
                "value": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_type": "eoa",
                "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            }
        },
    }

    payload = build_effective_permissions(target_analysis, target_snapshot=target_snapshot)

    pause = payload["functions"][0]
    assert pause["controllers"] == [
        {
            "controller_id": "state_variable:governance",
            "label": "governance",
            "source": "governance",
            "kind": "state_variable",
            "principals": [
                {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "resolved_type": "eoa",
                    "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                    "source_contract": "Target",
                    "source_controller_id": "state_variable:governance",
                }
            ],
            "notes": [],
        }
    ]


def test_build_effective_permissions_uses_resolved_role_principals_and_skips_non_auth_contracts():
    target_analysis = {
        "subject": {
            "address": "0x1111111111111111111111111111111111111111",
            "name": "Target",
        },
        "access_control": {
            "privileged_functions": [
                {
                    "function": "pause()",
                    "controller_refs": ["PAUSE_ROLE", "LIDO"],
                    "effect_targets": ["paused"],
                    "effect_labels": ["pause_toggle"],
                    "action_summary": "Pauses the contract.",
                }
            ]
        },
    }
    target_snapshot = {
        "contract_name": "Target",
        "controller_values": {
            "role_identifier:PAUSE_ROLE": {
                "source": "PAUSE_ROLE",
                "value": "0x" + "11" * 32,
                "resolved_type": "unknown",
                "details": {
                    "adapter": "access_control_enumerable",
                    "resolved_principals": [
                        {
                            "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "resolved_type": "eoa",
                            "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                        }
                    ],
                },
            },
            "external_contract:LIDO": {
                "source": "LIDO",
                "value": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "resolved_type": "contract",
                "details": {"address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
            },
        },
    }

    payload = build_effective_permissions(target_analysis, target_snapshot=target_snapshot)

    pause = payload["functions"][0]
    assert pause["controllers"] == [
        {
            "controller_id": "role_identifier:PAUSE_ROLE",
            "label": "PAUSE_ROLE",
            "source": "PAUSE_ROLE",
            "kind": "role_identifier",
            "principals": [
                {
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "resolved_type": "eoa",
                    "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                    "source_controller_id": "role_identifier:PAUSE_ROLE",
                }
            ],
            "notes": [],
        }
    ]
