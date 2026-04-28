import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.policy.principal_enrichment import build_principal_labels


def test_build_principal_labels_enriches_safe_admin_and_operator(monkeypatch):
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "BoringVault",
        "functions": [
            {
                "function": "manage(address,bytes,uint256)",
                "effect_labels": ["arbitrary_external_call"],
                "authority_public": False,
                "authority_roles": [
                    {
                        "role": 1,
                        "principals": [
                            {
                                "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "resolved_type": "unknown",
                                "details": {},
                            }
                        ],
                    }
                ],
                "direct_owner": None,
            },
            {
                "function": "setAuthority(address)",
                "effect_labels": ["authority_update"],
                "authority_public": False,
                "authority_roles": [
                    {
                        "role": 8,
                        "principals": [
                            {
                                "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "resolved_type": "safe",
                                "details": {
                                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                    "owners": [
                                        "0xcccccccccccccccccccccccccccccccccccccccc",
                                        "0xdddddddddddddddddddddddddddddddddddddddd",
                                    ],
                                    "threshold": 2,
                                },
                            }
                        ],
                    }
                ],
                "direct_owner": None,
            },
        ],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "BoringVault",
                "contract_name": "BoringVault",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "node_type": "principal",
                "resolved_type": "safe",
                "label": "owner",
                "contract_name": None,
                "depth": 2,
                "analyzed": False,
                "details": {
                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "owners": [
                        "0xcccccccccccccccccccccccccccccccccccccccc",
                        "0xdddddddddddddddddddddddddddddddddddddddd",
                    ],
                    "threshold": 2,
                },
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "relation": "controller_value",
                "label": "owner",
                "source_controller_id": "state_variable:owner",
                "notes": [],
            }
        ],
    }

    monkeypatch.setattr(
        "services.policy.principal_enrichment.classify_resolved_address_with_status",
        lambda rpc_url, address: ("eoa", {"address": address}, True),
    )

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
        rpc_url="http://rpc.example",
    )

    principals = {item["address"]: item for item in payload["principals"]}

    manage_principal = principals["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    assert manage_principal["resolved_type"] == "eoa"
    assert manage_principal["display_name"] == "BoringVault manager"
    assert "boringvault_manager" in manage_principal["labels"]
    assert "boringvault_role_1_holder" in manage_principal["labels"]

    admin_safe = principals["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
    assert admin_safe["resolved_type"] == "safe"
    assert admin_safe["display_name"] == "BoringVault admin Safe"
    assert "boringvault_admin" in admin_safe["labels"]
    assert "safe_multisig" in admin_safe["labels"]


def test_build_principal_labels_with_resolved_graph_admin_safe():
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "setAuthority(address)",
                "effect_labels": ["authority_update"],
                "authority_public": False,
                "authority_roles": [
                    {
                        "role": 8,
                        "principals": [
                            {
                                "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "resolved_type": "safe",
                                "details": {
                                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                    "owners": ["0xcccccccccccccccccccccccccccccccccccccccc"],
                                    "threshold": 1,
                                },
                            }
                        ],
                    }
                ],
                "direct_owner": None,
            }
        ],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "node_type": "principal",
                "resolved_type": "safe",
                "label": "owner",
                "contract_name": None,
                "depth": 1,
                "analyzed": False,
                "details": {
                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "owners": ["0xcccccccccccccccccccccccccccccccccccccccc"],
                    "threshold": 1,
                },
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "relation": "controller_value",
                "label": "owner",
                "source_controller_id": "state_variable:owner",
                "notes": [],
            }
        ],
    }

    payload = build_principal_labels(effective_permissions, resolved_control_graph=resolved_graph)

    assert payload["contract_name"] == "Target"
    principals = {item["address"]: item for item in payload["principals"]}
    assert principals["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]["display_name"] == "Target admin Safe"


def test_build_principal_labels_includes_generic_controller_principals(monkeypatch):
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [
            {
                "function": "pause()",
                "effect_labels": ["pause_toggle"],
                "authority_public": False,
                "authority_roles": [],
                "direct_owner": None,
                "controllers": [
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
                            }
                        ],
                        "notes": [],
                    }
                ],
            }
        ],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "node_type": "principal",
                "resolved_type": "eoa",
                "label": "governance",
                "contract_name": None,
                "depth": 1,
                "analyzed": False,
                "details": {"address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "relation": "controller_value",
                "label": "governance",
                "source_controller_id": "state_variable:governance",
                "notes": [],
            }
        ],
    }

    monkeypatch.setattr(
        "services.policy.principal_enrichment.classify_resolved_address_with_status",
        lambda rpc_url, address: ("eoa", {"address": address}, True),
    )

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
        rpc_url="http://rpc.example",
    )

    principals = {item["address"]: item for item in payload["principals"]}
    governance = principals["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    assert governance["display_name"] == "Target governance"
    assert "target_controller_governance" in governance["labels"]
    assert governance["controller_context"] == ["governance"]


def test_build_principal_labels_prefers_analyzed_contract_name_for_contract_principals():
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0x2222222222222222222222222222222222222222",
                "address": "0x2222222222222222222222222222222222222222",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "role principal",
                "contract_name": "Executor",
                "depth": 1,
                "analyzed": True,
                "details": {"address": "0x2222222222222222222222222222222222222222"},
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0x2222222222222222222222222222222222222222",
                "relation": "controller_value",
                "label": "governance",
                "source_controller_id": "state_variable:governance",
                "notes": [],
            }
        ],
    }

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
    )

    principals = {item["address"]: item for item in payload["principals"]}
    assert principals["0x2222222222222222222222222222222222222222"]["display_name"] == "Executor"


def test_build_principal_labels_uses_graph_context_for_unnamed_contract_principals():
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0x3333333333333333333333333333333333333333",
                "address": "0x3333333333333333333333333333333333333333",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "role principal",
                "contract_name": None,
                "depth": 1,
                "analyzed": False,
                "details": {"address": "0x3333333333333333333333333333333333333333"},
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x4444444444444444444444444444444444444444",
                "to_id": "address:0x3333333333333333333333333333333333333333",
                "relation": "controller_value",
                "label": "token",
                "source_controller_id": "state_variable:token",
                "notes": [],
            }
        ],
    }
    resolved_graph["nodes"].append(
        {
            "id": "address:0x4444444444444444444444444444444444444444",
            "address": "0x4444444444444444444444444444444444444444",
            "node_type": "contract",
            "resolved_type": "contract",
            "label": "TokenManager",
            "contract_name": "TokenManager",
            "depth": 0,
            "analyzed": True,
            "details": {"address": "0x4444444444444444444444444444444444444444"},
            "artifacts": {},
        }
    )

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
    )

    principals = {item["address"]: item for item in payload["principals"]}
    assert principals["0x3333333333333333333333333333333333333333"]["display_name"] == "TokenManager token"


def test_build_principal_labels_skips_nonterminal_contract_principals():
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0x2222222222222222222222222222222222222222",
                "address": "0x2222222222222222222222222222222222222222",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Executor",
                "contract_name": "Executor",
                "depth": 1,
                "analyzed": True,
                "details": {"address": "0x2222222222222222222222222222222222222222"},
                "artifacts": {},
            },
            {
                "id": "address:0x3333333333333333333333333333333333333333",
                "address": "0x3333333333333333333333333333333333333333",
                "node_type": "principal",
                "resolved_type": "safe",
                "label": "owner",
                "contract_name": None,
                "depth": 2,
                "analyzed": False,
                "details": {
                    "address": "0x3333333333333333333333333333333333333333",
                    "owners": ["0x4444444444444444444444444444444444444444"],
                    "threshold": 1,
                },
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0x2222222222222222222222222222222222222222",
                "relation": "controller_value",
                "label": "adminExecutor",
                "source_controller_id": "state_variable:adminExecutor",
                "notes": [],
            },
            {
                "from_id": "address:0x2222222222222222222222222222222222222222",
                "to_id": "address:0x3333333333333333333333333333333333333333",
                "relation": "controller_value",
                "label": "owner",
                "source_controller_id": "state_variable:owner",
                "notes": [],
            },
        ],
    }

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
    )

    principals = {item["address"]: item for item in payload["principals"]}
    assert "0x2222222222222222222222222222222222222222" not in principals
    assert "0x3333333333333333333333333333333333333333" in principals


def test_build_principal_labels_skips_unresolved_aragon_app_contract_principals():
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0x2222222222222222222222222222222222222222",
                "address": "0x2222222222222222222222222222222222222222",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Lido",
                "contract_name": "Lido",
                "depth": 1,
                "analyzed": True,
                "details": {
                    "address": "0x2222222222222222222222222222222222222222",
                    "authority_kind": "aragon_app_like",
                },
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0x2222222222222222222222222222222222222222",
                "relation": "role_principal",
                "label": "role principal",
                "source_controller_id": None,
                "notes": [],
            }
        ],
    }

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
    )

    principals = {item["address"]: item for item in payload["principals"]}
    assert "0x2222222222222222222222222222222222222222" not in principals


def test_build_principal_labels_skips_permission_controller_contract_principals():
    effective_permissions = {
        "contract_address": "0x1111111111111111111111111111111111111111",
        "contract_name": "Target",
        "functions": [],
    }
    resolved_graph = {
        "nodes": [
            {
                "id": "address:0x1111111111111111111111111111111111111111",
                "address": "0x1111111111111111111111111111111111111111",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "Target",
                "contract_name": "Target",
                "depth": 0,
                "analyzed": True,
                "details": {"address": "0x1111111111111111111111111111111111111111"},
                "artifacts": {},
            },
            {
                "id": "address:0x2222222222222222222222222222222222222222",
                "address": "0x2222222222222222222222222222222222222222",
                "node_type": "contract",
                "resolved_type": "contract",
                "label": "PermissionController",
                "contract_name": "PermissionController",
                "depth": 1,
                "analyzed": True,
                "details": {
                    "address": "0x2222222222222222222222222222222222222222",
                    "controller_label": "permissionController",
                },
                "artifacts": {},
            },
        ],
        "edges": [
            {
                "from_id": "address:0x1111111111111111111111111111111111111111",
                "to_id": "address:0x2222222222222222222222222222222222222222",
                "relation": "controller_value",
                "label": "permissionController",
                "source_controller_id": "external_contract:permissionController",
                "notes": [],
            }
        ],
    }

    payload = build_principal_labels(
        effective_permissions,
        resolved_control_graph=resolved_graph,
    )

    principals = {item["address"]: item for item in payload["principals"]}
    assert "0x2222222222222222222222222222222222222222" not in principals
