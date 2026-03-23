import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.principal_enrichment import build_principal_labels, write_principal_labels_from_files


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
        "services.principal_enrichment.classify_resolved_address",
        lambda rpc_url, address: ("eoa", {"address": address}),
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


def test_write_principal_labels_from_files(tmp_path):
    effective_permissions_path = tmp_path / "effective_permissions.json"
    resolved_graph_path = tmp_path / "resolved_control_graph.json"

    effective_permissions_path.write_text(
        json.dumps(
            {
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
        )
        + "\n"
    )
    resolved_graph_path.write_text(
        json.dumps(
            {
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
        )
        + "\n"
    )

    output_path = write_principal_labels_from_files(
        effective_permissions_path,
        resolved_control_graph_path=resolved_graph_path,
    )

    payload = json.loads(output_path.read_text())
    assert output_path.name == "principal_labels.json"
    assert payload["contract_name"] == "Target"
    principals = {item["address"]: item for item in payload["principals"]}
    assert principals["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]["display_name"] == "Target admin Safe"
