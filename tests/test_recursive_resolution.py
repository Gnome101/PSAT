import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.recursive_resolution import resolve_control_graph


def _write_artifact_dir(base: Path, name: str, *, address: str, contract_name: str, snapshot: dict) -> Path:
    directory = base / name
    directory.mkdir(parents=True)
    (directory / "contract_analysis.json").write_text(
        json.dumps(
            {
                "subject": {
                    "address": address,
                    "name": contract_name,
                }
            }
        )
        + "\n"
    )
    (directory / "control_tracking_plan.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "contract_address": address,
                "contract_name": contract_name,
                "tracking_strategy": "event_first_with_polling_fallback",
                "tracked_controllers": [],
                "tracked_policies": [],
            }
        )
        + "\n"
    )
    (directory / "control_snapshot.json").write_text(json.dumps(snapshot) + "\n")
    return directory / "contract_analysis.json"


def test_resolve_control_graph_recurses_to_contract_and_safe(monkeypatch, tmp_path):
    root_address = "0x1111111111111111111111111111111111111111"
    authority_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    safe_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    signer_address = "0xcccccccccccccccccccccccccccccccccccccccc"

    root_analysis_path = _write_artifact_dir(
        tmp_path,
        "root",
        address=root_address,
        contract_name="Vault",
        snapshot={
            "schema_version": "0.1",
            "contract_address": root_address,
            "contract_name": "Vault",
            "block_number": 1,
            "controller_values": {
                "external_contract:authority": {
                    "source": "authority",
                    "value": authority_address,
                    "block_number": 1,
                    "observed_via": "eth_call",
                    "resolved_type": "contract",
                    "details": {"address": authority_address},
                },
                "state_variable:owner": {
                    "source": "owner",
                    "value": "0x0000000000000000000000000000000000000000",
                    "block_number": 1,
                    "observed_via": "eth_call",
                    "resolved_type": "zero",
                    "details": {"address": "0x0000000000000000000000000000000000000000"},
                },
            },
        },
    )

    nested_path = _write_artifact_dir(
        tmp_path,
        "authority",
        address=authority_address,
        contract_name="RolesAuthority",
        snapshot={
            "schema_version": "0.1",
            "contract_address": authority_address,
            "contract_name": "RolesAuthority",
            "block_number": 2,
            "controller_values": {
                "state_variable:owner": {
                    "source": "owner",
                    "value": safe_address,
                    "block_number": 2,
                    "observed_via": "eth_call",
                    "resolved_type": "safe",
                    "details": {
                        "address": safe_address,
                        "owners": [signer_address],
                        "threshold": 1,
                    },
                }
            },
        },
    )

    def fake_materialize(address, rpc_url, *, workspace_prefix, refresh_snapshots):
        assert address == authority_address
        return {
            "project_dir": nested_path.parent,
            "analysis_path": nested_path,
            "plan_path": nested_path.with_name("control_tracking_plan.json"),
            "snapshot_path": nested_path.with_name("control_snapshot.json"),
            "analysis": json.loads(nested_path.read_text()),
            "snapshot": json.loads(nested_path.with_name("control_snapshot.json").read_text()),
        }

    def fake_classify(rpc_url, address, block_tag="latest"):
        if address == signer_address:
            return "eoa", {"address": signer_address}
        return "unknown", {"address": address}

    monkeypatch.setattr("services.recursive_resolution._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr("services.recursive_resolution.classify_resolved_address", fake_classify)

    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url="http://rpc.example",
        max_depth=3,
        refresh_snapshots=False,
    )

    nodes = {node["address"]: node for node in graph["nodes"]}
    edges = {(edge["from_id"], edge["relation"], edge["to_id"]) for edge in graph["edges"]}

    assert nodes[root_address]["analyzed"] is True
    assert nodes[root_address]["contract_name"] == "Vault"
    assert nodes[authority_address]["analyzed"] is True
    assert nodes[authority_address]["contract_name"] == "RolesAuthority"
    assert nodes[safe_address]["resolved_type"] == "safe"
    assert nodes[signer_address]["resolved_type"] == "eoa"

    assert (
        "address:0x1111111111111111111111111111111111111111",
        "controller_value",
        "address:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ) in edges
    assert (
        "address:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "controller_value",
        "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ) in edges
    assert (
        "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "safe_owner",
        "address:0xcccccccccccccccccccccccccccccccccccccccc",
    ) in edges


def test_resolve_control_graph_dedupes_recursive_contract_addresses(monkeypatch, tmp_path):
    root_address = "0x1111111111111111111111111111111111111111"
    shared_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    root_analysis_path = _write_artifact_dir(
        tmp_path,
        "root",
        address=root_address,
        contract_name="Vault",
        snapshot={
            "schema_version": "0.1",
            "contract_address": root_address,
            "contract_name": "Vault",
            "block_number": 1,
            "controller_values": {
                "external_contract:authority": {
                    "source": "authority",
                    "value": shared_address,
                    "block_number": 1,
                    "observed_via": "eth_call",
                    "resolved_type": "contract",
                    "details": {"address": shared_address},
                },
                "external_contract:guardian": {
                    "source": "guardian",
                    "value": shared_address,
                    "block_number": 1,
                    "observed_via": "eth_call",
                    "resolved_type": "contract",
                    "details": {"address": shared_address},
                },
            },
        },
    )

    nested_path = _write_artifact_dir(
        tmp_path,
        "shared",
        address=shared_address,
        contract_name="SharedController",
        snapshot={
            "schema_version": "0.1",
            "contract_address": shared_address,
            "contract_name": "SharedController",
            "block_number": 2,
            "controller_values": {},
        },
    )

    materialize_calls: list[str] = []

    def fake_materialize(address, rpc_url, *, workspace_prefix, refresh_snapshots):
        materialize_calls.append(address)
        return {
            "project_dir": nested_path.parent,
            "analysis_path": nested_path,
            "plan_path": nested_path.with_name("control_tracking_plan.json"),
            "snapshot_path": nested_path.with_name("control_snapshot.json"),
            "analysis": json.loads(nested_path.read_text()),
            "snapshot": json.loads(nested_path.with_name("control_snapshot.json").read_text()),
        }

    monkeypatch.setattr("services.recursive_resolution._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr(
        "services.recursive_resolution.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("unknown", {"address": address}),
    )

    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url="http://rpc.example",
        max_depth=2,
        refresh_snapshots=False,
    )

    analyzed_addresses = [node["address"] for node in graph["nodes"] if node.get("analyzed")]
    assert analyzed_addresses.count(shared_address) == 1
    assert materialize_calls == [shared_address]


def test_resolve_control_graph_recurses_into_role_holder_contracts(monkeypatch, tmp_path):
    root_address = "0x1111111111111111111111111111111111111111"
    role_holder_address = "0xdddddddddddddddddddddddddddddddddddddddd"
    safe_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    signer_address = "0xcccccccccccccccccccccccccccccccccccccccc"

    root_analysis_path = _write_artifact_dir(
        tmp_path,
        "root",
        address=root_address,
        contract_name="Vault",
        snapshot={
            "schema_version": "0.1",
            "contract_address": root_address,
            "contract_name": "Vault",
            "block_number": 1,
            "controller_values": {},
        },
    )
    root_analysis_path.with_name("effective_permissions.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "contract_address": root_address,
                "contract_name": "Vault",
                "functions": [
                    {
                        "function": "manage(address,bytes,uint256)",
                        "selector": "0x12345678",
                        "authority_public": False,
                        "authority_roles": [
                            {
                                "role": 1,
                                "principals": [
                                    {
                                        "address": role_holder_address,
                                        "resolved_type": "contract",
                                        "details": {"address": role_holder_address},
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        )
        + "\n"
    )

    nested_path = _write_artifact_dir(
        tmp_path,
        "role_holder",
        address=role_holder_address,
        contract_name="ManagerContract",
        snapshot={
            "schema_version": "0.1",
            "contract_address": role_holder_address,
            "contract_name": "ManagerContract",
            "block_number": 2,
            "controller_values": {
                "state_variable:owner": {
                    "source": "owner",
                    "value": safe_address,
                    "block_number": 2,
                    "observed_via": "eth_call",
                    "resolved_type": "safe",
                    "details": {
                        "address": safe_address,
                        "owners": [signer_address],
                        "threshold": 1,
                    },
                }
            },
        },
    )

    materialize_calls: list[str] = []

    def fake_materialize(address, rpc_url, *, workspace_prefix, refresh_snapshots):
        materialize_calls.append(address)
        assert address == role_holder_address
        return {
            "project_dir": nested_path.parent,
            "analysis_path": nested_path,
            "plan_path": nested_path.with_name("control_tracking_plan.json"),
            "snapshot_path": nested_path.with_name("control_snapshot.json"),
            "analysis": json.loads(nested_path.read_text()),
            "snapshot": json.loads(nested_path.with_name("control_snapshot.json").read_text()),
        }

    def fake_classify(rpc_url, address, block_tag="latest"):
        if address == signer_address:
            return "eoa", {"address": signer_address}
        if address == safe_address:
            return "safe", {"address": safe_address, "owners": [signer_address], "threshold": 1}
        if address == role_holder_address:
            return "contract", {"address": role_holder_address}
        return "unknown", {"address": address}

    monkeypatch.setattr("services.recursive_resolution._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr("services.recursive_resolution.classify_resolved_address", fake_classify)

    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url="http://rpc.example",
        max_depth=3,
        refresh_snapshots=False,
    )

    nodes = {node["address"]: node for node in graph["nodes"]}
    edges = {(edge["from_id"], edge["relation"], edge["to_id"]) for edge in graph["edges"]}

    assert materialize_calls == [role_holder_address]
    assert nodes[role_holder_address]["analyzed"] is True
    assert nodes[role_holder_address]["contract_name"] == "ManagerContract"
    assert nodes[safe_address]["resolved_type"] == "safe"
    assert nodes[signer_address]["resolved_type"] == "eoa"
    assert (
        "address:0x1111111111111111111111111111111111111111",
        "role_principal",
        "address:0xdddddddddddddddddddddddddddddddddddddddd",
    ) in edges
    assert (
        "address:0xdddddddddddddddddddddddddddddddddddddddd",
        "controller_value",
        "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ) in edges
    assert (
        "address:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "safe_owner",
        "address:0xcccccccccccccccccccccccccccccccccccccccc",
    ) in edges
