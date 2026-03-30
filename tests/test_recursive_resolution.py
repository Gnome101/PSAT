import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.resolution.recursive import _add_edge, _materialize_contract_artifacts, resolve_control_graph


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

    monkeypatch.setattr("services.resolution.recursive._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr("services.resolution.recursive.classify_resolved_address", fake_classify)

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

    monkeypatch.setattr("services.resolution.recursive._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr(
        "services.resolution.recursive.classify_resolved_address",
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

    monkeypatch.setattr("services.resolution.recursive._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr("services.resolution.recursive.classify_resolved_address", fake_classify)

    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url="http://rpc.example",
        max_depth=3,
        refresh_snapshots=False,
    )

    nodes = {node["address"]: node for node in graph["nodes"]}
    edges = {(edge["from_id"], edge["relation"], edge["to_id"]) for edge in graph["edges"]}

    assert materialize_calls == [role_holder_address]


def test_materialize_contract_artifacts_tolerates_slither_cli_failure(monkeypatch, tmp_path):
    address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    project_dir = tmp_path / "recursive_TestContract_aaaaaaaa"
    analysis_path = project_dir / "contract_analysis.json"
    plan_path = project_dir / "control_tracking_plan.json"
    snapshot_path = project_dir / "control_snapshot.json"

    monkeypatch.setattr("services.resolution.recursive.CONTRACTS_DIR", tmp_path)

    monkeypatch.setattr(
        "services.resolution.recursive.classify_single",
        lambda address, rpc_url: {"address": address, "type": "regular"},
        raising=False,
    )
    monkeypatch.setattr(
        "services.resolution.recursive.fetch",
        lambda _address: {"ContractName": "TestContract"},
    )

    def fake_scaffold(_address, _project_name, _result):
        project_dir.mkdir(parents=True, exist_ok=True)
        return project_dir

    monkeypatch.setattr("services.resolution.recursive.scaffold", fake_scaffold)
    monkeypatch.setattr(
        "services.resolution.recursive.analyze",
        lambda project_dir, contract_name, effective_address: (_ for _ in ()).throw(RuntimeError("slither failed")),
    )

    def fake_analyze_contract(_project_dir):
        analysis_path.write_text(
            json.dumps({"subject": {"address": address, "name": "TestContract"}, "access_control": {"privileged_functions": []}}) + "\n"
        )
        return analysis_path

    monkeypatch.setattr("services.resolution.recursive.analyze_contract", fake_analyze_contract)
    monkeypatch.setattr(
        "services.resolution.recursive.write_control_tracking_plan",
        lambda _analysis_path, out_path: out_path.write_text(
            json.dumps(
                {
                    "schema_version": "0.1",
                    "contract_address": address,
                    "contract_name": "TestContract",
                    "tracking_strategy": "event_first_with_polling_fallback",
                    "tracked_controllers": [],
                    "tracked_policies": [],
                }
            )
            + "\n"
        ),
    )

    sentinel = {
        "project_dir": project_dir,
        "analysis_path": analysis_path,
        "plan_path": plan_path,
        "snapshot_path": snapshot_path,
        "analysis": {"subject": {"address": address, "name": "TestContract"}, "access_control": {"privileged_functions": []}},
        "snapshot": {
            "schema_version": "0.1",
            "contract_address": address,
            "contract_name": "TestContract",
            "block_number": 1,
            "controller_values": {},
        },
    }
    project_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(sentinel["snapshot"]) + "\n")
    monkeypatch.setattr(
        "services.resolution.recursive._load_or_build_artifacts",
        lambda analysis_path, rpc_url, refresh_snapshots: sentinel,
    )

    loaded = _materialize_contract_artifacts(
        address,
        "http://rpc.example",
        workspace_prefix="recursive",
        refresh_snapshots=False,
    )

    assert loaded == sentinel


def test_materialize_contract_artifacts_writes_effective_permissions(monkeypatch, tmp_path):
    address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    project_dir = tmp_path / "recursive_TestContract_aaaaaaaa"
    analysis_path = project_dir / "contract_analysis.json"
    plan_path = project_dir / "control_tracking_plan.json"
    snapshot_path = project_dir / "control_snapshot.json"
    effective_permissions_path = project_dir / "effective_permissions.json"

    monkeypatch.setattr("services.resolution.recursive.CONTRACTS_DIR", tmp_path)
    monkeypatch.setattr(
        "services.resolution.recursive.fetch",
        lambda _address: {"ContractName": "TestContract"},
    )
    monkeypatch.setattr("services.resolution.recursive.scaffold", lambda *_args, **_kwargs: project_dir.mkdir(parents=True, exist_ok=True))
    monkeypatch.setattr("services.resolution.recursive.analyze", lambda *args, **kwargs: None)
    def fake_analyze_contract(_project_dir):
        analysis_path.write_text(
            json.dumps({"subject": {"address": address, "name": "TestContract"}, "access_control": {"privileged_functions": []}}) + "\n"
        )
        return analysis_path

    monkeypatch.setattr("services.resolution.recursive.analyze_contract", fake_analyze_contract)
    monkeypatch.setattr(
        "services.resolution.recursive.write_control_tracking_plan",
        lambda _analysis_path, out_path: out_path.write_text(
            json.dumps(
                {
                    "schema_version": "0.1",
                    "contract_address": address,
                    "contract_name": "TestContract",
                    "tracking_strategy": "event_first_with_polling_fallback",
                    "tracked_controllers": [],
                    "tracked_policies": [],
                }
            )
            + "\n"
        ),
    )
    monkeypatch.setattr(
        "services.resolution.recursive._load_or_build_artifacts",
        lambda analysis_path, rpc_url, refresh_snapshots: {
            "project_dir": project_dir,
            "analysis_path": analysis_path,
            "plan_path": plan_path,
            "snapshot_path": snapshot_path,
            "analysis": {"subject": {"address": address, "name": "TestContract"}, "access_control": {"privileged_functions": []}},
            "snapshot": {
                "schema_version": "0.1",
                "contract_address": address,
                "contract_name": "TestContract",
                "block_number": 1,
                "controller_values": {},
            },
        },
    )
    project_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "contract_address": address,
                "contract_name": "TestContract",
                "block_number": 1,
                "controller_values": {},
            }
        )
        + "\n"
    )
    monkeypatch.setattr(
        "services.resolution.recursive.write_effective_permissions_from_files",
        lambda analysis_path, target_snapshot_path, output_path, principal_resolution: output_path.write_text(
            json.dumps(
                {
                    "schema_version": "0.1",
                    "contract_address": address,
                    "contract_name": "TestContract",
                    "principal_resolution": principal_resolution,
                    "artifacts": {},
                    "functions": [],
                }
            )
            + "\n"
        ) or output_path,
    )

    _materialize_contract_artifacts(
        address,
        "http://rpc.example",
        workspace_prefix="recursive",
        refresh_snapshots=False,
    )

    assert effective_permissions_path.exists()


def test_resolve_control_graph_skips_failed_nested_materialization(monkeypatch, tmp_path):
    root_address = "0x1111111111111111111111111111111111111111"
    nested_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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
                    "value": nested_address,
                    "block_number": 1,
                    "observed_via": "eth_call",
                    "resolved_type": "contract",
                    "details": {"address": nested_address},
                }
            },
        },
    )

    monkeypatch.setattr(
        "services.resolution.recursive._materialize_contract_artifacts",
        lambda address, rpc_url, *, workspace_prefix, refresh_snapshots: (_ for _ in ()).throw(
            RuntimeError("nested compile failed")
        ),
    )


def test_resolve_control_graph_names_failed_nested_contract_from_metadata(monkeypatch, tmp_path):
    root_address = "0x1111111111111111111111111111111111111111"
    nested_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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
                "state_variable:pauseRole": {
                    "source": "pauseRole",
                    "value": nested_address,
                    "block_number": 1,
                    "observed_via": "eth_call",
                    "resolved_type": "contract",
                    "details": {"address": nested_address},
                }
            },
        },
    )

    monkeypatch.setattr(
        "services.resolution.recursive._materialize_contract_artifacts",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("materialize failed")),
    )
    monkeypatch.setattr(
        "services.resolution.recursive._contract_name_for_address",
        lambda address: "GateSeal" if address == nested_address else None,
    )

    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url="http://rpc.example",
        max_depth=2,
        refresh_snapshots=False,
    )

    nodes = {node["address"]: node for node in graph["nodes"]}
    assert nodes[nested_address]["label"] == "GateSeal"
    assert nodes[nested_address]["contract_name"] == "GateSeal"
    assert "materialize_error" in nodes[nested_address]["details"]


def test_add_edge_dedupes_nested_safe_owner_edges_across_sources():
    edges = {}
    first = {
        "from_id": "address:0xsafe",
        "to_id": "address:0xowner",
        "relation": "safe_owner",
        "label": "safe owner",
        "source_controller_id": "state_variable:owner",
        "notes": ["path=owner"],
    }
    second = {
        "from_id": "address:0xsafe",
        "to_id": "address:0xowner",
        "relation": "safe_owner",
        "label": "safe owner",
        "source_controller_id": None,
        "notes": ["path=role"],
    }

    _add_edge(edges, first)
    _add_edge(edges, second)

    assert len(edges) == 1
    merged = next(iter(edges.values()))
    assert merged["notes"] == ["path=owner", "path=role"]


def test_resolve_control_graph_skips_self_referential_role_principal_edges(monkeypatch, tmp_path):
    root_address = "0x1111111111111111111111111111111111111111"

    root_analysis_path = _write_artifact_dir(
        tmp_path,
        "root",
        address=root_address,
        contract_name="Voting",
        snapshot={
            "schema_version": "0.1",
            "contract_address": root_address,
            "contract_name": "Voting",
            "block_number": 1,
            "controller_values": {},
        },
    )
    root_analysis_path.with_name("effective_permissions.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "contract_address": root_address,
                "contract_name": "Voting",
                "functions": [
                    {
                        "function": "forward(bytes)",
                        "selector": "0x12345678",
                        "authority_public": False,
                        "authority_roles": [
                            {
                                "role": 1,
                                "principals": [
                                    {
                                        "address": root_address,
                                        "resolved_type": "contract",
                                        "details": {"address": root_address},
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

    monkeypatch.setattr(
        "services.resolution.recursive.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("contract", {"address": address}),
    )

    graph = resolve_control_graph(
        root_analysis_path,
        rpc_url="http://rpc.example",
        max_depth=2,
        refresh_snapshots=False,
    )

    assert all(edge["from_id"] != edge["to_id"] for edge in graph["edges"])
