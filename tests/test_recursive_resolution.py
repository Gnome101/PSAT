import sys
from pathlib import Path
from typing import cast

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.resolved_control_graph import ResolvedGraphEdge
from services.resolution.recursive import (
    LoadedArtifacts,
    _add_edge,
    _materialize_contract_artifacts,
    resolve_control_graph,
)


def _bundle(address: str, contract_name: str, *, snapshot: dict, effective_permissions: dict | None = None) -> dict:
    """Build an in-memory ``LoadedArtifacts`` for a contract."""
    plan = {
        "schema_version": "0.1",
        "contract_address": address,
        "contract_name": contract_name,
        "tracking_strategy": "event_first_with_polling_fallback",
        "tracked_controllers": [],
        "tracked_policies": [],
    }
    analysis = {
        "subject": {
            "address": address,
            "name": contract_name,
        }
    }
    bundle = {
        "analysis": analysis,
        "tracking_plan": plan,
        "snapshot": snapshot,
    }
    if effective_permissions is not None:
        bundle["effective_permissions"] = effective_permissions
    return bundle


def test_resolve_control_graph_recurses_to_contract_and_safe(monkeypatch):
    root_address = "0x1111111111111111111111111111111111111111"
    authority_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    safe_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    signer_address = "0xcccccccccccccccccccccccccccccccccccccccc"

    root_bundle = _bundle(
        root_address,
        "Vault",
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

    authority_bundle = _bundle(
        authority_address,
        "RolesAuthority",
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

    def fake_materialize(address, rpc_url, *, workspace_prefix):
        assert address == authority_address
        return authority_bundle

    def fake_classify(rpc_url, address, block_tag="latest"):
        if address == signer_address:
            return "eoa", {"address": signer_address}
        return "unknown", {"address": address}

    monkeypatch.setattr("services.resolution.recursive._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr("services.resolution.recursive.classify_resolved_address", fake_classify)

    graph, nested = resolve_control_graph(
        root_artifacts=cast(LoadedArtifacts, root_bundle),
        rpc_url="http://rpc.example",
        max_depth=3,
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
    # Nested artifact for authority was materialized and returned.
    assert authority_address in nested


def test_resolve_control_graph_dedupes_recursive_contract_addresses(monkeypatch):
    root_address = "0x1111111111111111111111111111111111111111"
    shared_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    root_bundle = _bundle(
        root_address,
        "Vault",
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

    shared_bundle = _bundle(
        shared_address,
        "SharedController",
        snapshot={
            "schema_version": "0.1",
            "contract_address": shared_address,
            "contract_name": "SharedController",
            "block_number": 2,
            "controller_values": {},
        },
    )

    materialize_calls: list[str] = []

    def fake_materialize(address, rpc_url, *, workspace_prefix):
        materialize_calls.append(address)
        return shared_bundle

    monkeypatch.setattr("services.resolution.recursive._materialize_contract_artifacts", fake_materialize)
    monkeypatch.setattr(
        "services.resolution.recursive.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("unknown", {"address": address}),
    )

    graph, _nested = resolve_control_graph(
        root_artifacts=cast(LoadedArtifacts, root_bundle),
        rpc_url="http://rpc.example",
        max_depth=2,
    )

    analyzed_addresses = [node["address"] for node in graph["nodes"] if node.get("analyzed")]
    assert analyzed_addresses.count(shared_address) == 1
    assert materialize_calls == [shared_address]


def test_resolve_control_graph_recurses_into_role_holder_contracts(monkeypatch):
    root_address = "0x1111111111111111111111111111111111111111"
    role_holder_address = "0xdddddddddddddddddddddddddddddddddddddddd"
    safe_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    signer_address = "0xcccccccccccccccccccccccccccccccccccccccc"

    root_bundle = _bundle(
        root_address,
        "Vault",
        snapshot={
            "schema_version": "0.1",
            "contract_address": root_address,
            "contract_name": "Vault",
            "block_number": 1,
            "controller_values": {},
        },
        effective_permissions={
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
        },
    )

    role_holder_bundle = _bundle(
        role_holder_address,
        "ManagerContract",
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

    def fake_materialize(address, rpc_url, *, workspace_prefix):
        materialize_calls.append(address)
        assert address == role_holder_address
        return role_holder_bundle

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

    resolve_control_graph(
        root_artifacts=cast(LoadedArtifacts, root_bundle),
        rpc_url="http://rpc.example",
        max_depth=3,
    )

    assert materialize_calls == [role_holder_address]


# test_materialize_contract_artifacts_tolerates_slither_cli_failure was
# deleted in commit 438a11c (Slither CLI subprocess rip-out). The
# materialize path no longer invokes the CLI, so the failure-tolerance
# test no longer has a code path to exercise.


def test_materialize_contract_artifacts_builds_effective_permissions(monkeypatch):
    address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    monkeypatch.setattr(
        "services.resolution.recursive.classify_single",
        lambda address, rpc_url: {"address": address, "type": "regular"},
        raising=False,
    )
    monkeypatch.setattr(
        "services.resolution.recursive.fetch",
        lambda _address: {"ContractName": "TestContract"},
    )
    monkeypatch.setattr(
        "services.resolution.recursive.scaffold",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "services.resolution.recursive.collect_contract_analysis",
        lambda _project_dir: {
            "subject": {"address": address, "name": "TestContract"},
            "access_control": {"privileged_functions": []},
        },
    )
    monkeypatch.setattr(
        "services.resolution.recursive.build_control_tracking_plan",
        lambda _analysis: {
            "schema_version": "0.1",
            "contract_address": address,
            "contract_name": "TestContract",
            "tracking_strategy": "event_first_with_polling_fallback",
            "tracked_controllers": [],
            "tracked_policies": [],
        },
    )
    monkeypatch.setattr(
        "services.resolution.recursive.build_control_snapshot",
        lambda _plan, _rpc: {
            "schema_version": "0.1",
            "contract_address": address,
            "contract_name": "TestContract",
            "block_number": 1,
            "controller_values": {},
        },
    )
    marker = {"schema_version": "0.1", "functions": []}
    monkeypatch.setattr(
        "services.resolution.recursive._build_effective_permissions",
        lambda _analysis, _snapshot: marker,
    )

    loaded = _materialize_contract_artifacts(
        address,
        "http://rpc.example",
        workspace_prefix="recursive",
    )

    assert loaded.get("effective_permissions") is marker


def test_resolve_control_graph_skips_failed_nested_materialization(monkeypatch):
    root_address = "0x1111111111111111111111111111111111111111"
    nested_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    root_bundle = _bundle(
        root_address,
        "Vault",
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
        lambda address, rpc_url, *, workspace_prefix: (_ for _ in ()).throw(RuntimeError("nested compile failed")),
    )

    graph, _nested = resolve_control_graph(
        root_artifacts=cast(LoadedArtifacts, root_bundle),
        rpc_url="http://rpc.example",
        max_depth=2,
    )

    nodes = {node["address"]: node for node in graph["nodes"]}
    assert nodes[nested_address]["analyzed"] is False
    assert "materialize_error" in nodes[nested_address]["details"]


def test_resolve_control_graph_names_failed_nested_contract_from_metadata(monkeypatch):
    root_address = "0x1111111111111111111111111111111111111111"
    nested_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    root_bundle = _bundle(
        root_address,
        "Vault",
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

    graph, _nested = resolve_control_graph(
        root_artifacts=cast(LoadedArtifacts, root_bundle),
        rpc_url="http://rpc.example",
        max_depth=2,
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

    _add_edge(edges, cast(ResolvedGraphEdge, first))
    _add_edge(edges, cast(ResolvedGraphEdge, second))

    assert len(edges) == 1
    merged = next(iter(edges.values()))
    assert merged["notes"] == ["path=owner", "path=role"]


def test_resolve_control_graph_skips_self_referential_role_principal_edges(monkeypatch):
    root_address = "0x1111111111111111111111111111111111111111"

    root_bundle = _bundle(
        root_address,
        "Voting",
        snapshot={
            "schema_version": "0.1",
            "contract_address": root_address,
            "contract_name": "Voting",
            "block_number": 1,
            "controller_values": {},
        },
        effective_permissions={
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
        },
    )

    monkeypatch.setattr(
        "services.resolution.recursive.classify_resolved_address",
        lambda rpc_url, address, block_tag="latest": ("contract", {"address": address}),
    )

    graph, _nested = resolve_control_graph(
        root_artifacts=cast(LoadedArtifacts, root_bundle),
        rpc_url="http://rpc.example",
        max_depth=2,
    )

    assert all(edge["from_id"] != edge["to_id"] for edge in graph["edges"])
