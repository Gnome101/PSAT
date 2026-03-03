import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.call_graph import discover_dynamic_dependency_files, export_call_graph


def test_export_call_graph_writes_mermaid_dot_and_html(tmp_path):
    payload = {
        "address": "0x1111111111111111111111111111111111111111",
        "dependency_graph": [
            {
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "op": "CALL",
                "provenance": [{"tx_hash": "0xaaa", "block_number": 1}],
            },
            {
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "op": "CALL",
                "provenance": [{"tx_hash": "0xbbb", "block_number": 2}],
            },
            {
                "from": "0x2222222222222222222222222222222222222222",
                "to": "0x3333333333333333333333333333333333333333",
                "op": "DELEGATECALL",
                "provenance": [{"tx_hash": "0xccc", "block_number": 3}],
            },
        ],
    }

    mermaid_path, dot_path, html_path = export_call_graph(payload, tmp_path)

    mermaid = mermaid_path.read_text()
    dot = dot_path.read_text()
    html = html_path.read_text()

    assert mermaid_path.name == "dynamic_call_graph.mmd"
    assert dot_path.name == "dynamic_call_graph.dot"
    assert html_path.name == "dynamic_call_graph.html"
    assert "flowchart LR" in mermaid
    assert "CALL x2" in mermaid
    assert "DELEGATECALL x1" in mermaid
    assert "digraph G {" in dot
    assert "CALL x2" in dot
    assert "Call Graph Explorer" in html
    assert "vis-network.min.js" in html
    assert "0x1111111111111111111111111111111111111111" in html


def test_discover_dynamic_dependency_files_from_dir_and_file(tmp_path):
    project_dir = tmp_path / "contracts" / "Sample"
    project_dir.mkdir(parents=True)
    dynamic_file = project_dir / "dynamic_dependencies.json"
    dynamic_file.write_text(json.dumps({"address": "0x" + "1" * 40, "dependency_graph": []}))

    files_from_dir = discover_dynamic_dependency_files(tmp_path / "contracts")
    files_from_file = discover_dynamic_dependency_files(dynamic_file)

    assert files_from_dir == [dynamic_file]
    assert files_from_file == [dynamic_file]


def test_export_call_graph_html_shows_variable_impact_from_artifact(tmp_path):
    (tmp_path / "contract_meta.json").write_text(json.dumps({"contract_name": "Test"}))

    out_dir = tmp_path / "out" / "Test.sol"
    out_dir.mkdir(parents=True)
    artifact = {
        "methodIdentifiers": {"setValue(uint256)": "55241077"},
        "ast": {
            "nodeType": "SourceUnit",
            "nodes": [
                {
                    "nodeType": "ContractDefinition",
                    "id": 1,
                    "name": "Test",
                    "linearizedBaseContracts": [1],
                    "nodes": [
                        {
                            "nodeType": "VariableDeclaration",
                            "id": 10,
                            "name": "value",
                            "stateVariable": True,
                            "visibility": "public",
                            "mutability": "mutable",
                            "typeDescriptions": {"typeString": "uint256"},
                        },
                        {
                            "nodeType": "FunctionDefinition",
                            "id": 20,
                            "name": "setValue",
                            "kind": "function",
                            "visibility": "external",
                            "stateMutability": "nonpayable",
                            "parameters": {
                                "parameters": [
                                    {
                                        "typeDescriptions": {"typeString": "uint256"},
                                        "typeName": {"nodeType": "ElementaryTypeName", "name": "uint256"},
                                    }
                                ]
                            },
                            "body": {
                                "nodeType": "Block",
                                "statements": [
                                    {
                                        "nodeType": "Assignment",
                                        "operator": "=",
                                        "leftHandSide": {
                                            "nodeType": "Identifier",
                                            "referencedDeclaration": 10,
                                        },
                                        "rightHandSide": {
                                            "nodeType": "Identifier",
                                            "referencedDeclaration": 999,
                                        },
                                    }
                                ],
                            },
                        },
                    ],
                }
            ],
        },
    }
    (out_dir / "Test.json").write_text(json.dumps(artifact))

    payload = {
        "address": "0x1111111111111111111111111111111111111111",
        "transactions_analyzed": [
            {
                "tx_hash": "0xtx1",
                "block_number": 1,
                "method_selector": "0x55241077",
            }
        ],
        "dependency_graph": [
            {
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "op": "CALL",
                "provenance": [{"tx_hash": "0xtx1", "block_number": 1}],
            }
        ],
        "provenance": {
            "0x2222222222222222222222222222222222222222": [
                {
                    "tx_hash": "0xtx1",
                    "block_number": 1,
                    "from": "0x1111111111111111111111111111111111111111",
                    "op": "CALL",
                }
            ]
        },
    }

    _mermaid_path, _dot_path, html_path = export_call_graph(payload, tmp_path)
    html = html_path.read_text()

    assert "setValue(uint256)" in html
    assert "value" in html
    assert "affected_state_vars" in html
    assert "0x2222222222222222222222222222222222222222" in html
