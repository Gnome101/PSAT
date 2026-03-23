import importlib
import json
import sys
import types


def load_pipeline_module():
    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")
        requests_stub.get = lambda *args, **kwargs: None
        requests_stub.post = lambda *args, **kwargs: None
        sys.modules["requests"] = requests_stub

    if "dotenv" not in sys.modules:
        dotenv_stub = types.ModuleType("dotenv")
        dotenv_stub.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv_stub

    if "main" in sys.modules:
        return importlib.reload(sys.modules["main"])
    return importlib.import_module("main")


def test_process_writes_dependencies_json(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)
    monkeypatch.setattr(
        pipeline,
        "analyze",
        lambda _project_dir, _contract_name, _address: tmp_path / "analysis_report.txt",
    )
    monkeypatch.setattr(pipeline, "analyze_contract", lambda _project_dir: tmp_path / "contract_analysis.json")

    calls = []
    deps_payload = {
        "address": "0x1111111111111111111111111111111111111111",
        "dependencies": ["0x2222222222222222222222222222222222222222"],
        "rpc": "https://rpc.example",
        "network": "ethereum",
    }

    def fake_find_dependencies(address, rpc_url):
        calls.append((address, rpc_url))
        return deps_payload

    monkeypatch.setattr(pipeline, "find_dependencies", fake_find_dependencies)

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=True,
        deps_rpc="https://rpc.example",
        run_dynamic_deps=False,
    )

    written = tmp_path / "dependencies.json"
    assert written.exists()
    assert json.loads(written.read_text()) == deps_payload
    assert calls == [("0x1111111111111111111111111111111111111111", "https://rpc.example")]


def test_process_continues_if_dependency_discovery_fails(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)

    analyze_calls = []

    def fake_analyze(project_dir, contract_name, address):
        analyze_calls.append((project_dir, contract_name, address))
        return tmp_path / "analysis_report.txt"

    monkeypatch.setattr(pipeline, "analyze", fake_analyze)
    monkeypatch.setattr(pipeline, "analyze_contract", lambda _project_dir: tmp_path / "contract_analysis.json")
    monkeypatch.setattr(
        pipeline,
        "find_dependencies",
        lambda _address, _rpc_url: (_ for _ in ()).throw(RuntimeError("RPC unavailable")),
    )

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=True,
        run_dynamic_deps=False,
    )

    assert analyze_calls
    assert not (tmp_path / "dependencies.json").exists()


def test_process_writes_dynamic_dependencies_json(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)
    monkeypatch.setattr(
        pipeline,
        "analyze",
        lambda _project_dir, _contract_name, _address: tmp_path / "analysis_report.txt",
    )
    monkeypatch.setattr(pipeline, "analyze_contract", lambda _project_dir: tmp_path / "contract_analysis.json")

    calls = []
    dyn_payload = {
        "address": "0x1111111111111111111111111111111111111111",
        "rpc": "https://trace-rpc.example",
        "transactions_analyzed": [
            {
                "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "block_number": 1,
                "method_selector": "0xaaaaaaaa",
            }
        ],
        "trace_methods": ["debug_traceTransaction"],
        "dependencies": ["0x2222222222222222222222222222222222222222"],
        "provenance": {
            "0x2222222222222222222222222222222222222222": [
                {
                    "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "block_number": 1,
                    "from": "0x1111111111111111111111111111111111111111",
                    "op": "DELEGATECALL",
                }
            ]
        },
        "dependency_graph": [
            {
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "op": "DELEGATECALL",
                "provenance": [
                    {
                        "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "block_number": 1,
                    }
                ],
            }
        ],
    }

    def fake_find_dynamic_dependencies(address, rpc_url, tx_limit, tx_hashes):
        calls.append((address, rpc_url, tx_limit, tx_hashes))
        return dyn_payload

    monkeypatch.setattr(pipeline, "find_dynamic_dependencies", fake_find_dynamic_dependencies)

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=False,
        run_dynamic_deps=True,
        dynamic_rpc="https://trace-rpc.example",
        dynamic_tx_limit=3,
        dynamic_tx_hashes=["0xtxhash1"],
    )

    written = tmp_path / "dynamic_dependencies.json"
    assert written.exists()
    assert json.loads(written.read_text()) == dyn_payload
    assert calls == [
        (
            "0x1111111111111111111111111111111111111111",
            "https://trace-rpc.example",
            3,
            ["0xtxhash1"],
        )
    ]


def test_process_continues_if_dynamic_dependency_discovery_fails(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)

    analyze_calls = []

    def fake_analyze(project_dir, contract_name, address):
        analyze_calls.append((project_dir, contract_name, address))
        return tmp_path / "analysis_report.txt"

    monkeypatch.setattr(pipeline, "analyze", fake_analyze)
    monkeypatch.setattr(pipeline, "analyze_contract", lambda _project_dir: tmp_path / "contract_analysis.json")
    monkeypatch.setattr(
        pipeline,
        "find_dynamic_dependencies",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("trace unavailable")),
    )

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=False,
        run_dynamic_deps=True,
    )

    assert analyze_calls
    assert not (tmp_path / "dynamic_dependencies.json").exists()


def test_process_writes_contract_analysis_json(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)
    monkeypatch.setattr(
        pipeline,
        "analyze",
        lambda _project_dir, _contract_name, _address: tmp_path / "analysis_report.txt",
    )

    payload = {"schema_version": "0.1", "summary": {"control_model": "ownable"}}

    def fake_analyze_contract(project_dir):
        path = project_dir / "contract_analysis.json"
        path.write_text(json.dumps(payload))
        return path

    monkeypatch.setattr(pipeline, "analyze_contract", fake_analyze_contract)

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=False,
        run_dynamic_deps=False,
    )

    written = tmp_path / "contract_analysis.json"
    assert written.exists()
    assert json.loads(written.read_text()) == payload


def test_process_writes_control_tracking_plan_json(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)
    monkeypatch.setattr(
        pipeline,
        "analyze",
        lambda _project_dir, _contract_name, _address: tmp_path / "analysis_report.txt",
    )

    payload = {
        "schema_version": "0.1",
        "subject": {
            "address": "0x1111111111111111111111111111111111111111",
            "name": "Mock",
            "compiler_version": "v0.8.19",
            "source_verified": True,
        },
        "controller_tracking": [
            {
                "controller_id": "state_variable:owner",
                "label": "owner",
                "source": "owner",
                "kind": "state_variable",
                "tracking_mode": "event_plus_state",
                "writer_functions": [
                    {
                        "contract": "OwnableLike",
                        "function": "transferOwnership(address)",
                        "visibility": "public",
                        "writes": ["owner"],
                        "associated_events": [
                            {
                                "name": "OwnershipTransferred",
                                "signature": "OwnershipTransferred(address,address)",
                                "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                                "inputs": [
                                    {"name": "user", "type": "address", "indexed": True},
                                    {"name": "newOwner", "type": "address", "indexed": True},
                                ],
                            }
                        ],
                        "evidence": [],
                    }
                ],
                "associated_events": [
                    {
                        "name": "OwnershipTransferred",
                        "signature": "OwnershipTransferred(address,address)",
                        "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
                        "inputs": [
                            {"name": "user", "type": "address", "indexed": True},
                            {"name": "newOwner", "type": "address", "indexed": True},
                        ],
                    }
                ],
                "polling_sources": ["owner"],
                "notes": ["Monitor events and confirm with RPC reads."],
            }
        ],
        "policy_tracking": [],
    }

    def fake_analyze_contract(project_dir):
        path = project_dir / "contract_analysis.json"
        path.write_text(json.dumps(payload))
        return path

    monkeypatch.setattr(pipeline, "analyze_contract", fake_analyze_contract)

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=False,
        run_dynamic_deps=False,
    )

    written = tmp_path / "control_tracking_plan.json"
    assert written.exists()
    assert json.loads(written.read_text())["tracked_controllers"][0]["event_watch"]["events"][0]["name"] == "OwnershipTransferred"


def test_process_continues_if_contract_analysis_fails(tmp_path, monkeypatch):
    pipeline = load_pipeline_module()

    monkeypatch.setattr(pipeline, "fetch", lambda _address: {"ContractName": "Mock"})
    monkeypatch.setattr(pipeline, "scaffold", lambda _address, _name, _result: tmp_path)

    analyze_calls = []

    def fake_analyze(project_dir, contract_name, address):
        analyze_calls.append((project_dir, contract_name, address))
        return tmp_path / "analysis_report.txt"

    monkeypatch.setattr(pipeline, "analyze", fake_analyze)
    monkeypatch.setattr(
        pipeline,
        "analyze_contract",
        lambda _project_dir: (_ for _ in ()).throw(RuntimeError("analysis unavailable")),
    )

    pipeline.process(
        "0x1111111111111111111111111111111111111111",
        run_llm=False,
        run_deps=False,
        run_dynamic_deps=False,
    )

    assert analyze_calls
    assert not (tmp_path / "contract_analysis.json").exists()
