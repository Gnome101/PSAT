import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.static import collect_contract_analysis
from services.static.contract_analysis_pipeline import build_semantic_guards

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "contracts"


def _write_project(tmp_path: Path, contract_name: str, source_code: str) -> Path:
    project_dir = tmp_path / contract_name
    (project_dir / "src").mkdir(parents=True)
    (project_dir / "foundry.toml").write_text(
        '[profile.default]\nsrc = "src"\nout = "out"\nlibs = ["lib"]\nsolc_version = "0.8.19"\n'
    )
    (project_dir / "src" / f"{contract_name}.sol").write_text(source_code)
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": "0x1111111111111111111111111111111111111111",
                "contract_name": contract_name,
                "compiler_version": "v0.8.19+commit.7dd6d404",
            }
        )
        + "\n"
    )
    (project_dir / "slither_results.json").write_text(json.dumps({"results": {"detectors": []}}) + "\n")
    return project_dir


def _fixture_source(relative_path: str) -> str:
    return (FIXTURES_DIR / relative_path).read_text()


def _semantic_function(payload: dict, signature: str) -> dict:
    for function in payload["functions"]:
        if function["function"] == signature:
            return function
    raise AssertionError(f"Semantic guard entry {signature} not found")


def test_semantic_guards_direct_owner_pause(tmp_path):
    project_dir = _write_project(tmp_path, "DirectOwnerPause", _fixture_source("pause/direct_owner_pause.sol"))
    analysis = collect_contract_analysis(project_dir)

    payload = build_semantic_guards(analysis)
    pause = _semantic_function(payload, "pause()")

    assert pause["status"] == "resolved"
    assert pause["predicates"] == [
        {
            "kind": "caller_equals_controller",
            "controller_kind": "state_variable",
            "controller_label": "owner",
            "controller_source": "owner",
            "read_spec": None,
        }
    ]


def test_semantic_guards_external_role_guard(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "ExternalRolePause",
        """
        pragma solidity ^0.8.19;

        interface IRoleRegistry {
            function hasRole(bytes32 role, address account) external view returns (bool);
            function PROTOCOL_PAUSER() external view returns (bytes32);
        }

        contract ExternalRolePause {
            IRoleRegistry public roleRegistry;
            bool public paused;

            constructor(IRoleRegistry registry) {
                roleRegistry = registry;
            }

            function pauseContract() external {
                require(roleRegistry.hasRole(roleRegistry.PROTOCOL_PAUSER(), msg.sender), "bad role");
                paused = true;
            }
        }
        """,
    )
    analysis = collect_contract_analysis(project_dir)

    payload = build_semantic_guards(analysis)
    pause = _semantic_function(payload, "pauseContract()")

    assert pause["status"] == "resolved"
    assert {
        (predicate["kind"], predicate.get("role_source"), predicate.get("authority_source"))
        for predicate in pause["predicates"]
    } == {("role_member", "PROTOCOL_PAUSER", "roleRegistry")}


def test_semantic_guards_external_helper_remains_partial(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "RoleRegistryUpgradeTarget",
        """
        pragma solidity ^0.8.19;

        contract RoleRegistryLike {
            function onlyProtocolUpgrader(address) external view {}
        }

        contract RoleRegistryUpgradeTarget {
            RoleRegistryLike public roleRegistry;

            constructor(RoleRegistryLike registry) {
                roleRegistry = registry;
            }

            function upgradeTo(address newImplementation) external {
                roleRegistry.onlyProtocolUpgrader(msg.sender);
                (bool ok,) = newImplementation.delegatecall("");
                require(ok, "delegatecall failed");
            }
        }
        """,
    )
    analysis = collect_contract_analysis(project_dir)

    payload = build_semantic_guards(analysis)
    upgrade = _semantic_function(payload, "upgradeTo(address)")

    assert upgrade["status"] == "partial"
    assert upgrade["predicates"] == [
        {
            "kind": "external_helper",
            "authority_source": ["roleRegistry"],
            "helper": "onlyProtocolUpgrader",
            "status": "unresolved",
        }
    ]


def test_semantic_guards_external_policy_helper_is_canonicalized(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OpaqueExternalPolicyGuard",
        """
        pragma solidity ^0.8.19;

        interface IPolicy {
            function q(address who, address target, bytes4 sig) external view;
        }

        contract OpaqueExternalPolicyGuard {
            IPolicy public policy;
            bool public executed;

            constructor(IPolicy policy_) {
                policy = policy_;
            }

            function execute() external {
                policy.q(msg.sender, address(this), this.execute.selector);
                executed = true;
            }
        }
        """,
    )
    analysis = collect_contract_analysis(project_dir)

    payload = build_semantic_guards(analysis)
    execute = _semantic_function(payload, "execute()")

    assert execute["status"] == "partial"
    assert execute["predicates"] == [
        {
            "kind": "policy_check",
            "authority_source": ["policy"],
            "helper": "q",
            "status": "unresolved",
        }
    ]
