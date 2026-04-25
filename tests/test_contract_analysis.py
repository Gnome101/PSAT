import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.contract_analysis import (
    ContractAnalysis,
    ControllerTrackingTarget,
    GuardRecord,
    PolicyTrackingTarget,
    PrivilegedFunction,
    SinkRecord,
)
from services.static import collect_contract_analysis

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "contracts"
FIXTURE_INDEX_PATH = FIXTURES_DIR / "index.json"


def _write_project(tmp_path: Path, contract_name: str, source_code: str, slither_output: dict | None = None) -> Path:
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
    (project_dir / "slither_results.json").write_text(
        json.dumps(
            slither_output
            or {
                "results": {
                    "detectors": [
                        {
                            "check": "reentrancy-events",
                            "impact": "Medium",
                            "confidence": "Medium",
                            "description": "Sample finding",
                        }
                    ]
                }
            }
        )
        + "\n"
    )
    return project_dir


def _fixture_source(relative_path: str) -> str:
    return (FIXTURES_DIR / relative_path).read_text()


def _fixture_index() -> list[dict]:
    return json.loads(FIXTURE_INDEX_PATH.read_text())["fixtures"]


def _privileged_function(analysis: ContractAnalysis, signature: str) -> PrivilegedFunction:
    for function in analysis["access_control"]["privileged_functions"]:
        if function["function"] == signature:
            return function
    raise AssertionError(f"Privileged function {signature} not found")


def _sink(analysis: ContractAnalysis, function_signature: str, target: str) -> SinkRecord:
    for sink in analysis["permission_graph"]["sinks"]:
        if sink["function"] == function_signature and sink["target"] == target:
            return sink
    raise AssertionError(f"Sink for {function_signature} -> {target} not found")


def _guards_for_sink(analysis: ContractAnalysis, function_signature: str, target: str) -> list[GuardRecord]:
    sink = _sink(analysis, function_signature, target)
    guard_ids = set(sink["guarded_by"])
    return [guard for guard in analysis["permission_graph"]["guards"] if guard["id"] in guard_ids]


def _controller_labels(analysis: ContractAnalysis, guard: GuardRecord) -> set[str]:
    controller_ids = set(guard["controller_ids"])
    return {
        controller["label"]
        for controller in analysis["permission_graph"]["controllers"]
        if controller["id"] in controller_ids
    }


def _tracked_controller(analysis: ContractAnalysis, label: str) -> ControllerTrackingTarget:
    for controller in analysis["controller_tracking"]:
        if controller["label"] == label:
            return controller
    raise AssertionError(f"Tracked controller {label} not found")


def _tracked_policy(analysis: ContractAnalysis, label: str) -> PolicyTrackingTarget:
    for policy in analysis["policy_tracking"]:
        if policy["label"] == label:
            return policy
    raise AssertionError(f"Tracked policy {label} not found")


def test_fixture_index_covers_all_solidity_contract_fixtures():
    indexed_paths = {entry["path"] for entry in _fixture_index()}
    fixture_paths = {str(path.relative_to(FIXTURES_DIR)) for path in FIXTURES_DIR.rglob("*.sol")}

    assert indexed_paths == fixture_paths

    for entry in _fixture_index():
        assert entry["category"]
        assert entry["contract_name"]
        assert entry["description"]
        assert entry["detection_patterns"]
        assert (FIXTURES_DIR / entry["path"]).exists()


def test_collect_contract_analysis_detects_erc20_ownable_and_pausable(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "Token",
        _fixture_source("token/token_erc20_ownable_pausable.sol"),
    )

    analysis = collect_contract_analysis(project_dir)

    assert analysis["subject"]["name"] == "Token"
    assert analysis["summary"]["control_model"] == "ownable"
    assert analysis["summary"]["is_pausable"] is True
    assert analysis["summary"]["static_risk_level"] == "medium"
    assert "ERC20" in analysis["contract_classification"]["standards"]
    assert analysis["contract_classification"]["is_nft"] is False
    assert analysis["access_control"]["owner_variables"] == ["owner"]
    assert "paused" in analysis["pausability"]["pause_variables"]
    privileged = {item["function"] for item in analysis["access_control"]["privileged_functions"]}
    assert "pause()" in privileged
    assert "mint(address,uint256)" in privileged
    assert "approve(address,uint256)" not in privileged
    assert "transfer(address,uint256)" not in privileged
    assert not any(signature.startswith("constructor(") for signature in privileged)


def test_collect_contract_analysis_detects_upgradeability_timelock_and_factory(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "UpgradeFactory",
        _fixture_source("composed/upgrade_factory_uups.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    assert analysis["summary"]["is_upgradeable"] is True
    assert analysis["upgradeability"]["pattern"] == "uups"
    assert analysis["timelock"]["has_timelock"] is True
    assert analysis["timelock"]["pattern"] == "custom"
    assert analysis["contract_classification"]["is_factory"] is True
    assert "createChild()" in analysis["contract_classification"]["factory_functions"]
    assert "eip1967.proxy.implementation" in analysis["upgradeability"]["implementation_slots"]
    create_child = _privileged_function(analysis, "createChild()")
    assert "createChild():node1:contract_creation:Child" in create_child["sink_ids"]
    assert "caller_equals_storage" in create_child["guard_kinds"]
    assert "owner" in create_child["controller_refs"]


def test_collect_contract_analysis_detects_erc721_as_nft(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "Collectible",
        _fixture_source("nft/collectible_erc721.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    assert "ERC721" in analysis["contract_classification"]["standards"]
    assert analysis["contract_classification"]["is_nft"] is True
    assert analysis["summary"]["is_nft"] is True


@pytest.mark.parametrize(
    ("contract_name", "fixture_name", "signature", "target", "guard_kind", "controller_label"),
    [
        (
            "DirectOwnerPause",
            "pause/direct_owner_pause.sol",
            "pause()",
            "paused",
            "caller_equals_storage",
            "owner",
        ),
        (
            "MappingPause",
            "pause/mapping_pause.sol",
            "pause()",
            "paused",
            "caller_in_mapping",
            "wards",
        ),
        (
            "HelperPause",
            "pause/helper_pause.sol",
            "pause()",
            "paused",
            "caller_equals_storage",
            "owner",
        ),
        (
            "AuthorityPause",
            "pause/authority_pause.sol",
            "pause()",
            "paused",
            "external_authority_check",
            "authority",
        ),
        (
            "RolePause",
            "pause/role_pause.sol",
            "pause()",
            "paused",
            "role_membership_check",
            "PAUSER_ROLE",
        ),
        (
            "DirectAdminUpgrade",
            "upgrade/direct_admin_upgrade.sol",
            "upgradeTo(address)",
            "implementation",
            "caller_equals_storage",
            "admin",
        ),
    ],
)
def test_permission_graph_detects_structural_guards(
    tmp_path, contract_name, fixture_name, signature, target, guard_kind, controller_label
):
    project_dir = _write_project(
        tmp_path,
        contract_name,
        _fixture_source(fixture_name),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    guards = _guards_for_sink(analysis, signature, target)

    assert any(guard["kind"] == guard_kind for guard in guards)
    assert any(controller_label in _controller_labels(analysis, guard) for guard in guards)


def test_permission_graph_recovers_library_helper_controllers(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "HelperGovernance",
        """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

library GovState {
    error CallerIsNotGovernance(address caller);
    error CallerIsNotAdminExecutor(address caller);

    struct Context {
        address governance;
        address adminExecutor;
    }

    function setGovernance(Context storage self, address newGovernance) internal {
        self.governance = newGovernance;
    }

    function setAdminExecutor(Context storage self, address newAdminExecutor) internal {
        self.adminExecutor = newAdminExecutor;
    }

    function checkCallerIsGovernance(Context storage self) internal view {
        if (self.governance != msg.sender) {
            revert CallerIsNotGovernance(msg.sender);
        }
    }

    function checkCallerIsAdminExecutor(Context storage self) internal view {
        if (self.adminExecutor != msg.sender) {
            revert CallerIsNotAdminExecutor(msg.sender);
        }
    }
}

contract HelperGovernance {
    using GovState for GovState.Context;

    GovState.Context private _state;
    bool public paused;

    constructor(address governance_, address adminExecutor_) {
        _state.setGovernance(governance_);
        _state.setAdminExecutor(adminExecutor_);
    }

    function getGovernance() external view returns (address) {
        return _state.governance;
    }

    function getAdminExecutor() external view returns (address) {
        return _state.adminExecutor;
    }

    function pause() external {
        _state.checkCallerIsGovernance();
        paused = true;
    }

    function unpause() external {
        _state.checkCallerIsAdminExecutor();
        paused = false;
    }
}
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    pause = _privileged_function(analysis, "pause()")
    unpause = _privileged_function(analysis, "unpause()")
    assert "governance" in pause["controller_refs"]
    assert "adminExecutor" in unpause["controller_refs"]
    assert "caller_via_helper_function" in pause["guard_kinds"]
    assert "caller_via_helper_function" in unpause["guard_kinds"]

    controllers = {controller["source"]: controller for controller in analysis["permission_graph"]["controllers"]}
    assert controllers["governance"]["read_spec"] == {"strategy": "getter_call", "target": "getGovernance"}
    assert controllers["adminExecutor"]["read_spec"] == {"strategy": "getter_call", "target": "getAdminExecutor"}

    tracked_governance = _tracked_controller(analysis, "governance")
    tracked_admin = _tracked_controller(analysis, "adminExecutor")
    assert tracked_governance["read_spec"] == {"strategy": "getter_call", "target": "getGovernance"}
    assert tracked_admin["read_spec"] == {"strategy": "getter_call", "target": "getAdminExecutor"}


def test_controller_tracking_synthesizes_semantic_owner_when_graph_misses_modifier_body(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "ExecutorLike",
        """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

contract OwnableLike {
    address internal _owner;

    modifier onlyOwner() {
        require(msg.sender == _owner, "only owner");
        _;
    }

    constructor(address owner_) {
        _owner = owner_;
    }

    function owner() external view returns (address) {
        return _owner;
    }
}

contract ExecutorLike is OwnableLike {
    constructor(address owner_) OwnableLike(owner_) {}

    function execute(address target, bytes calldata data) external onlyOwner {
        (bool ok,) = target.call(data);
        require(ok, "exec failed");
    }
}
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    execute = _privileged_function(analysis, "execute(address,bytes)")
    assert "owner" in execute["controller_refs"]
    tracked_owner = _tracked_controller(analysis, "owner")
    assert tracked_owner["controller_id"] == "state_variable:owner"
    assert tracked_owner["tracking_mode"] == "state_only"


def test_permission_graph_requires_guard_to_dominate_sink(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "NonDominatingOwnerPause",
        _fixture_source("pause/non_dominating_owner_pause.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    sink = _sink(analysis, "pause(bool)", "paused")

    assert sink["guarded_by"] == []


def test_collect_contract_analysis_builds_can_call_policy_tracking(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "RolesAuthorityPolicy",
        _fixture_source("tracking/roles_authority_policy.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    policy = _tracked_policy(analysis, "canCall policy")

    assert policy["policy_function"] == "canCall(address,address,bytes4)"
    assert policy["tracked_state_targets"] == [
        "getRolesWithCapability",
        "getUserRoles",
        "isCapabilityPublic",
    ]
    assert {item["function"] for item in policy["writer_functions"]} == {
        "setPublicCapability(address,bytes4,bool)",
        "setRoleCapability(uint8,address,bytes4,bool)",
        "setUserRole(address,uint8,bool)",
    }
    assert {item["name"] for item in policy["associated_events"]} == {
        "PublicCapabilityUpdated",
        "RoleCapabilityUpdated",
        "UserRoleUpdated",
    }

    role_capability = next(item for item in policy["associated_events"] if item["name"] == "RoleCapabilityUpdated")
    assert role_capability["signature"] == "RoleCapabilityUpdated(uint8,address,bytes4,bool)"
    assert role_capability["inputs"] == [
        {"name": "role", "type": "uint8", "indexed": True},
        {"name": "target", "type": "address", "indexed": True},
        {"name": "functionSig", "type": "bytes4", "indexed": True},
        {"name": "enabled", "type": "bool", "indexed": False},
    ]


def test_permission_graph_tracks_state_write_in_internal_helper(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "IndirectOwnerPause",
        _fixture_source("pause/indirect_owner_pause.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    sink = _sink(analysis, "pause()", "paused")

    assert sink["kind"] == "state_write"
    assert "_setPaused@" in sink["id"]
    guards = _guards_for_sink(analysis, "pause()", "paused")
    assert any(guard["kind"] == "caller_equals_storage" for guard in guards)

    privileged = _privileged_function(analysis, "pause()")
    assert "owner" in privileged["controller_refs"]
    assert any(sink_id.endswith(":state_write:paused") for sink_id in privileged["sink_ids"])


def test_permission_graph_detects_contract_creation_sink(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "UpgradeFactory",
        _fixture_source("composed/upgrade_factory_uups.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    sink = _sink(analysis, "createChild()", "Child")

    assert sink["kind"] == "contract_creation"
    assert sink["effects"] == ["factory_deployment"]
    guards = _guards_for_sink(analysis, "createChild()", "Child")
    assert any(guard["kind"] == "caller_equals_storage" for guard in guards)
    privileged = _privileged_function(analysis, "createChild()")
    assert privileged["effect_labels"] == ["contract_deployment"]
    assert privileged["action_summary"] == "Deploys a new contract instance."


@pytest.mark.parametrize(
    ("contract_name", "fixture_name", "signature", "target", "sink_kind", "effect"),
    [
        (
            "ExternalCallControl",
            "calls/external_call_control.sol",
            "pingTarget(uint256)",
            "target.ping",
            "external_call",
            "privileged_external_call",
        ),
        (
            "DelegateCallControl",
            "calls/delegatecall_control.sol",
            "execute(bytes)",
            "implementation",
            "delegatecall",
            "delegatecall_control",
        ),
        (
            "SelfDestructControl",
            "calls/selfdestruct_control.sol",
            "destroy()",
            "selfdestruct",
            "selfdestruct",
            "selfdestruct_capability",
        ),
    ],
)
def test_permission_graph_detects_additional_permissioned_sink_kinds(
    tmp_path, contract_name, fixture_name, signature, target, sink_kind, effect
):
    project_dir = _write_project(
        tmp_path,
        contract_name,
        _fixture_source(fixture_name),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    sink = _sink(analysis, signature, target)

    assert sink["kind"] == sink_kind
    assert effect in sink["effects"]
    guards = _guards_for_sink(analysis, signature, target)
    assert any(guard["kind"] == "caller_equals_storage" for guard in guards)

    privileged = _privileged_function(analysis, signature)
    assert any(sink_id.endswith(f":{sink_kind}:{target}") for sink_id in privileged["sink_ids"])
    assert "caller_equals_storage" in privileged["guard_kinds"]
    assert "owner" in privileged["controller_refs"]


def test_permission_graph_tracks_external_call_in_internal_helper(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "IndirectExternalCallControl",
        _fixture_source("calls/indirect_external_call_control.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    sink = _sink(analysis, "pingTarget(uint256)", "target.ping")

    assert sink["kind"] == "external_call"
    assert "_ping@" in sink["id"]
    guards = _guards_for_sink(analysis, "pingTarget(uint256)", "target.ping")
    assert any(guard["kind"] == "caller_equals_storage" for guard in guards)

    privileged = _privileged_function(analysis, "pingTarget(uint256)")
    assert "owner" in privileged["controller_refs"]
    assert any(sink_id.endswith(":external_call:target.ping") for sink_id in privileged["sink_ids"])


def test_permission_graph_recovers_modifier_helper_auth_structure(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "AuthModifierController",
        _fixture_source("composed/auth_modifier_controller.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    for signature, target in (
        ("setHook(address)", "hook"),
        ("manage(PingTarget,uint256)", "target.ping"),
        ("transferOwnership(address)", "owner"),
    ):
        privileged = _privileged_function(analysis, signature)
        assert {"caller_equals_storage", "external_authority_check"}.issubset(set(privileged["guard_kinds"]))
        assert {"owner", "authority"}.issubset(set(privileged["controller_refs"]))

        guards = _guards_for_sink(analysis, signature, target)
        assert any(guard["kind"] == "caller_equals_storage" for guard in guards)
        assert any(guard["kind"] == "external_authority_check" for guard in guards)

    assert not any(
        sink["target"] == "auth.canCall" and sink["function"] == "manage(PingTarget,uint256)"
        for sink in analysis["permission_graph"]["sinks"]
    )
    privileged_signatures = {item["function"] for item in analysis["access_control"]["privileged_functions"]}
    assert not any(signature.startswith("constructor(") for signature in privileged_signatures)

    owner_tracking = _tracked_controller(analysis, "owner")
    assert owner_tracking["tracking_mode"] == "event_plus_state"
    assert owner_tracking["associated_events"] == [
        {
            "name": "OwnershipTransferred",
            "signature": "OwnershipTransferred(address,address)",
            "topic0": "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
            "inputs": [
                {"name": "user", "type": "address", "indexed": True},
                {"name": "newOwner", "type": "address", "indexed": True},
            ],
        }
    ]
    assert {writer["function"] for writer in owner_tracking["writer_functions"]} == {"transferOwnership(address)"}

    authority_tracking = _tracked_controller(analysis, "authority")
    assert authority_tracking["tracking_mode"] == "event_plus_state"
    assert authority_tracking["associated_events"] == [
        {
            "name": "AuthorityUpdated",
            "signature": "AuthorityUpdated(address,address)",
            "topic0": "0xa3396fd7f6e0a21b50e5089d2da70d5ac0a3bbbd1f617a93f134b76389980198",
            "inputs": [
                {"name": "user", "type": "address", "indexed": True},
                {"name": "newAuthority", "type": "address", "indexed": True},
            ],
        }
    ]
    assert {writer["function"] for writer in authority_tracking["writer_functions"]} == {"setAuthority(AuthorityLike)"}

    manage = _privileged_function(analysis, "manage(PingTarget,uint256)")
    assert manage["effect_labels"] == ["external_contract_call"]
    assert manage["effect_targets"] == ["target.ping"]
    assert manage["action_summary"] == "Calls an external contract from the contract context."

    set_hook = _privileged_function(analysis, "setHook(address)")
    assert set_hook["effect_labels"] == ["hook_update"]
    assert set_hook["effect_targets"] == ["hook"]
    assert set_hook["action_summary"] == "Updates hook configuration that can affect later contract behavior."

    transfer_ownership = _privileged_function(analysis, "transferOwnership(address)")
    assert transfer_ownership["effect_labels"] == ["ownership_transfer"]
    assert transfer_ownership["action_summary"] == "Transfers contract ownership."


def test_privileged_function_semantics_detect_pause_and_asset_flow(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "Token",
        _fixture_source("token/token_erc20_ownable_pausable.sol"),
    )

    analysis = collect_contract_analysis(project_dir)

    pause = _privileged_function(analysis, "pause()")
    assert "pause_toggle" in pause["effect_labels"]
    assert pause["action_summary"] == "Changes the contract pause state."


def test_controller_tracking_falls_back_to_state_only_without_events(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OwnerNoEvent",
        _fixture_source("tracking/owner_update_no_event.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    owner_tracking = _tracked_controller(analysis, "owner")
    assert owner_tracking["tracking_mode"] == "state_only"
    assert owner_tracking["associated_events"] == []
    assert {writer["function"] for writer in owner_tracking["writer_functions"]} == {"transferOwnership(address)"}


def test_permission_graph_ignores_non_authority_external_calls_with_caller_args(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "NonAuthorityExternalCallGuard",
        """
        pragma solidity ^0.8.19;

        interface TokenLike {
            function balanceOf(address account) external view returns (uint256);
        }

        interface PingTarget {
            function ping(uint256 value) external;
        }

        contract NonAuthorityExternalCallGuard {
            address public owner;
            TokenLike public token;

            constructor(TokenLike token_) {
                owner = msg.sender;
                token = token_;
            }

            function manage(PingTarget target, uint256 value) external {
                require(msg.sender == owner, "not owner");
                require(token.balanceOf(msg.sender) > 0, "no balance");
                target.ping(value);
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "manage(PingTarget,uint256)")

    assert "owner" in privileged["controller_refs"]
    assert "token" not in privileged["controller_refs"]
    assert "external_authority_check" not in privileged["guard_kinds"]

    guards = _guards_for_sink(analysis, "manage(PingTarget,uint256)", "target.ping")
    assert all(guard["kind"] != "external_authority_check" for guard in guards)


def test_controller_tracking_preserves_explicit_auth_state_variable_names(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "ExplicitTimelockGuard",
        """
        pragma solidity ^0.8.19;

        interface TimelockLike {
            function getGovernance() external view returns (address);
        }

        contract ExplicitTimelockGuard {
            TimelockLike public EMERGENCY_PROTECTED_TIMELOCK;

            constructor(TimelockLike timelock_) {
                EMERGENCY_PROTECTED_TIMELOCK = timelock_;
            }

            function _checkCallerIsGovernance() internal view {
                require(msg.sender == EMERGENCY_PROTECTED_TIMELOCK.getGovernance(), "not governance");
            }

            function reseal() external {
                _checkCallerIsGovernance();
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "reseal()")

    assert "EMERGENCY_PROTECTED_TIMELOCK" in privileged["controller_refs"]
    assert "timelock" in privileged["controller_refs"]

    tracked = {item["label"]: item for item in analysis["controller_tracking"]}
    assert "EMERGENCY_PROTECTED_TIMELOCK" in tracked
    assert tracked["EMERGENCY_PROTECTED_TIMELOCK"]["source"] == "EMERGENCY_PROTECTED_TIMELOCK"


def test_controller_tracking_preserves_role_registry_on_uups_upgrade_auth(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "RoleRegistryUpgrade",
        """
        pragma solidity ^0.8.19;

        contract RoleRegistryLike {
            function onlyProtocolUpgrader(address) external view {}
        }

        abstract contract UUPSUpgradeable {
            function upgradeTo(address newImplementation) external virtual {
                _authorizeUpgrade(newImplementation);
            }

            function upgradeToAndCall(address newImplementation, bytes memory data) external payable virtual {
                _authorizeUpgrade(newImplementation);
                data;
            }

            function _authorizeUpgrade(address) internal virtual;
        }

        contract RoleRegistryUpgrade is UUPSUpgradeable {
            RoleRegistryLike public roleRegistry;

            constructor(RoleRegistryLike registry) {
                roleRegistry = registry;
            }

            function _authorizeUpgrade(address) internal override {
                roleRegistry.onlyProtocolUpgrader(msg.sender);
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    for signature in ("upgradeTo(address)", "upgradeToAndCall(address,bytes)"):
        privileged = _privileged_function(analysis, signature)
        assert "roleRegistry" in privileged["guards"]
        assert "roleRegistry" in privileged["controller_refs"]


def test_effect_target_role_registry_upgrader_preserves_controller_ref(tmp_path):
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
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "upgradeTo(address)")

    assert "roleRegistry" in privileged["controller_refs"]


def test_external_has_role_constant_is_tracked_as_role_identifier(tmp_path):
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
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "pauseContract()")

    assert "PROTOCOL_PAUSER" in privileged["guards"]
    assert "PROTOCOL_PAUSER" in privileged["controller_refs"]
    assert "pauser" in privileged["controller_refs"]
    assert "roleRegistry" in privileged["controller_refs"]

    tracked = _tracked_controller(analysis, "PROTOCOL_PAUSER")
    assert tracked["controller_id"] == "role_identifier:PROTOCOL_PAUSER"
    assert tracked["kind"] == "role_identifier"


def test_external_has_role_constant_without_authish_name_is_tracked_as_role_identifier(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OpaqueRolePause",
        """
        pragma solidity ^0.8.19;

        interface IRoleRegistry {
            function hasRole(bytes32 role, address account) external view returns (bool);
            function BREAK_GLASS() external view returns (bytes32);
        }

        contract OpaqueRolePause {
            IRoleRegistry public roleRegistry;
            bool public paused;

            constructor(IRoleRegistry registry) {
                roleRegistry = registry;
            }

            function pauseContract() external {
                require(roleRegistry.hasRole(roleRegistry.BREAK_GLASS(), msg.sender), "bad role");
                paused = true;
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "pauseContract()")

    # The robust behavior we want is to preserve the exact role identifier,
    # even when the constant name doesn't look auth-ish.
    assert "BREAK_GLASS" in privileged["guards"]
    assert "BREAK_GLASS" in privileged["controller_refs"]

    tracked = _tracked_controller(analysis, "BREAK_GLASS")
    assert tracked["controller_id"] == "role_identifier:BREAK_GLASS"
    assert tracked["kind"] == "role_identifier"
    assert tracked["read_spec"] == {
        "strategy": "getter_call",
        "target": "BREAK_GLASS",
        "contract_source": "roleRegistry",
    }


def test_modifier_helper_preserves_opaque_role_identifier(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OpaqueRoleModifierPause",
        """
        pragma solidity ^0.8.19;

        interface IAuth {
            function q(bytes32 role, address who) external view returns (bool);
        }

        contract OpaqueRoleModifierPause {
            bytes32 public constant BREAK_GLASS = keccak256("x");
            IAuth public auth;
            bool public paused;

            constructor(IAuth auth_) {
                auth = auth_;
            }

            modifier gate(bytes32 x) {
                _check(x);
                _;
            }

            function _check(bytes32 x) internal view {
                require(auth.q(x, msg.sender), "bad");
            }

            function pause() external gate(BREAK_GLASS) {
                paused = true;
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "pause()")

    assert "BREAK_GLASS" in privileged["guards"]
    assert "BREAK_GLASS" in privileged["controller_refs"]

    tracked = _tracked_controller(analysis, "BREAK_GLASS")
    assert tracked["controller_id"] == "role_identifier:BREAK_GLASS"
    assert tracked["kind"] == "role_identifier"


def test_opaque_external_void_helper_guard_would_need_semantic_execution(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OpaqueExternalGuard",
        """
        pragma solidity ^0.8.19;

        interface IGate {
            function x(address who) external view;
        }

        contract OpaqueGate is IGate {
            address public owner;
            error Denied();

            constructor(address owner_) {
                owner = owner_;
            }

            function x(address who) external view {
                if (who != owner) revert Denied();
            }
        }

        contract OpaqueExternalGuard {
            IGate public gate;
            bool public paused;

            constructor(IGate gate_) {
                gate = gate_;
            }

            function pause() external {
                gate.x(msg.sender);
                paused = true;
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "pause()")

    # Desired semantic outcome:
    #   gate.x(msg.sender) -> inside OpaqueGate.x(): require(who == owner)
    # so the direct caller set should reduce to owner(gate).
    assert "external_authority_check" in privileged["guard_kinds"]
    assert "gate" in privileged["controller_refs"]


def test_opaque_external_role_helper_would_need_semantic_execution(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OpaqueExternalRoleGuard",
        """
        pragma solidity ^0.8.19;

        interface IAuth {
            function z(address who) external view;
        }

        contract OpaqueRoleAuth is IAuth {
            bytes32 public constant BREAK_GLASS = keccak256("x");
            mapping(bytes32 => mapping(address => bool)) internal roles;
            error Denied();

            constructor(address pauser) {
                roles[BREAK_GLASS][pauser] = true;
            }

            function z(address who) external view {
                if (!roles[BREAK_GLASS][who]) revert Denied();
            }
        }

        contract OpaqueExternalRoleGuard {
            IAuth public auth;
            bool public paused;

            constructor(IAuth auth_) {
                auth = auth_;
            }

            function pause() external {
                auth.z(msg.sender);
                paused = true;
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "pause()")

    # Desired semantic outcome:
    #   auth.z(msg.sender) -> inside OpaqueRoleAuth.z(): caller must be a member
    #   of BREAK_GLASS on auth.
    assert "external_authority_check" in privileged["guard_kinds"]
    assert "auth" in privileged["controller_refs"]
    assert "BREAK_GLASS" in privileged["controller_refs"]


def test_opaque_external_policy_helper_would_need_semantic_execution(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "OpaqueExternalPolicyGuard",
        """
        pragma solidity ^0.8.19;

        interface IPolicy {
            function q(address who, address target, bytes4 sig) external view;
        }

        contract OpaquePolicy is IPolicy {
            mapping(address => mapping(bytes4 => mapping(address => bool))) internal can;
            error Denied();

            constructor(address admin) {
                can[address(this)][this.guard.selector][admin] = true;
            }

            function guard() external {}

            function q(address who, address target, bytes4 sig) external view {
                if (!can[target][sig][who]) revert Denied();
            }
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
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, "execute()")

    # Desired semantic outcome:
    #   policy.q(msg.sender, address(this), this.execute.selector)
    #   should reduce to a canonical policy check predicate.
    assert "external_authority_check" in privileged["guard_kinds"]
    assert "policy" in privileged["controller_refs"]


@pytest.mark.parametrize(
    ("contract_name", "fixture_name", "signature", "expected_guards"),
    [
        (
            "DirectOwnerPause",
            "pause/direct_owner_pause.sol",
            "pause()",
            {"owner"},
        ),
        (
            "MappingPause",
            "pause/mapping_pause.sol",
            "pause()",
            {"wards"},
        ),
        (
            "HelperPause",
            "pause/helper_pause.sol",
            "pause()",
            {"owner", "_checkAuth"},
        ),
        (
            "RolePause",
            "pause/role_pause.sol",
            "pause()",
            {"PAUSER_ROLE", "onlyRole", "role"},
        ),
        (
            "AuthorityPause",
            "pause/authority_pause.sol",
            "pause()",
            {"authority", "authority.canCall"},
        ),
        (
            "TimelockPause",
            "pause/timelock_pause.sol",
            "pause()",
            {"timelock"},
        ),
    ],
)
def test_collect_contract_analysis_detects_pause_control_patterns(
    tmp_path, contract_name, fixture_name, signature, expected_guards
):
    project_dir = _write_project(
        tmp_path,
        contract_name,
        _fixture_source(fixture_name),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, signature)

    assert expected_guards.issubset(set(privileged["guards"]))


@pytest.mark.parametrize(
    ("contract_name", "fixture_name", "signature", "expected_guards", "expected_pattern"),
    [
        (
            "DirectAdminUpgrade",
            "upgrade/direct_admin_upgrade.sol",
            "upgradeTo(address)",
            {"admin"},
            "custom",
        ),
        (
            "UUPSOwnableUpgrade",
            "upgrade/uups_ownable_upgrade.sol",
            "upgradeTo(address)",
            {"onlyOwner", "owner"},
            "uups",
        ),
        (
            "TransparentUpgradeableProxy",
            "upgrade/transparent_upgradeable_proxy.sol",
            "upgradeTo(address)",
            {"admin"},
            "transparent",
        ),
        (
            "UpgradeableBeacon",
            "upgrade/upgradeable_beacon.sol",
            "upgradeTo(address)",
            {"onlyOwner", "owner"},
            "beacon",
        ),
    ],
)
def test_collect_contract_analysis_detects_upgrade_control_patterns(
    tmp_path, contract_name, fixture_name, signature, expected_guards, expected_pattern
):
    project_dir = _write_project(
        tmp_path,
        contract_name,
        _fixture_source(fixture_name),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    privileged = _privileged_function(analysis, signature)

    assert expected_guards.issubset(set(privileged["guards"]))
    assert analysis["upgradeability"]["pattern"] == expected_pattern
