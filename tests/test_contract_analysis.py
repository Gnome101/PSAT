import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.contract_analysis import (
    ContractAnalysis,
    ControllerTrackingTarget,
    SemanticFunctionSummary,
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


def _semantic_function(analysis: ContractAnalysis, signature: str) -> SemanticFunctionSummary:
    for function in analysis["semantic_control"]["semantic_functions"]:
        if function["function"] == signature:
            return function
    raise AssertionError(f"Semantic function {signature} not found")


def _tracked_controller(analysis: ContractAnalysis, label: str) -> ControllerTrackingTarget:
    for controller in analysis["controller_tracking"]:
        if controller["label"] == label:
            return controller
    raise AssertionError(f"Tracked controller {label} not found")


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


def test_collect_contract_analysis_with_artifacts_returns_semantic_artifacts(tmp_path):
    """The worker-facing entrypoint
    ``collect_contract_analysis_with_artifacts`` returns the semantic
    ``predicate_trees`` and ``effects`` artifacts alongside the
    analysis dict, off a single Slither parse."""
    from services.static.contract_analysis_pipeline import collect_contract_analysis_with_artifacts

    project_dir = _write_project(
        tmp_path,
        "Token",
        _fixture_source("token/token_erc20_ownable_pausable.sol"),
    )

    analysis, predicate_trees, effects = collect_contract_analysis_with_artifacts(project_dir)

    assert analysis["schema_version"] == "0.1"
    assert predicate_trees is not None
    assert predicate_trees.get("schema_version") == "semantic"
    # Successful emit produces a `trees` dict; an error path would
    # set `error` instead.
    assert "trees" in predicate_trees or "error" in predicate_trees
    assert effects is not None
    assert effects.get("schema_version") == "semantic"
    assert "functions" in effects or "error" in effects


def test_collect_contract_analysis_uses_semantic_factory_without_upgrade_timelock_name_guessing(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "UpgradeFactory",
        _fixture_source("composed/upgrade_factory_uups.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    assert analysis["summary"]["is_upgradeable"] is False
    assert analysis["upgradeability"]["pattern"] == "none"
    assert analysis["timelock"]["has_timelock"] is False
    assert analysis["timelock"]["pattern"] == "none"
    assert analysis["contract_classification"]["is_factory"] is True
    assert "createChild()" in analysis["contract_classification"]["factory_functions"]
    assert analysis["upgradeability"]["implementation_slots"] == []
    create_child = _semantic_function(analysis, "createChild()")
    # sink_ids come from the semantic effects artifact and end with
    # ``:<kind>:<target>``; the inner segment is the per-function sink index.
    assert any(sink_id.endswith(":contract_creation:Child") for sink_id in create_child["sink_ids"])
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


def test_state_write_in_internal_helper_surfaces_on_caller(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "IndirectOwnerPause",
        _fixture_source("pause/indirect_owner_pause.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    semantic = _semantic_function(analysis, "pause()")
    assert "owner" in semantic["controller_refs"]
    assert any(sink_id.endswith(":state_write:paused") for sink_id in semantic["sink_ids"])


def test_contract_creation_sink_classified(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "UpgradeFactory",
        _fixture_source("composed/upgrade_factory_uups.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    semantic = _semantic_function(analysis, "createChild()")
    assert semantic["effect_labels"] == ["contract_deployment"]
    assert semantic["action_summary"] == "Deploys a new contract instance."


@pytest.mark.parametrize(
    ("contract_name", "fixture_name", "signature", "target", "sink_kind"),
    [
        (
            "ExternalCallControl",
            "calls/external_call_control.sol",
            "pingTarget(uint256)",
            "target.ping",
            "external_call",
        ),
        (
            "DelegateCallControl",
            "calls/delegatecall_control.sol",
            "execute(bytes)",
            "implementation",
            "delegatecall",
        ),
        (
            "SelfDestructControl",
            "calls/selfdestruct_control.sol",
            "destroy()",
            "selfdestruct",
            "selfdestruct",
        ),
    ],
)
def test_additional_semantic_sink_kinds_surface_on_semantic_summary(
    tmp_path, contract_name, fixture_name, signature, target, sink_kind
):
    project_dir = _write_project(
        tmp_path,
        contract_name,
        _fixture_source(fixture_name),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    semantic = _semantic_function(analysis, signature)
    assert any(sink_id.endswith(f":{sink_kind}:{target}") for sink_id in semantic["sink_ids"])
    assert "owner" in semantic["controller_refs"]


def test_external_call_in_internal_helper_surfaces_on_caller(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "IndirectExternalCallControl",
        _fixture_source("calls/indirect_external_call_control.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    semantic = _semantic_function(analysis, "pingTarget(uint256)")
    assert "owner" in semantic["controller_refs"]
    assert any(sink_id.endswith(":external_call:target.ping") for sink_id in semantic["sink_ids"])


def test_modifier_helper_auth_structure_recovered(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "AuthModifierController",
        _fixture_source("composed/auth_modifier_controller.sol"),
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)

    for signature in ("setHook(address)", "manage(PingTarget,uint256)", "transferOwnership(address)"):
        semantic = _semantic_function(analysis, signature)
        assert {"owner", "authority"}.issubset(set(semantic["controller_refs"]))

    semantic_signatures = {item["function"] for item in analysis["semantic_control"]["semantic_functions"]}
    assert not any(sig.startswith("constructor(") for sig in semantic_signatures)

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

    manage = _semantic_function(analysis, "manage(PingTarget,uint256)")
    assert manage["effect_labels"] == ["external_contract_call"]
    # Semantic effects include both body and modifier-level external_call
    # sinks. ``target.ping`` is the body sink; ``auth.canCall`` comes from
    # the modifier ``isAuthorized`` body.
    assert "target.ping" in manage["effect_targets"]
    assert manage["action_summary"] == "Calls an external contract from the contract context."

    set_hook = _semantic_function(analysis, "setHook(address)")
    assert set_hook["effect_labels"] == ["hook_update"]
    # See manage() — modifier sinks now appear alongside body sinks.
    assert "hook" in set_hook["effect_targets"]
    assert set_hook["action_summary"] == "Updates hook configuration that can affect later contract behavior."

    transfer_ownership = _semantic_function(analysis, "transferOwnership(address)")
    assert transfer_ownership["effect_labels"] == ["ownership_transfer"]
    assert transfer_ownership["action_summary"] == "Transfers contract ownership."


def test_semantic_function_semantics_detect_pause_and_asset_flow(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "Token",
        _fixture_source("token/token_erc20_ownable_pausable.sol"),
    )

    analysis = collect_contract_analysis(project_dir)

    pause = _semantic_function(analysis, "pause()")
    assert "pause_toggle" in pause["effect_labels"]
    assert pause["action_summary"] == "Changes the contract pause state."


def test_layerzero_bridge_semantics_surface_as_multichain_effects(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "LayerZeroBridgeAdapter",
        """
        pragma solidity ^0.8.19;

        interface IERC20 {
            function transferFrom(address from, address to, uint256 amount) external returns (bool);
        }

        interface ILayerZeroEndpoint {
            function send(
                uint16 dstChainId,
                bytes calldata destination,
                bytes calldata payload,
                address payable refundAddress,
                address zroPaymentAddress,
                bytes calldata adapterParams
            ) external payable;
        }

        contract LayerZeroBridgeAdapter {
            address public owner;
            IERC20 public token;
            ILayerZeroEndpoint public endpoint;
            mapping(uint16 => bytes) public trustedRemoteLookup;
            mapping(uint64 => bytes32) public receivedPayloads;

            constructor(IERC20 token_, ILayerZeroEndpoint endpoint_) {
                owner = msg.sender;
                token = token_;
                endpoint = endpoint_;
            }

            function setTrustedRemote(uint16 srcChainId, bytes calldata path) external {
                require(msg.sender == owner, "not owner");
                trustedRemoteLookup[srcChainId] = path;
            }

            function sendFrom(
                address from,
                uint16 dstChainId,
                bytes calldata toAddress,
                uint256 amount,
                address payable refundAddress,
                address zroPaymentAddress,
                bytes calldata adapterParams
            ) external payable {
                token.transferFrom(from, address(this), amount);
                endpoint.send{value: msg.value}(
                    dstChainId,
                    toAddress,
                    abi.encode(from, amount),
                    refundAddress,
                    zroPaymentAddress,
                    adapterParams
                );
            }

            function lzReceive(
                uint16 srcChainId,
                bytes calldata srcAddress,
                uint64 nonce,
                bytes calldata payload
            ) external {
                require(msg.sender == address(endpoint), "bad endpoint");
                receivedPayloads[nonce] = keccak256(abi.encode(srcChainId, srcAddress, payload));
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    send_from = next(
        fn for fn in analysis["semantic_control"]["semantic_functions"] if fn["function"].startswith("sendFrom(")
    )
    set_remote = _semantic_function(analysis, "setTrustedRemote(uint16,bytes)")
    receive = _semantic_function(analysis, "lzReceive(uint16,bytes,uint64,bytes)")

    assert {"Bridge", "LayerZero"}.issubset(set(analysis["contract_classification"]["standards"]))
    assert {"asset_pull", "cross_chain_message", "bridge_transfer"}.issubset(set(send_from["effect_labels"]))
    assert send_from["action_summary"] == "Transfers value across chains through a bridge or messaging endpoint."
    assert "bridge_config_update" in set_remote["effect_labels"]
    assert "cross_chain_message" not in set_remote["effect_labels"]
    assert set_remote["action_summary"] == "Updates cross-chain bridge or messaging configuration."
    assert {"cross_chain_message", "bridge_receive"}.issubset(set(receive["effect_labels"]))


def test_bridge_context_includes_upgradeability_for_bridge_logic(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "UpgradeableLayerZeroBridge",
        """
        pragma solidity ^0.8.19;

        interface ILayerZeroEndpoint {
            function send(
                uint16 dstChainId,
                bytes calldata destination,
                bytes calldata payload,
                address payable refundAddress,
                address zroPaymentAddress,
                bytes calldata adapterParams
            ) external payable;
        }

        contract UpgradeableLayerZeroBridge {
            address public owner;
            address public implementation;
            ILayerZeroEndpoint public endpoint;
            mapping(uint16 => bytes) public trustedRemoteLookup;

            constructor(ILayerZeroEndpoint endpoint_) {
                owner = msg.sender;
                endpoint = endpoint_;
            }

            function upgradeTo(address newImplementation) external {
                require(msg.sender == owner, "not owner");
                implementation = newImplementation;
            }

            function sendFrom(
                address from,
                uint16 dstChainId,
                bytes calldata toAddress,
                uint256 amount,
                address payable refundAddress,
                address zroPaymentAddress,
                bytes calldata adapterParams
            ) external payable {
                endpoint.send{value: msg.value}(
                    dstChainId,
                    toAddress,
                    abi.encode(from, amount),
                    refundAddress,
                    zroPaymentAddress,
                    adapterParams
                );
            }

            fallback() external payable {
                address impl = implementation;
                assembly {
                    calldatacopy(0, 0, calldatasize())
                    let result := delegatecall(gas(), impl, 0, calldatasize(), 0, 0)
                    returndatacopy(0, 0, returndatasize())
                    switch result
                    case 0 { revert(0, returndatasize()) }
                    default { return(0, returndatasize()) }
                }
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    bridge_context = analysis["bridge_context"]
    upgrade_context = bridge_context["upgrade_context"]

    assert bridge_context["is_bridge"] is True
    assert bridge_context["protocols"] == ["LayerZero"]
    assert "cross_chain_value_transfer" in bridge_context["movement_models"]
    assert any(fn["function"].startswith("sendFrom(") for fn in bridge_context["send_functions"])
    assert upgrade_context["code_has_upgrade_path"] is True
    assert upgrade_context["can_change_bridge_logic"] is True
    assert "upgradeTo(address)" in upgrade_context["admin_paths"]
    assert any(fn["function"] == "upgradeTo(address)" for fn in upgrade_context["upgrade_functions"])
    assert (
        "Bridge behavior can change through an implementation update path detected in code." in bridge_context["notes"]
    )


def test_layerzero_dvn_security_config_surfaces_as_bridge_security(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "LayerZeroDvnConfig",
        """
        pragma solidity ^0.8.19;

        contract LayerZeroDvnConfig {
            struct UlnConfig {
                address[] requiredDVNs;
                address[] optionalDVNs;
                uint8 optionalDVNThreshold;
            }

            address public owner;
            mapping(uint32 => UlnConfig) internal ulnConfigs;

            constructor() {
                owner = msg.sender;
            }

            function setDvnConfig(
                uint32 dstEid,
                address[] calldata requiredDvns,
                address[] calldata optionalDvns,
                uint8 optionalThreshold
            ) external {
                require(msg.sender == owner, "not owner");
                UlnConfig storage cfg = ulnConfigs[dstEid];
                delete cfg.requiredDVNs;
                delete cfg.optionalDVNs;
                for (uint256 i = 0; i < requiredDvns.length; i++) {
                    cfg.requiredDVNs.push(requiredDvns[i]);
                }
                for (uint256 i = 0; i < optionalDvns.length; i++) {
                    cfg.optionalDVNs.push(optionalDvns[i]);
                }
                cfg.optionalDVNThreshold = optionalThreshold;
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    set_config = _semantic_function(analysis, "setDvnConfig(uint32,address[],address[],uint8)")

    assert {"Bridge", "LayerZero"}.issubset(set(analysis["contract_classification"]["standards"]))
    assert {"bridge_config_update", "bridge_security_config"}.issubset(set(set_config["effect_labels"]))
    assert set_config["action_summary"] == "Updates cross-chain bridge security or verification configuration."


def test_hyperlane_ism_security_config_surfaces_as_bridge_security(tmp_path):
    project_dir = _write_project(
        tmp_path,
        "HyperlaneSecuredApp",
        """
        pragma solidity ^0.8.19;

        interface IInterchainSecurityModule {
            function moduleType() external view returns (uint8);
        }

        contract HyperlaneSecuredApp {
            address public owner;
            IInterchainSecurityModule public interchainSecurityModule;

            constructor(IInterchainSecurityModule ism) {
                owner = msg.sender;
                interchainSecurityModule = ism;
            }

            function setInterchainSecurityModule(IInterchainSecurityModule ism) external {
                require(msg.sender == owner, "not owner");
                interchainSecurityModule = ism;
            }
        }
        """,
        slither_output={"results": {"detectors": []}},
    )

    analysis = collect_contract_analysis(project_dir)
    set_ism = _semantic_function(analysis, "setInterchainSecurityModule(IInterchainSecurityModule)")

    assert {"Bridge", "Hyperlane"}.issubset(set(analysis["contract_classification"]["standards"]))
    assert {"bridge_config_update", "bridge_security_config"}.issubset(set(set_ism["effect_labels"]))
    assert set_ism["action_summary"] == "Updates cross-chain bridge security or verification configuration."


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


def test_non_authority_external_calls_with_caller_args_not_classified_as_authority(tmp_path):
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
    semantic = _semantic_function(analysis, "manage(PingTarget,uint256)")

    assert "owner" in semantic["controller_refs"]
    assert "token" not in semantic["controller_refs"]
    assert "external_authority_check" not in semantic["guard_kinds"]


def test_void_role_registry_upgrader_is_controller_ref(tmp_path):
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
    semantic = _semantic_function(analysis, "upgradeTo(address)")

    assert "roleRegistry" in semantic["controller_refs"]
    assert "delegatecall_execution" in semantic["effect_labels"]


def test_external_role_getter_name_is_not_tracked_as_role_identifier(tmp_path):
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
    semantic = _semantic_function(analysis, "pauseContract()")

    assert "roleRegistry" in semantic["controller_refs"]
    assert "BREAK_GLASS" not in semantic["controller_refs"]

    tracked_ids = {target["controller_id"] for target in analysis["controller_tracking"]}
    assert "external_contract:roleRegistry" in tracked_ids
    assert "role_identifier:BREAK_GLASS" not in tracked_ids


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
    semantic = _semantic_function(analysis, "pause()")

    # ``guards`` is no longer populated by the semantic summary.
    assert "BREAK_GLASS" in semantic["controller_refs"]

    tracked = _tracked_controller(analysis, "BREAK_GLASS")
    assert tracked["controller_id"] == "role_identifier:BREAK_GLASS"
    assert tracked["kind"] == "role_identifier"


def test_opaque_external_void_helper_guard_is_controller_ref(tmp_path):
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
    semantic = _semantic_function(analysis, "pause()")
    assert "gate" in semantic["controller_refs"]
    assert "external_contract_call" in semantic["effect_labels"]


def test_opaque_external_role_helper_is_controller_ref(tmp_path):
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
    semantic = _semantic_function(analysis, "pause()")
    assert "auth" in semantic["controller_refs"]
    assert "external_contract_call" in semantic["effect_labels"]


def test_opaque_external_policy_helper_is_controller_ref(tmp_path):
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
    semantic = _semantic_function(analysis, "execute()")
    assert "policy" in semantic["controller_refs"]
    assert "external_contract_call" in semantic["effect_labels"]
