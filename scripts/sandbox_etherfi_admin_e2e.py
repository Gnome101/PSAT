"""End-to-end smoke test on an EtherFiAdmin-shaped contract.

Compiles a synthetic contract that exercises every shape PR A-D
should resolve:

  * ``transferOwnership(address)`` — OZ Ownable `onlyOwner` → caller
    must equal ``_owner`` state variable. PR A.2 enumerates this via
    ``ControllerValue`` rows; PR D.1 preserves the equality.
  * ``upgradeTo(address)`` — gated by an external authority call
    ``roleRegistry.onlyProtocolUpgrader(msg.sender)``. PR C.1
    classifies this as a void cross-contract auth call; PR C.2
    inlines the registry's own predicate tree.
  * ``pauseContract()`` — gated by ``hasRole(PROTOCOL_PAUSER, msg.sender)``
    on the role registry. PR A.1 attaches the set descriptor.
  * ``setBalance(address user, uint256 amount)`` — gated by
    ``balances[msg.sender] >= 10`` (a value predicate). PR D.1
    preserves the operator + RHS; PR D.4 (when ENVIO_API_TOKEN
    is set) replays traces to enumerate the population.

Then runs through the static pipeline + capability resolver and
prints the resolved capability for every external function.
Without network access the value-predicate function falls back to
external_check_only — that's the honest output, not a regression.

Run:
  cd /home/gnome2/asu/capstone/psat-predicate-pipeline
  source /tmp/psat_test_env.sh   # exports TEST_DATABASE_URL, MinIO, etc.
  uv run --active python scripts/sandbox_etherfi_admin_e2e.py
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


CONTRACT_SOURCE = r"""// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

interface IRoleRegistry {
    function onlyProtocolUpgrader(address user) external view;
    function hasRole(bytes32 role, address account) external view returns (bool);
}

contract EtherFiAdminLike {
    // OZ Ownable shape — _owner state var, transferOwnership/renounceOwnership
    address private _owner;
    bool public paused;
    uint256 private _reentrancyStatus;
    mapping(address => uint256) public balances;
    mapping(address => bool) public delegators;
    IRoleRegistry public roleRegistry;

    bytes32 public constant PROTOCOL_PAUSER = keccak256("PROTOCOL_PAUSER");
    bytes32 public constant PROTOCOL_ADMIN = keccak256("PROTOCOL_ADMIN");

    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event Paused();
    event Unpaused();
    event BalanceSet(address indexed user, uint256 amount);
    event DelegatorSet(address indexed who, bool allowed);

    constructor(address registry_) {
        _owner = msg.sender;
        roleRegistry = IRoleRegistry(registry_);
        _reentrancyStatus = 1;
    }

    modifier onlyOwner() {
        require(msg.sender == _owner, "not owner");
        _;
    }

    modifier whenNotPaused() {
        require(!paused, "paused");
        _;
    }

    modifier nonReentrant() {
        require(_reentrancyStatus != 2, "reentrancy");
        _reentrancyStatus = 2;
        _;
        _reentrancyStatus = 1;
    }

    function owner() public view returns (address) {
        return _owner;
    }

    function transferOwnership(address newOwner) public onlyOwner {
        require(newOwner != address(0), "zero");
        emit OwnershipTransferred(_owner, newOwner);
        _owner = newOwner;
    }

    function renounceOwnership() public onlyOwner {
        emit OwnershipTransferred(_owner, address(0));
        _owner = address(0);
    }

    // Cross-contract authority — void onlyXxx call (PR C.1).
    function upgradeTo(address newImplementation) external {
        roleRegistry.onlyProtocolUpgrader(msg.sender);
        // ... do upgrade ...
        require(newImplementation != address(0), "zero impl");
    }

    // PROTOCOL_PAUSER hasRole shape (PR A.1).
    function pauseContract() external {
        require(roleRegistry.hasRole(PROTOCOL_PAUSER, msg.sender), "not pauser");
        paused = true;
        emit Paused();
    }

    // whenNotPaused + role check — exercises pause-side-condition.
    function unpauseContract() external whenNotPaused {
        // Inverted: callable only if NOT paused, by PROTOCOL_ADMIN.
        require(roleRegistry.hasRole(PROTOCOL_ADMIN, msg.sender), "not admin");
        paused = false;
        emit Unpaused();
    }

    // Multi-role OR — pauser OR admin can drain.
    function emergencyAction() external nonReentrant {
        require(
            roleRegistry.hasRole(PROTOCOL_PAUSER, msg.sender)
                || roleRegistry.hasRole(PROTOCOL_ADMIN, msg.sender),
            "no role"
        );
        // ... emergency logic ...
    }

    // Value predicate (PR D.1+): caller must hold balance >= 10 to set
    // someone else's balance. Polarity-folded into op="gte", rhs=["10"].
    function setBalance(address user, uint256 amount) external whenNotPaused {
        if (balances[msg.sender] < 10) revert();
        balances[user] = amount;
        emit BalanceSet(user, amount);
    }

    // Bool-mapping membership: ``delegators[msg.sender] == true``.
    function delegate(address target) external {
        require(delegators[msg.sender], "not delegator");
        // ... delegation logic ...
        target;
    }

    // Owner-managed bool-mapping: only owner adds delegators.
    function setDelegator(address who, bool allowed) external onlyOwner {
        delegators[who] = allowed;
        emit DelegatorSet(who, allowed);
    }
}
"""


def main() -> int:
    with tempfile.TemporaryDirectory() as workdir:
        sl = _compile(Path(workdir), CONTRACT_SOURCE)
        contract = next(c for c in sl.contracts if c.name == "EtherFiAdminLike")
        print(f"=== Compiled {contract.name}")

        # 1. Static — predicate trees per function.
        from services.static.contract_analysis_pipeline.predicate_artifacts import (
            build_predicate_artifacts,
        )

        artifact = build_predicate_artifacts(contract)
        trees = artifact["trees"]

        print(f"\n=== Predicate trees: {len(trees)} guarded functions")
        for fn, tree in sorted(trees.items()):
            print(f"\n--- {fn}")
            print(json.dumps(_dump(tree), indent=2, default=_serialize))

        # 2. Resolution — evaluate each tree against an empty in-memory
        # context so we exercise the dispatch chain without DB / RPC.
        from services.resolution.adapters import AdapterRegistry, EvaluationContext
        from services.resolution.adapters.access_control import AccessControlAdapter
        from services.resolution.adapters.aragon_acl import (
            AragonACLAdapter,
            DSAuthAdapter,
            EIP1271Adapter,
        )
        from services.resolution.adapters.event_indexed import EventIndexedAdapter
        from services.resolution.adapters.mapping_trace import MappingTraceAdapter
        from services.resolution.adapters.safe import SafeAdapter
        from services.resolution.capability_resolver import capability_to_dict
        from services.resolution.predicate_evaluator import evaluate_tree_with_registry

        registry = AdapterRegistry()
        for cls in (
            AccessControlAdapter,
            SafeAdapter,
            AragonACLAdapter,
            DSAuthAdapter,
            EIP1271Adapter,
            EventIndexedAdapter,
            MappingTraceAdapter,
        ):
            registry.register(cls)

        # State-var values seeded as if the resolver had read the
        # ControllerValue table — the contract has _owner and roleRegistry.
        seeded_owner = "0xdeadbeefcafebabedeadbeefcafebabedeadbeef"
        seeded_registry = "0x0000000000000000000000000000000000001234"

        # Synthetic trace fetcher seeds two ``setBalance`` calls so PR D.4
        # has data to enumerate. Two callers:
        #   - alice with balance 50 → passes ``>= 10``, lands in finite_set
        #   - eve   with balance 3  → fails the gate
        from eth_abi.abi import encode

        from services.resolution.adapters.mapping_trace import FetchedTrace

        alice = "0x" + "11" * 20
        eve = "0x" + "22" * 20
        bob = "0x" + "33" * 20
        from eth_utils.crypto import keccak

        set_balance_selector = "0x" + keccak(text="setBalance(address,uint256)").hex()[:8]
        set_delegator_selector = "0x" + keccak(text="setDelegator(address,bool)").hex()[:8]

        def _balance_trace(user: str, value: int, block: int) -> FetchedTrace:
            body = encode(["address", "uint256"], [user, value]).hex()
            return FetchedTrace(
                block_number=block,
                transaction_index=0,
                trace_address=(0,),
                input_data=set_balance_selector + body,
                call_type="call",
                error=None,
            )

        def _delegator_trace(who: str, allowed: bool, block: int) -> FetchedTrace:
            body = encode(["address", "bool"], [who, allowed]).hex()
            return FetchedTrace(
                block_number=block,
                transaction_index=0,
                trace_address=(0,),
                input_data=set_delegator_selector + body,
                call_type="call",
                error=None,
            )

        class _StubTraceFetcher:
            def fetch_traces(self, **_):
                return [
                    _balance_trace(alice, 50, block=100),
                    _balance_trace(eve, 3, block=101),
                    # bob became a delegator; eve was added then revoked.
                    _delegator_trace(bob, True, block=200),
                    _delegator_trace(eve, True, block=201),
                    _delegator_trace(eve, False, block=202),
                ]

        ctx = EvaluationContext(
            chain_id=1,
            contract_address="0xCC00000000000000000000000000000000000001",
            state_var_values={
                "_owner": seeded_owner,
                "roleRegistry": seeded_registry,
            },
            trace_fetcher=_StubTraceFetcher(),
        )

        print("\n=== Resolved capabilities")
        for fn, tree in sorted(trees.items()):
            cap = evaluate_tree_with_registry(tree, registry, ctx)
            cap_dict = capability_to_dict(cap)
            print(f"\n--- {fn}")
            print(json.dumps(cap_dict, indent=2, default=str))

        return 0


def _compile(workdir: Path, source: str):
    src_path = workdir / "EtherFiAdmin.sol"
    src_path.write_text(source)
    from slither.slither import Slither

    return Slither(str(src_path))


def _dump(obj):
    """Recursive copy that strips non-JSON-serializable internals so the
    tree prints cleanly. Bytes / dataclasses become strings."""
    if isinstance(obj, dict):
        return {k: _dump(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_dump(v) for v in obj]
    return obj


def _serialize(obj):
    if isinstance(obj, bytes):
        return "0x" + obj.hex()
    return str(obj)


def _summarize_tree(node, depth: int = 0) -> None:
    indent = "  " * depth
    if not isinstance(node, dict):
        print(f"{indent}<bad node: {type(node).__name__}>")
        return
    kind = node.get("kind")
    if kind == "leaf":
        leaf = node.get("leaf") or {}
        op = leaf.get("operator")
        leaf_kind = leaf.get("kind")
        role = leaf.get("authority_role")
        descriptor = leaf.get("set_descriptor") or {}
        storage_var = descriptor.get("storage_var")
        truthy = descriptor.get("truthy_value")
        vp = descriptor.get("value_predicate")
        ws = descriptor.get("writer_selectors")
        ac = descriptor.get("authority_contract")
        bits = [f"kind={leaf_kind}", f"op={op}", f"role={role}"]
        if storage_var:
            bits.append(f"sv={storage_var}")
        if truthy:
            bits.append(f"truthy={truthy}")
        if vp:
            bits.append(f"value_pred={vp['op']}({vp['rhs_values']})")
        if ws:
            bits.append(f"writer_selectors={len(ws)}")
        if ac:
            bits.append(f"authority={ac}")
        print(f"{indent}{' '.join(bits)}")
    else:
        print(f"{indent}{kind}")
        for child in node.get("children") or []:
            _summarize_tree(child, depth + 1)


if __name__ == "__main__":
    sys.exit(main())
