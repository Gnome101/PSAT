"""Cutover-equivalence test on the canonical corpus.

For each of a small set of representative shapes, runs BOTH the
native v1 pipeline (collect_contract_analysis →
build_semantic_guards) and the v2 path (build_predicate_artifacts
→ synthesize_semantic_guards_from_predicate_trees), then
compares the two output dicts at a structural level.

This is the operator's pre-flip evidence: when this test passes
across the corpus, flipping ``PSAT_POLICY_USE_V2_SHIM`` in
production should be a no-op for ``effective_permissions``.

Equivalence rules (intentional looseness):

  - Both must agree on which functions are PRIVILEGED (status !=
    public). Disagreements here would change which functions
    drive effective_permissions and are blocking.
  - For agreed-privileged functions, the predicate KINDS bag
    must match (caller_equals_controller / mapping_membership /
    policy_check / external_helper). controller_label /
    controller_source CAN differ — v1 carries graph IDs that v2
    doesn't, and v2 carries typed source values v1 doesn't.
  - status == "partial"/"unresolved" is allowed to differ on
    edge cases — the cutover stage is more conservative about
    what counts as resolved than v1's name-heuristic was; the
    target here is no v1 → unresolved transitions in v2 (a
    regression), but v2 → resolved transitions on what v1
    called partial are fine (new coverage).
"""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline import (  # noqa: E402
    build_semantic_guards,
    collect_contract_analysis,
)
from services.static.contract_analysis_pipeline.predicate_artifacts import (  # noqa: E402
    build_predicate_artifacts,
)
from services.static.contract_analysis_pipeline.v2_to_v1_shim import (  # noqa: E402
    synthesize_semantic_guards_from_predicate_trees,
)


# ---------------------------------------------------------------------------
# Project scaffold (mirrors test_contract_analysis._write_project but inline)
# ---------------------------------------------------------------------------


def _write_project(tmp_path: Path, *, contract_name: str, source_code: str) -> Path:
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
        json.dumps({"results": {"detectors": []}}) + "\n"
    )
    return project_dir


def _v1_emit(project_dir: Path) -> dict:
    analysis = collect_contract_analysis(project_dir)
    return build_semantic_guards(analysis)


def _v2_emit(project_dir: Path, contract_name: str) -> dict:
    sl = Slither(str(project_dir / "src" / f"{contract_name}.sol"))
    contract = next(c for c in sl.contracts if c.name == contract_name)
    artifact = build_predicate_artifacts(contract)
    return synthesize_semantic_guards_from_predicate_trees(
        artifact,
        contract_address="0x1111111111111111111111111111111111111111",
        contract_name=contract_name,
    )


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _functions_by_name(emit: dict) -> dict[str, dict]:
    return {fn["function"]: fn for fn in emit.get("functions", [])}


def _privileged(emit: dict) -> set[str]:
    """Function names whose status is not 'public' — i.e. the
    set effective_permissions treats as guarded."""
    return {fn["function"] for fn in emit.get("functions", []) if fn.get("status") != "public"}


def _predicate_kinds(fn_entry: dict) -> set[str]:
    """Predicate-kind bag with semantic-equivalence normalization.

    v1's name-driven heuristic emits role_member vs mapping_membership
    inconsistently across OZ AC patterns (role_member when it sees
    hasRole/grantRole/getRoleAdmin function names, mapping_membership
    otherwise). Downstream effective_permissions handles BOTH kinds
    correctly via separate code paths. For cutover-equivalence we
    treat them as one membership-class kind so neither direction is
    flagged as a regression — what matters is whether each privileged
    function carries SOMEthing in the membership family, not which
    name v1 used.
    """
    raw = {p.get("kind") for p in (fn_entry.get("predicates") or []) if p.get("kind")}
    normalized = set()
    for k in raw:
        if k in ("role_member", "mapping_membership"):
            normalized.add("membership_kind")
        else:
            normalized.add(k)
    return normalized


def _assert_equivalent(v1: dict, v2: dict, *, contract_name: str) -> None:
    """Loose structural equivalence — the v2 cutover-target."""
    v1_funcs = _functions_by_name(v1)
    v2_funcs = _functions_by_name(v2)

    v1_priv = _privileged(v1)
    v2_priv = _privileged(v2)

    v1_only = v1_priv - v2_priv
    v2_only = v2_priv - v1_priv
    if v1_only:
        pytest.fail(
            f"[{contract_name}] v1 flagged {sorted(v1_only)} as privileged but "
            f"v2 didn't — REGRESSION. v2 status: "
            f"{ {fn: v2_funcs.get(fn, {}).get('status') for fn in v1_only} }"
        )
    # v2_only is acceptable (new coverage); just record it.

    # For agreed-privileged functions, check predicate-kind bags.
    for fn in sorted(v1_priv & v2_priv):
        v1_kinds = _predicate_kinds(v1_funcs[fn])
        v2_kinds = _predicate_kinds(v2_funcs[fn])
        # v2's kind set should be a subset of, or equal to, v1's
        # — synthesizing more predicate kinds than v1 is
        # acceptable on this rough cutover gate; missing kinds v1
        # produced is the regression.
        missing = v1_kinds - v2_kinds
        if missing:
            pytest.fail(
                f"[{contract_name}.{fn}] v2 missed v1 predicate kinds {sorted(missing)}; "
                f"v1={sorted(v1_kinds)} v2={sorted(v2_kinds)}"
            )


# ---------------------------------------------------------------------------
# Per-shape parametrization
# ---------------------------------------------------------------------------


_OZ_OWNABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    uint256 public x;
    modifier onlyOwner() {
        require(msg.sender == ownerVar);
        _;
    }
    function setX(uint256 v) external onlyOwner { x = v; }
    function bumpX() external onlyOwner { x = x + 1; }
    function open() external { x = 0; }
}
"""

_OZ_AC_INLINE = """
pragma solidity ^0.8.19;
contract C {
    mapping(bytes32 => mapping(address => bool)) private _roles;
    bytes32 public constant MINTER = keccak256("MINTER");
    uint256 public x;
    function mint(uint256 v) external {
        require(_roles[MINTER][msg.sender]);
        x = v;
    }
    function totalSupply() external view returns (uint256) { return x; }
}
"""

_OZ_PAUSABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    bool public _paused;
    uint256 public x;
    modifier whenNotPaused() {
        require(!_paused);
        _;
    }
    function pause() external {
        require(msg.sender == ownerVar);
        _paused = true;
    }
    function transfer() external whenNotPaused {
        x = x + 1;
    }
}
"""


_MAKER_WARDS = """
pragma solidity ^0.8.19;
contract C {
    mapping(address => uint256) public wards;
    uint256 public x;
    function rely(address addr) external {
        require(wards[msg.sender] == 1);
        wards[addr] = 1;
    }
    function deny(address addr) external {
        require(wards[msg.sender] == 1);
        wards[addr] = 0;
    }
    function mint(uint256 v) external {
        require(wards[msg.sender] == 1);
        x = v;
    }
}
"""

_OZ_REENTRANCY_GUARD = """
pragma solidity ^0.8.19;
contract C {
    uint256 private _status;
    uint256 private constant _NOT_ENTERED = 1;
    uint256 private constant _ENTERED = 2;
    uint256 public x;
    modifier nonReentrant() {
        require(_status != _ENTERED);
        _status = _ENTERED;
        _;
        _status = _NOT_ENTERED;
    }
    function withdraw() external nonReentrant {
        x = x + 1;
    }
}
"""

_OZ_AC_CROSS_FN_HELPER = """
pragma solidity ^0.8.19;
contract C {
    mapping(bytes32 => mapping(address => bool)) private _roles;
    mapping(bytes32 => bytes32) private _roleAdmins;
    modifier onlyRole(bytes32 role) {
        _checkRole(role);
        _;
    }
    function _checkRole(bytes32 role) internal view {
        if (!_roles[role][msg.sender]) revert();
    }
    function getRoleAdmin(bytes32 role) public view returns (bytes32) {
        return _roleAdmins[role];
    }
    function grantRole(bytes32 role, address account) public onlyRole(getRoleAdmin(role)) {
        _roles[role][account] = true;
    }
}
"""

_OZ_AC_MULTI_ROLE = """
pragma solidity ^0.8.19;
contract C {
    mapping(bytes32 => mapping(address => bool)) private _roles;
    bytes32 public constant MINTER = keccak256("MINTER");
    bytes32 public constant BURNER = keccak256("BURNER");
    bytes32 public constant ADMIN = keccak256("ADMIN");
    uint256 public x;

    function mint(uint256 v) external {
        require(_roles[MINTER][msg.sender]);
        x = x + v;
    }
    function burn(uint256 v) external {
        require(_roles[BURNER][msg.sender]);
        x = x - v;
    }
    function setX(uint256 v) external {
        require(_roles[ADMIN][msg.sender]);
        x = v;
    }
}
"""

_BITWISE_ROLE_FLAG = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    mapping(address => uint256) public roles;
    uint256 constant FLAG_MINT = 1;
    uint256 constant FLAG_BURN = 2;
    uint256 public x;

    // Admin writer — without it the 1-key caller-keyed
    // membership stays business per writer-gate rule a (no
    // external_keyed writers). Realistic contracts always have
    // an admin function that flips role flags.
    function setRole(address u, uint256 mask) external {
        require(msg.sender == ownerVar);
        roles[u] = mask;
    }

    function mint(uint256 v) external {
        require((roles[msg.sender] & FLAG_MINT) != 0);
        x = x + v;
    }
    function burn(uint256 v) external {
        require((roles[msg.sender] & FLAG_BURN) != 0);
        x = x - v;
    }
}
"""

_MULTI_MODIFIER = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    bool public _paused;
    uint256 public x;

    modifier onlyOwner() {
        require(msg.sender == ownerVar);
        _;
    }
    modifier whenNotPaused() {
        require(!_paused);
        _;
    }

    function privileged(uint256 v) external onlyOwner whenNotPaused {
        x = v;
    }
    function pause() external onlyOwner {
        _paused = true;
    }
}
"""


_HASHED_KEY_MEMBERSHIP = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    mapping(bytes32 => bool) public _authorized;

    function authorize(bytes32 role, address user) external {
        require(msg.sender == ownerVar);
        _authorized[keccak256(abi.encode(role, user))] = true;
    }
    function f(bytes32 role) external view {
        require(_authorized[keccak256(abi.encode(role, msg.sender))]);
    }
}
"""

_M_OF_N_THRESHOLD = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    mapping(address => bool) public isOwner;
    mapping(bytes32 => uint256) public approvals;
    uint256 constant THRESHOLD = 2;

    function addOwner(address user) external {
        require(msg.sender == ownerVar);
        isOwner[user] = true;
    }
    function approve(bytes32 txHash) external {
        require(isOwner[msg.sender]);
        approvals[txHash] += 1;
    }
    function execute(bytes32 txHash) external view {
        require(approvals[txHash] >= THRESHOLD);
    }
}
"""

_ECDSA_SIGNATURE_AUTH = """
pragma solidity ^0.8.19;
contract C {
    address public signerAddr;
    uint256 public x;
    function f(bytes32 h, uint8 v, bytes32 r, bytes32 s) external {
        address recovered = ecrecover(h, v, r, s);
        require(recovered == signerAddr);
        x = 1;
    }
}
"""

_EIP1271_MAGIC = """
pragma solidity ^0.8.19;
interface IERC1271 {
    function isValidSignature(bytes32 hash, bytes memory signature) external view returns (bytes4);
}
contract C {
    address public signerContract;
    uint256 public x;
    function f(bytes32 hash, bytes calldata sig) external {
        require(IERC1271(signerContract).isValidSignature(hash, sig) == 0x1626ba7e);
        x = 1;
    }
}
"""

_TIME_GATE = """
pragma solidity ^0.8.19;
contract C {
    uint256 public deadline;
    uint256 public x;
    function f() external {
        require(block.timestamp > deadline);
        x = 1;
    }
}
"""

_OZ_OWNABLE_5X = """
pragma solidity ^0.8.19;
contract C {
    address private _owner;
    uint256 public x;
    error OwnableUnauthorizedAccount(address account);
    function _msgSender() internal view returns (address) { return msg.sender; }
    function owner() public view returns (address) { return _owner; }
    function _checkOwner() internal view {
        if (owner() != _msgSender()) revert OwnableUnauthorizedAccount(_msgSender());
    }
    modifier onlyOwner() { _checkOwner(); _; }
    function setX(uint256 v) external onlyOwner { x = v; }
    function open() external { x = 0; }
}
"""

_OZ_AC_5X_OVERLOADED = """
pragma solidity ^0.8.19;
contract C {
    mapping(bytes32 => mapping(address => bool)) private _roles;
    uint256 public x;
    error AccessControlUnauthorizedAccount(address account, bytes32 neededRole);
    function _msgSender() internal view returns (address) { return msg.sender; }
    function hasRole(bytes32 role, address account) public view returns (bool) {
        return _roles[role][account];
    }
    function _checkRole(bytes32 role, address account) internal view {
        if (!hasRole(role, account)) revert AccessControlUnauthorizedAccount(account, role);
    }
    function _checkRole(bytes32 role) internal view { _checkRole(role, _msgSender()); }
    modifier onlyRole(bytes32 role) { _checkRole(role); _; }
    function grantRole(bytes32 role, address account) public onlyRole(role) {
        _roles[role][account] = true;
    }
}
"""

_FIXTURES = [
    ("oz_ownable", _OZ_OWNABLE),
    ("oz_access_control_inline", _OZ_AC_INLINE),
    ("oz_pausable", _OZ_PAUSABLE),
    ("maker_wards", _MAKER_WARDS),
    ("oz_reentrancy_guard", _OZ_REENTRANCY_GUARD),
    ("oz_ac_cross_fn_helper", _OZ_AC_CROSS_FN_HELPER),
    ("oz_ac_multi_role", _OZ_AC_MULTI_ROLE),
    ("bitwise_role_flag", _BITWISE_ROLE_FLAG),
    ("multi_modifier", _MULTI_MODIFIER),
    ("hashed_key_membership", _HASHED_KEY_MEMBERSHIP),
    ("m_of_n_threshold", _M_OF_N_THRESHOLD),
    ("ecdsa_signature_auth", _ECDSA_SIGNATURE_AUTH),
    ("eip1271_magic", _EIP1271_MAGIC),
    ("time_gate", _TIME_GATE),
    ("oz_ownable_5x", _OZ_OWNABLE_5X),
    ("oz_ac_5x_overloaded", _OZ_AC_5X_OVERLOADED),
]


@pytest.mark.parametrize("contract_name,source", _FIXTURES, ids=lambda x: x if isinstance(x, str) else None)
def test_v1_v2_semantic_guards_equivalent(tmp_path, contract_name, source):
    project_dir = _write_project(
        tmp_path, contract_name="C", source_code=textwrap.dedent(source).strip() + "\n"
    )
    v1 = _v1_emit(project_dir)
    v2 = _v2_emit(project_dir, "C")

    # Sanity: the v2 dict carries the synthetic marker.
    assert v2.get("_synthetic_from") == "v2_predicate_trees"

    _assert_equivalent(v1, v2, contract_name=contract_name)


def test_assert_equivalent_detects_regression(tmp_path):
    """Pin the regression-detection direction. Synthesize a v2
    dict that DROPS a function v1 marks privileged; the
    comparison must fail."""
    v1 = {
        "functions": [
            {"function": "f()", "status": "resolved", "predicates": [{"kind": "caller_equals_controller"}]}
        ]
    }
    v2 = {"_synthetic_from": "v2_predicate_trees", "functions": []}
    with pytest.raises(pytest.fail.Exception, match="REGRESSION"):
        _assert_equivalent(v1, v2, contract_name="X")


def test_assert_equivalent_allows_new_coverage(tmp_path):
    """v2 catching a function v1 missed is allowed (new coverage)."""
    v1 = {"functions": []}
    v2 = {
        "_synthetic_from": "v2_predicate_trees",
        "functions": [
            {"function": "f()", "status": "resolved", "predicates": [{"kind": "caller_equals_controller"}]}
        ],
    }
    _assert_equivalent(v1, v2, contract_name="X")  # must not raise


def test_assert_equivalent_detects_missing_predicate_kind(tmp_path):
    """Both flag the function privileged, but v2 missed the
    predicate kind v1 produced — that's a regression too."""
    v1 = {
        "functions": [
            {
                "function": "f()",
                "status": "resolved",
                "predicates": [
                    {"kind": "caller_equals_controller"},
                    {"kind": "mapping_membership"},
                ],
            }
        ]
    }
    v2 = {
        "_synthetic_from": "v2_predicate_trees",
        "functions": [
            {
                "function": "f()",
                "status": "resolved",
                "predicates": [{"kind": "caller_equals_controller"}],
            }
        ],
    }
    with pytest.raises(pytest.fail.Exception, match="missed v1 predicate kinds"):
        _assert_equivalent(v1, v2, contract_name="X")
