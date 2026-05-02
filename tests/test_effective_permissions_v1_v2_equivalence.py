"""End-to-end equivalence test: ``build_effective_permissions`` output
must be structurally equivalent between the v1-native semantic_guards
path and the v2-shim-derived path.

This is the load-bearing cutover test. Equivalent kind-name bags
(test_v1_v2_equivalence.py) only verify the SHAPE of the synthetic
v1 dict the shim produces — they don't verify that the downstream
consumer (effective_permissions) produces the same authoritative
output. That's where the recent shim bugs hid:

  - Shim emitting controller_source=null for 1-key caller-only
    mappings caused effective_permissions to lose controllers in
    Maker wards / OZ Pausable / blacklist contracts. The kind-name
    bag still matched (mapping_membership both sides), but the
    resolved-controllers list differed.

This test catches that class of bug by comparing the actual
effective_permissions output dict per privileged function.
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

from services.policy.effective_permissions import build_effective_permissions  # noqa: E402
from services.static.contract_analysis_pipeline import (  # noqa: E402
    build_semantic_guards,
    collect_contract_analysis,
)
from services.static.contract_analysis_pipeline.predicate_artifacts import (  # noqa: E402
    build_predicate_artifacts,
)
from services.static.contract_analysis_pipeline.semantic_guards import (  # noqa: E402
    synthesize_semantic_guards_from_predicate_trees,
)


# ---------------------------------------------------------------------------
# Project scaffold (mirrors test_v1_v2_equivalence._write_project)
# ---------------------------------------------------------------------------


_TARGET_ADDRESS = "0x1111111111111111111111111111111111111111"
_OWNER_ADDRESS = "0x2222222222222222222222222222222222222222"


def _write_project(tmp_path: Path, *, source_code: str) -> Path:
    project_dir = tmp_path / "C"
    (project_dir / "src").mkdir(parents=True)
    (project_dir / "foundry.toml").write_text(
        '[profile.default]\nsrc = "src"\nout = "out"\nlibs = ["lib"]\nsolc_version = "0.8.19"\n'
    )
    (project_dir / "src" / "C.sol").write_text(source_code)
    (project_dir / "contract_meta.json").write_text(
        json.dumps(
            {
                "address": _TARGET_ADDRESS,
                "contract_name": "C",
                "compiler_version": "v0.8.19+commit.7dd6d404",
            }
        )
        + "\n"
    )
    (project_dir / "slither_results.json").write_text(
        json.dumps({"results": {"detectors": []}}) + "\n"
    )
    return project_dir


def _v1_emit(project_dir: Path):
    analysis = collect_contract_analysis(project_dir)
    return analysis, build_semantic_guards(analysis)


def _v2_emit(project_dir: Path):
    sl = Slither(str(project_dir / "src" / "C.sol"))
    contract = next(c for c in sl.contracts if c.name == "C")
    artifact = build_predicate_artifacts(contract)
    return synthesize_semantic_guards_from_predicate_trees(
        artifact,
        contract_address=_TARGET_ADDRESS,
        contract_name="C",
    )


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _function_controllers(payload, fn_signature: str) -> list[dict]:
    """Pull the resolved controllers list for the function — the
    field downstream consumers (UI, policy decisions) read."""
    for fn in payload.get("functions", []) or []:
        if fn.get("function") == fn_signature:
            return fn.get("controllers") or []
    return []


def _privileged_signatures(payload) -> set[str]:
    """Functions that have at least one resolved controller — i.e.
    the set effective_permissions treats as guarded."""
    out = set()
    for fn in payload.get("functions", []) or []:
        if fn.get("controllers"):
            out.add(fn.get("function"))
    return out


def _normalize_controller(c: dict) -> dict:
    """Strip metadata that legitimately differs between v1-native
    and v2-shim derivations. We compare semantic content: the set
    of resolved principals + the controller's source label."""
    return {
        "source": c.get("source"),
        "label": c.get("label"),
        "kind": c.get("kind"),
        "principal_addresses": sorted(
            (p.get("address") or "").lower()
            for p in (c.get("principals") or [])
            if isinstance(p, dict)
        ),
    }


def _compare(v1_payload, v2_payload, *, fixture: str) -> None:
    v1_priv = _privileged_signatures(v1_payload)
    v2_priv = _privileged_signatures(v2_payload)

    # v2 catching what v1 missed is acceptable (new_coverage); v1
    # flagging more than v2 is a regression.
    v1_only = v1_priv - v2_priv
    if v1_only:
        pytest.fail(
            f"[{fixture}] v1 flagged {sorted(v1_only)} as having "
            f"controllers but v2 produced empty controllers — REGRESSION"
        )

    for fn in sorted(v1_priv & v2_priv):
        v1_ctrls = sorted(
            (json.dumps(_normalize_controller(c), sort_keys=True))
            for c in _function_controllers(v1_payload, fn)
        )
        v2_ctrls = sorted(
            (json.dumps(_normalize_controller(c), sort_keys=True))
            for c in _function_controllers(v2_payload, fn)
        )
        # v2 may have a STRICT-SUPERSET of v1 controllers (more typed
        # grants from the predicate tree's structural data) — that's
        # acceptable. v1 having a controller v2 lost is the
        # regression direction.
        v1_set = set(v1_ctrls)
        v2_set = set(v2_ctrls)
        missing = v1_set - v2_set
        if missing:
            pytest.fail(
                f"[{fixture}.{fn}] v2 lost controllers v1 had: "
                f"{sorted(missing)}\n"
                f"  v1 controllers: {v1_ctrls}\n"
                f"  v2 controllers: {v2_ctrls}"
            )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


# Maker DSS wards — the fixture that surfaced the storage_var
# fallback bug. v1 emits controller_source='wards'; the shim before
# the fix emitted null. Pinning this case ensures the regression
# stays fixed.
_MAKER_WARDS = """
pragma solidity ^0.8.19;
contract C {
    mapping(address => uint256) public wards;
    uint256 public x;
    function rely(address addr) external {
        require(wards[msg.sender] == 1);
        wards[addr] = 1;
    }
    function mint(uint256 v) external {
        require(wards[msg.sender] == 1);
        x = v;
    }
}
"""

# OZ Ownable — caller_equals_controller path through both pipelines.
_OZ_OWNABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    uint256 public x;
    modifier onlyOwner() { require(msg.sender == ownerVar); _; }
    function setX(uint256 v) external onlyOwner { x = v; }
}
"""

# OZ AccessControl inline — multi-key membership; controller_source
# resolves to the constant role bytes.
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
}
"""

# OZ Pausable — admin-gated pause/unpause writers + transfer gated
# by pause leaf. Tests that the shim's pause-drop behavior matches
# v1's "transfer is privileged but unresolved (no controllers)"
# emit, AND that pause()/unpause() controllers (via ownerVar) match.
_OZ_PAUSABLE = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    bool public _paused;
    uint256 public x;
    modifier whenNotPaused() { require(!_paused); _; }
    function pause() external { require(msg.sender == ownerVar); _paused = true; }
    function unpause() external { require(msg.sender == ownerVar); _paused = false; }
    function transfer() external whenNotPaused { x = x + 1; }
}
"""

# OZ 5.x Ownable cross-fn — v1 emits setX with status=unresolved
# (no predicates; name-heuristic doesn't recognize the cross-fn
# _checkOwner pattern). v2 shim catches via caller_equals_controller
# with controller_source='_owner'. This is the "v2 catches what v1
# missed" / new_coverage direction — verified to NOT trigger a
# regression in the comparator (v1 has setX privileged but empty
# controllers -> not in v1_priv; v2 has setX with controllers ->
# in v2_priv -> v1_only=empty, v2_only={setX}, acceptable).
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
}
"""

# F1 bitwise role flag — value-predicate membership where the
# storage_var is the underlying mapping. Admin-gated setRole writer
# promotes to caller_authority via writer-gate b.i. The shim's
# storage_var fallback should kick in: keys=[msg.sender] only,
# so controller_op is None, fallback to set_descriptor.storage_var
# = 'roles'. This pins the storage_var fallback works for non-bool
# value-predicate maps (Maker wards is uint256==1; this one is
# bitwise mask).
_BITWISE_ROLE_FLAG = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    mapping(address => uint256) public roles;
    uint256 constant FLAG_MINT = 1;
    uint256 public x;
    function setRole(address u, uint256 mask) external {
        require(msg.sender == ownerVar);
        roles[u] = mask;
    }
    function mint(uint256 v) external {
        require((roles[msg.sender] & FLAG_MINT) != 0);
        x = x + v;
    }
}
"""

# Real OZ 5.0+ AccessControl 4-hop overloaded helper chain — onlyRole
# (role) -> _checkRole(role) -> _checkRole(role, _msgSender()) ->
# hasRole(role, account) -> state read. v1's name-heuristic gets
# confused by the 4-hop depth and emits grantRole with
# controller_source=None (unresolved); v2 catches via cross-fn
# revert detection + sub-engine memo + storage_var fallback ->
# controller_source='_roles'. v1 produces empty controllers in EP,
# v2 produces resolved controllers - new_coverage direction.
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

# F2 m-of-n threshold (chained writer-gate fixed-point promotion).
# addOwner -> isOwner promotes -> approve -> execute's threshold
# comparison promotes. v1 sees addOwner + approve as privileged
# (caller_equals_storage + caller_in_mapping); v2 also catches
# execute via comparison/gte but the shim drops it (only
# membership/equality/signature_auth/external_bool map cleanly to
# v1 predicate kinds). Net: target_analysis.access_control only
# has addOwner + approve so execute isn't in either EP output.
# Equivalence holds at the EP boundary.
_M_OF_N = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    mapping(address => bool) public isOwner;
    mapping(bytes32 => uint256) public approvals;
    uint256 constant THRESHOLD = 2;
    function addOwner(address u) external { require(msg.sender == ownerVar); isOwner[u] = true; }
    function approve(bytes32 h) external { require(isOwner[msg.sender]); approvals[h] += 1; }
    function execute(bytes32 h) external view { require(approvals[h] >= THRESHOLD); }
}
"""

# ECDSA signature_auth - canonical recover-then-equality pattern.
# v1's name-heuristic doesn't recognize signature gates (no
# msg.sender literal) so it emits 0 functions. v2 catches via
# signature_auth leaf -> shim emits policy_check predicate.
# effective_permissions with policy_check produces empty
# controllers (no controller_source), so both EP outputs treat
# f as non-privileged. Equivalence trivially holds.
_ECDSA_SIG = """
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

# OZ 5.0+ ReentrancyGuard - split _nonReentrantBefore/After helpers.
# Both v1 (no functions; nonReentrant doesn't classify as auth) and
# v2-shim (reentrancy leaf dropped) produce empty privileged sets.
_OZ_REENTRANCY_5X = """
pragma solidity ^0.8.19;
contract C {
    uint256 private constant NOT_ENTERED = 1;
    uint256 private constant ENTERED = 2;
    uint256 private _status;
    uint256 public x;
    error ReentrancyGuardReentrantCall();
    function _nonReentrantBefore() private {
        if (_status == ENTERED) revert ReentrancyGuardReentrantCall();
        _status = ENTERED;
    }
    function _nonReentrantAfter() private { _status = NOT_ENTERED; }
    modifier nonReentrant() { _nonReentrantBefore(); _; _nonReentrantAfter(); }
    function withdraw() external nonReentrant { x = x + 1; }
}
"""

# EIP-1271 magic-value oracle - external call result == 0x1626ba7e.
# v1 emits 0 functions (no msg.sender literal); v2 catches via
# signature_auth -> shim emits policy_check producing empty
# controllers in EP. Both EP outputs treat f as non-privileged.
_EIP1271 = """
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

# OR composition - require(msg.sender == owner || amount > threshold).
# Both v1 and v2 produce caller_equals_controller with
# controller_source='ownerVar' for the caller side; the business
# branch is structurally distinct in v2 (OR child) but not relevant
# to controllers. Equivalence pinned at ownerVar.
_OWNER_OR_BUSINESS = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    uint256 public minThreshold;
    uint256 public x;
    function f(uint256 amount) external {
        require(msg.sender == ownerVar || amount > minThreshold);
        x = amount;
    }
}
"""

# Time gate - require(block.timestamp > deadline). v1 emits 0
# functions (no caller-related operand); v2 catches via time
# authority_role -> shim drops it. Both EP outputs treat f as
# non-privileged. Pins that pure time gates don't accidentally
# produce controllers in either path.
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

# OZ AC multi-role — three functions each gated by a different
# role constant. v1 and v2 should agree per-function: each gets
# a single mapping_membership predicate with controller_source=
# '_roles' (the storage_var). This pins that the per-function shim
# emit doesn't accidentally cross-pollinate role data between
# functions.
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

# OZ AC cross-fn helper (the EtherFiTimelock case) — onlyRole(role)
# modifier dispatches to _checkRole(role) which contains the
# revert. v1 and v2 shim emit byte-identical mapping_membership
# leaves with controller_source='_roles' (v1 via name-heuristic
# recognizing onlyRole, v2 via cross-fn revert detection +
# caller-side ParameterBindingEnv + storage_var fallback). This
# pins the cross-fn parameter-binding path produces the same EP
# output as v1's direct emit.
_OZ_AC_CROSS_FN = """
pragma solidity ^0.8.19;
contract C {
    mapping(bytes32 => mapping(address => bool)) private _roles;
    mapping(bytes32 => bytes32) private _roleAdmins;
    modifier onlyRole(bytes32 role) { _checkRole(role); _; }
    function _checkRole(bytes32 role) internal view {
        if (!_roles[role][msg.sender]) revert();
    }
    function getRoleAdmin(bytes32 role) public view returns (bytes32) {
        return _roleAdmins[role];
    }
    function grantRole(bytes32 role, address account)
        public onlyRole(getRoleAdmin(role))
    {
        _roles[role][account] = true;
    }
}
"""

# F4 hashed-key membership — _authorized[keccak256(abi.encode(role,
# msg.sender))]. The set_descriptor has expanded key_sources via
# _expand_key_operand walking through the keccak/abi.encode SolidityCalls.
# v1's name-heuristic doesn't recognize the hashed-key membership
# pattern -> f isn't privileged in v1. v2 catches via multi-key
# direct-promote -> mapping_membership with controller_source from
# storage_var. Pure new_coverage direction.
_HASHED_KEY = """
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


# Combined stack — onlyOwner + whenNotPaused + nonReentrant.
# v1 emits 3 caller_equals_controller predicates (name-heuristic
# treats every modifier-style guard as caller-controller); v2 emits
# 1 caller_authority equality leaf (for onlyOwner) plus pause +
# reentrancy leaves which the shim drops. Functional equivalence
# in EP holds because the dedupe in _controller_grants_for_refs
# collapses the 3 v1 grants (all pointing to ownerVar) into 1
# grant matching v2's single grant. This pins that the dedupe
# protects the cutover.
_COMBINED_STACK = """
pragma solidity ^0.8.19;
contract C {
    address public ownerVar;
    bool public _paused;
    uint256 private _status;
    uint256 private constant _NOT_ENTERED = 1;
    uint256 private constant _ENTERED = 2;
    uint256 public x;
    modifier onlyOwner() { require(msg.sender == ownerVar); _; }
    modifier whenNotPaused() { require(!_paused); _; }
    modifier nonReentrant() {
        require(_status != _ENTERED);
        _status = _ENTERED;
        _;
        _status = _NOT_ENTERED;
    }
    function pause() external onlyOwner { _paused = true; }
    function withdraw() external onlyOwner whenNotPaused nonReentrant { x = x + 1; }
}
"""


_FIXTURES = [
    ("maker_wards", _MAKER_WARDS),
    ("oz_ownable", _OZ_OWNABLE),
    ("oz_ac_inline", _OZ_AC_INLINE),
    ("oz_pausable", _OZ_PAUSABLE),
    ("oz_ownable_5x", _OZ_OWNABLE_5X),
    ("bitwise_role_flag", _BITWISE_ROLE_FLAG),
    ("combined_stack", _COMBINED_STACK),
    ("hashed_key_membership", _HASHED_KEY),
    ("oz_ac_cross_fn", _OZ_AC_CROSS_FN),
    ("oz_ac_multi_role", _OZ_AC_MULTI_ROLE),
    ("oz_ac_5x_overloaded", _OZ_AC_5X_OVERLOADED),
    ("m_of_n_threshold", _M_OF_N),
    ("ecdsa_signature_auth", _ECDSA_SIG),
    ("oz_reentrancy_5x", _OZ_REENTRANCY_5X),
    ("eip1271_magic", _EIP1271),
    ("owner_or_business", _OWNER_OR_BUSINESS),
    ("time_gate", _TIME_GATE),
]


@pytest.mark.parametrize("fixture_name,source", _FIXTURES, ids=[f[0] for f in _FIXTURES])
def test_effective_permissions_equivalent(tmp_path: Path, fixture_name: str, source: str):
    project_dir = _write_project(
        tmp_path,
        source_code=textwrap.dedent(source).strip() + "\n",
    )
    target_analysis, v1_sg = _v1_emit(project_dir)
    v2_sg = _v2_emit(project_dir)

    # Same target_snapshot for both — sets up controller_lookup with
    # owner / wards / role values so resolved-principals are
    # populated. Without this the test only verifies the privileged-
    # set intersection, not the controller dicts.
    # _controller_lookup keys by value['source'] (or by the dict key
    # if no source field). Adding source matching the predicate's
    # controller_source makes the lookup hit and produce resolved
    # principals — without it both v1 and v2 produce empty controllers
    # and the comparator can't distinguish a real bug from "neither
    # side has signal".
    # State-var entries cover every fixture's controller_source. Each
    # entry's 'source' key is what _controller_lookup uses to resolve
    # a predicate's controller_source -> resolved principal. Listing
    # superset-of-needed values is fine — fixtures that don't reference
    # a given source just don't trip its lookup.
    def _sv_entry(name: str) -> dict:
        return {
            "source": name,
            "kind": "state_variable",
            "label": name,
            "value": _OWNER_ADDRESS,
            "resolved_type": "eoa",
            "details": {"address": _OWNER_ADDRESS},
        }

    target_snapshot = {
        "contract_name": "C",
        "controller_values": {
            "state_variable:ownerVar": _sv_entry("ownerVar"),
            "state_variable:wards": _sv_entry("wards"),
            "state_variable:_owner": _sv_entry("_owner"),
            "state_variable:roles": _sv_entry("roles"),
            "state_variable:_roles": _sv_entry("_roles"),
            "state_variable:_authorized": _sv_entry("_authorized"),
            "constant:0x" + "01" * 32: {
                "source": "0x" + "01" * 32,
                "kind": "constant",
                "label": "0x" + "01" * 32,
                "value": _OWNER_ADDRESS,
                "resolved_type": "eoa",
                "details": {"address": _OWNER_ADDRESS},
            },
        },
    }

    v1_payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        semantic_guards=v1_sg,
    )
    v2_payload = build_effective_permissions(
        target_analysis,
        target_snapshot=target_snapshot,
        semantic_guards=v2_sg,
    )

    _compare(v1_payload, v2_payload, fixture=fixture_name)
