"""Pinning tests for guard patterns the current pipeline misses.

Each test exercises a real-world auth pattern that the *generic*
predicate pipeline should pick up structurally — but doesn't, because
the static stage's structural detectors are too narrow. Marked
``xfail(strict=True)`` so when the generic extensions land, every
test flips to XPASS and pytest's strict mode forces removing the
xfail decorator (ratchet against silent regressions).

The fix in each case is a *generalization* of existing detection,
not a per-protocol adapter. The fix paths are summarized in the
docstring of each test.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

slither = pytest.importorskip("slither")
from slither import Slither  # noqa: E402

from services.static.contract_analysis_pipeline.predicates import (  # noqa: E402
    build_predicate_tree,
)
from services.static.contract_analysis_pipeline.writer_gate import (  # noqa: E402
    apply_writer_gate_pass,
)


def _compile(tmp_path: Path, source: str) -> Slither:
    src = textwrap.dedent(source).strip() + "\n"
    f = tmp_path / "C.sol"
    f.write_text(src)
    return Slither(str(f))


def _build_pipeline(contract):
    trees = {}
    for fn in contract.functions:
        if fn.is_constructor:
            continue
        trees[fn.full_name] = build_predicate_tree(fn)
    apply_writer_gate_pass(contract, trees)
    return trees


def _all_leaves(tree):
    if tree is None:
        return []
    if tree.get("op") == "LEAF":
        return [tree["leaf"]] if tree.get("leaf") else []
    out = []
    for child in tree.get("children") or []:
        out.extend(_all_leaves(child))
    return out


# ---------------------------------------------------------------------------
# 1. Diamond ACL — storage at hashed slot via assembly
# ---------------------------------------------------------------------------


def test_diamond_acl_membership_classifies_caller_authority(tmp_path):
    """SURPRISE PASS: the existing pipeline handles Diamond ACL
    structurally via internal-call recursion + Member/Index chaining.
    Slither models ``LibDiamond.aclStorage()`` as a library function
    returning a storage pointer; ProvenanceEngine recurses into the
    library (within internal_call_depth), the subsequent
    ``.members[role][msg.sender]`` chain produces a 2-key membership
    leaf with caller as one key, and AuthorityClassifier promotes it
    to caller_authority. No assembly-slot detection needed.

    Caveat: the membership leaf's set_descriptor.storage_var is the
    Slither SSA reference (e.g. "REF_1"), not the underlying mapping
    name. This means the writer-gate pass-2 won't find writers
    keyed to "REF_1". For now: the read-side classification is
    correct; the writer-gate enrichment for Diamond requires
    follow-up to map SSA references back to library-storage slots.
    """

    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;

        library LibDiamond {
            bytes32 constant DIAMOND_STORAGE_POSITION = keccak256("diamond.storage.acl");

            struct AclStorage {
                mapping(bytes32 => mapping(address => bool)) members;
            }

            function aclStorage() internal pure returns (AclStorage storage s) {
                bytes32 slot = DIAMOND_STORAGE_POSITION;
                assembly { s.slot := slot }
            }
        }

        contract C {
            function f(bytes32 role) external view {
                require(LibDiamond.aclStorage().members[role][msg.sender]);
            }
        }
    """,
    )
    contract = next(c for c in sl.contracts if c.name == "C")
    trees = _build_pipeline(contract)
    leaves = _all_leaves(trees["f(bytes32)"])
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "membership", f"expected membership leaf for diamond ACL, got {leaf['kind']}"
    assert leaf["authority_role"] == "caller_authority"


# ---------------------------------------------------------------------------
# 2. Bitwise role flags — (roles[msg.sender] & FLAG) != 0
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "Bitwise role flags: ``require((roles[msg.sender] & "
        "FLAG) != 0)``. Structurally a membership check — "
        "msg.sender is in the set of addresses with bit FLAG set "
        "in `roles`. Current pipeline produces a comparison leaf "
        "(business). "
        "FIX: extend the value-predicate recognition I already added "
        "for `map[k] == 1` (Maker wards) to also cover `(map[k] & "
        "V) != 0` and `map[k] >= V`. Add a `value_predicate` field "
        "to SetDescriptor that records {kind: 'eq'|'bitmask'|"
        "'threshold', value: ...}. Adapter then enumerates members "
        "where the predicate holds. Structural — no per-pattern "
        "adapter."
    ),
    strict=True,
)
def test_bitwise_flag_membership_classifies_caller_authority(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            address public ownerVar;
            mapping(address => uint256) public roles;
            uint256 constant MINTER_FLAG = 1;
            function setRole(address user, uint256 mask) external {
                require(msg.sender == ownerVar);
                roles[user] = mask;
            }
            function f() external view {
                require((roles[msg.sender] & MINTER_FLAG) != 0);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    leaves = _all_leaves(trees["f()"])
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "membership", f"expected membership leaf for bitwise flag, got {leaf['kind']}"
    assert leaf["authority_role"] == "caller_authority"


# ---------------------------------------------------------------------------
# 3. Custom M-of-N — counter map + threshold compare
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "Custom M-of-N: ``approve()`` increments approvals[txHash] "
        "(gated by isOwner check); ``execute()`` requires approvals"
        "[txHash] >= threshold. The execute() function is "
        "structurally an M-of-N gate — anyone can call, but only "
        "after M owners have approved. "
        "FIX: extend writer-gate analysis to recognize counter-map "
        "patterns: when ``map[k] >= threshold`` is read in fn F, "
        "and the same `map[k]` is incremented in fn G whose "
        "predicate has a caller_authority leaf, classify F's gate "
        "as `threshold_group` capability with M = threshold and "
        "signers = G's caller_authority set. Generic — covers any "
        "custom multisig, not just Safe."
    ),
    strict=True,
)
def test_custom_m_of_n_classifies_threshold_group(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        contract C {
            mapping(address => bool) public isOwner;
            mapping(bytes32 => uint256) public approvals;
            uint256 constant THRESHOLD = 2;
            function approve(bytes32 txHash) external {
                require(isOwner[msg.sender]);
                approvals[txHash] += 1;
            }
            function execute(bytes32 txHash) external view {
                require(approvals[txHash] >= THRESHOLD);
            }
        }
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    leaves = _all_leaves(trees["execute(bytes32)"])
    assert len(leaves) == 1
    leaf = leaves[0]
    # After the fix, the leaf should be a typed threshold-membership
    # leaf with authority_role=caller_authority. The capability
    # evaluator turns it into a threshold_group on the resolver side.
    assert leaf["authority_role"] == "caller_authority", (
        f"expected caller_authority for M-of-N execute gate, got {leaf['authority_role']}"
    )


# ---------------------------------------------------------------------------
# 4. EIP-1271 contract signatures — should classify as signature_auth
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "EIP-1271: ``IERC1271(signer).isValidSignature(hash, sig) "
        "== 0x1626ba7e``. The structural shape is an external_bool "
        "with magic-value comparison; the signer is a contract "
        "address (state var or parameter). Currently classifies as "
        "equality(external_call_result, constant) → business. "
        "FIX: detect external_call to ``isValidSignature(bytes32,"
        "bytes)`` returning bytes4, compared to 0x1626ba7e — emit "
        "signature_auth leaf with the signer as the operand. The "
        "resolver returns signature_witness with check_only "
        "confidence. Enumeration is intrinsically impossible (the "
        "signer contract decides arbitrarily); detection is "
        "generic via selector + magic-value pattern. Detection "
        "fires on selector match, not name match — so Slither's "
        "function_name=='isValidSignature' is a structural attribute "
        "of the selector hash, not identifier-name matching."
    ),
    strict=True,
)
def test_eip1271_classifies_signature_auth(tmp_path):
    sl = _compile(
        tmp_path,
        """
        pragma solidity ^0.8.19;
        interface IERC1271 {
            function isValidSignature(bytes32 hash, bytes memory signature) external view returns (bytes4);
        }
        contract C {
            address public signerContract;
            function f(bytes32 hash, bytes calldata sig) external view {
                require(IERC1271(signerContract).isValidSignature(hash, sig) == 0x1626ba7e);
            }
        }
    """,
    )
    contract = next(c for c in sl.contracts if c.name == "C")
    trees = _build_pipeline(contract)
    leaves = _all_leaves(trees["f(bytes32,bytes)"])
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["kind"] == "signature_auth", f"expected signature_auth, got {leaf['kind']}"
    assert leaf["authority_role"] == "caller_authority"


# ---------------------------------------------------------------------------
# 5. Computed-key membership — _members[keccak(role,msg.sender)]
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "Some contracts compute the storage key via a hash: "
        "``require(_authorized[keccak256(abi.encode(role, "
        "msg.sender))])``. Structurally a 1-key mapping read with "
        "the key derived from msg.sender + role. Currently classifies "
        "as 1-key membership business (the key source is `computed`, "
        "not msg_sender directly). "
        "FIX: in operand classification, when the key is a "
        "`computed` source whose taint includes msg.sender, treat "
        "it equivalently to a multi-key mapping with msg.sender as "
        "one of the conceptual keys. Generic via taint analysis — "
        "no per-pattern code."
    ),
    strict=True,
)
def test_hashed_key_membership_classifies_caller_authority(tmp_path):
    sl = _compile(
        tmp_path,
        """
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
    """,
    )
    contract = sl.contracts[0]
    trees = _build_pipeline(contract)
    leaves = _all_leaves(trees["f(bytes32)"])
    assert len(leaves) == 1
    leaf = leaves[0]
    assert leaf["authority_role"] == "caller_authority", (
        f"expected caller_authority for hashed-key membership, got {leaf['authority_role']}"
    )
