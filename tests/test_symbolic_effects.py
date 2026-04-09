"""Symbolic effect detection tests.

These test a two-pass approach to effect labeling:
  Pass 1: Classify each state variable by its ROLE (how it's used),
           not its name.
  Pass 2: Label each privileged function by what roles it writes to.

Every test uses fully randomized names to ensure zero name dependence.
Tests are grouped by the security question they answer.
"""

import random
import string
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slither.slither import Slither

from services.static.contract_analysis_pipeline.graph import build_permission_graph
from services.static.contract_analysis_pipeline.shared import _select_subject_contract
from services.static.contract_analysis_pipeline.summaries import _detect_access_control


def _rand(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


def _analyze(source: str, name: str = "Target"):
    with tempfile.TemporaryDirectory(prefix="psat_test_sym_") as tmp:
        p = Path(tmp) / f"{name}.sol"
        p.write_text(source)
        slither = Slither(str(p))
        subject = _select_subject_contract(slither, name)
        if subject is None:
            raise RuntimeError(f"Contract {name} not found")
        pg = build_permission_graph(subject, Path(tmp))
        return _detect_access_control(subject, Path(tmp), pg)


def _labels(ac, fn_name: str) -> set[str]:
    for pf in ac.get("privileged_functions", []):
        if pf["function"].split("(")[0] == fn_name:
            return set(pf.get("effect_labels", []))
    return set()


# =========================================================================
# Q1: CAN VALUE LEAVE THE CONTRACT?
#
# Storage role: "balance store" — mapping(address => uint256) that decreases,
# or ETH sent via any mechanism.
# =========================================================================


def test_q1_eth_leaves_via_call_value():
    """ETH leaves via .call{value:} with all random names."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address payable to, uint256 amt) external onlyOwner {{
        (bool ok,) = to.call{{value: amt}}("");
        require(ok);
    }}
    receive() external payable {{}}
}}
"""
    ac = _analyze(source)
    assert "asset_send" in _labels(ac, fn)


def test_q1_erc20_leaves_via_transfer():
    """ERC20 leaves via token.transfer() with random function name."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IERC20 {{ function transfer(address, uint256) external returns (bool); function balanceOf(address) external view returns (uint256); }}
contract Target {{
    address public owner;
    IERC20 public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address to) external onlyOwner {{ token.transfer(to, token.balanceOf(address(this))); }}
}}
"""
    ac = _analyze(source)
    assert "asset_send" in _labels(ac, fn)


def test_q1_erc20_leaves_via_encoded_selector():
    """ERC20 leaves via abi.encodeWithSelector(transfer) — fully obfuscated."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    address public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address to, uint256 amt) external onlyOwner {{
        (bool ok,) = token.call(abi.encodeWithSelector(0xa9059cbb, to, amt));
        require(ok);
    }}
}}
"""
    ac = _analyze(source)
    assert "asset_send" in _labels(ac, fn)


def test_q1_eth_leaves_via_selfdestruct():
    """All ETH drained via selfdestruct."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address payable to) external onlyOwner {{ selfdestruct(to); }}
    receive() external payable {{}}
}}
"""
    ac = _analyze(source)
    assert "selfdestruct_capability" in _labels(ac, fn)


def test_q1_value_leaves_via_internal_helper():
    """Value leaves through a randomly named internal function."""
    fn = _rand()
    helper = f"_{_rand()}"
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address payable to, uint256 amt) external onlyOwner {{ {helper}(to, amt); }}
    function {helper}(address payable to, uint256 amt) internal {{
        (bool ok,) = to.call{{value: amt}}("");
        require(ok);
    }}
    receive() external payable {{}}
}}
"""
    ac = _analyze(source)
    assert "asset_send" in _labels(ac, fn)


# =========================================================================
# Q2: CAN DEPOSITS/WITHDRAWALS BE BLOCKED?
#
# Storage role: "guard variable" — a bool that a modifier reads, and that
# modifier gates other functions. Writing this bool = pause_toggle.
# =========================================================================


def test_q2_random_bool_gates_functions():
    """A randomly named bool in a randomly named modifier blocks a function."""
    var = f"_{_rand()}"
    mod = _rand()
    stop_fn = _rand()
    resume_fn = _rand()
    guarded_fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    bool public {var};
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    modifier {mod}() {{ require(!{var}); _; }}
    function {stop_fn}() external onlyOwner {{ {var} = true; }}
    function {resume_fn}() external onlyOwner {{ {var} = false; }}
    function {guarded_fn}() external payable {mod} {{ }}
}}
"""
    ac = _analyze(source)
    assert "pause_toggle" in _labels(ac, stop_fn)
    assert "pause_toggle" in _labels(ac, resume_fn)


def test_q2_inverted_bool_guard():
    """Guard checks require(active) instead of require(!paused) — same pattern, inverted."""
    var = f"_{_rand()}"
    mod = _rand()
    disable_fn = _rand()
    guarded_fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    bool public {var};
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    modifier {mod}() {{ require({var}); _; }}
    function {disable_fn}() external onlyOwner {{ {var} = false; }}
    function {guarded_fn}() external payable {mod} {{ }}
}}
"""
    ac = _analyze(source)
    assert "pause_toggle" in _labels(ac, disable_fn)


# =========================================================================
# Q3: CAN NEW VALUE BE CREATED? (minting)
#
# Detected via: internal _mint/mint calls, cross-contract .mint() calls,
# or known selectors.
# =========================================================================


def test_q3_internal_mint_random_names():
    """Internal mint with random function names."""
    fn = _rand()
    # We need the internal to actually be named _mint or mint for current detection
    # This tests whether random internal names work
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    mapping(address => uint256) public balances;
    uint256 public totalSupply;
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address to, uint256 amt) external onlyOwner {{ _mint(to, amt); }}
    function _mint(address to, uint256 amt) internal {{ balances[to] += amt; totalSupply += amt; }}
}}
"""
    ac = _analyze(source)
    assert "mint" in _labels(ac, fn)


def test_q3_cross_contract_mint():
    """Calls token.mint() — external function named mint on another contract."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IMintable {{ function mint(address to, uint256 amount) external; }}
contract Target {{
    address public owner;
    IMintable public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address to, uint256 amt) external onlyOwner {{ token.mint(to, amt); }}
}}
"""
    ac = _analyze(source)
    assert "mint" in _labels(ac, fn)


def test_q3_mint_via_encoded_selector():
    """Mint via abi.encodeWithSelector(0x40c10f19) — the mint(address,uint256) selector."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    address public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address to, uint256 amt) external onlyOwner {{
        (bool ok,) = token.call(abi.encodeWithSelector(0x40c10f19, to, amt));
        require(ok);
    }}
}}
"""
    ac = _analyze(source)
    assert "mint" in _labels(ac, fn)


# =========================================================================
# Q4: CAN THE CODE CHANGE? (implementation update)
#
# Storage role: "delegation target" — address read in fallback, passed to
# delegatecall. Writing this = implementation_update.
# =========================================================================


def test_q4_random_impl_slot_delegatecall():
    """Random variable name stores impl address, fallback delegatecalls to it."""
    var = f"_{_rand()}"
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address private {var};
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address a) external onlyOwner {{ {var} = a; }}
    fallback() external payable {{
        address t = {var};
        assembly {{ calldatacopy(0,0,calldatasize()) let r := delegatecall(gas(),t,0,calldatasize(),0,0) returndatacopy(0,0,returndatasize()) switch r case 0 {{ revert(0,returndatasize()) }} default {{ return(0,returndatasize()) }} }}
    }}
}}
"""
    ac = _analyze(source)
    assert "implementation_update" in _labels(ac, fn)


def test_q4_assembly_sstore_sload_delegatecall():
    """Pure assembly: sstore a slot, fallback sloads it and delegatecalls."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address newImpl) external onlyOwner {{
        bytes32 slot = 0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef;
        assembly {{ sstore(slot, newImpl) }}
    }}
    fallback() external payable {{
        bytes32 slot = 0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef;
        assembly {{
            let impl := sload(slot)
            calldatacopy(0,0,calldatasize())
            let r := delegatecall(gas(),impl,0,calldatasize(),0,0)
            returndatacopy(0,0,returndatasize())
            switch r case 0 {{ revert(0,returndatasize()) }} default {{ return(0,returndatasize()) }}
        }}
    }}
}}
"""
    ac = _analyze(source)
    assert "implementation_update" in _labels(ac, fn)


# =========================================================================
# Q5: CAN WHO'S IN CHARGE CHANGE? (ownership transfer)
#
# Storage role: "owner/admin" — address compared to msg.sender in a modifier.
# Writing this = ownership_transfer.
# =========================================================================


def test_q5_random_owner_var():
    """Random variable name used as msg.sender check in modifier, then written."""
    var = f"_{_rand()}"
    mod = _rand()
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public {var};
    constructor() {{ {var} = msg.sender; }}
    modifier {mod}() {{ require(msg.sender == {var}); _; }}
    function {fn}(address newAdmin) external {mod} {{ {var} = newAdmin; }}
}}
"""
    ac = _analyze(source)
    assert "ownership_transfer" in _labels(ac, fn)


def test_q5_two_step_ownership():
    """Two-step ownership: nominate + accept, both with random names."""
    admin_var = f"_{_rand()}"
    pending_var = f"_{_rand()}"
    nominate_fn = _rand()
    accept_fn = _rand()
    mod = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public {admin_var};
    address public {pending_var};
    modifier {mod}() {{ require(msg.sender == {admin_var}); _; }}
    function {nominate_fn}(address a) external {mod} {{ {pending_var} = a; }}
    function {accept_fn}() external {{
        require(msg.sender == {pending_var});
        {admin_var} = msg.sender;
        {pending_var} = address(0);
    }}
}}
"""
    ac = _analyze(source)
    # The accept function writes the admin var which is used in the modifier
    assert "ownership_transfer" in _labels(ac, accept_fn)


# =========================================================================
# Q6: CAN THE RULES CHANGE? (authority/hook update)
#
# Storage role: "authority reference" — address that is CALLED (not just
# compared) within a modifier body. Writing this = authority_update.
#
# Storage role: "hook reference" — address called during transfer-like
# function execution. Writing this = hook_update.
# =========================================================================


def test_q6_random_authority_var():
    """Random variable stores authority contract, called in modifier for auth checks."""
    auth_var = f"_{_rand()}"
    mod = _rand()
    set_fn = _rand()
    guarded_fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IAuth {{ function canCall(address, address, bytes4) external view returns (bool); }}
contract Target {{
    address public owner;
    IAuth public {auth_var};
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    modifier {mod}(bytes4 sig) {{ require(address({auth_var}) == address(0) || {auth_var}.canCall(msg.sender, address(this), sig)); _; }}
    function {set_fn}(IAuth a) external onlyOwner {{ {auth_var} = a; }}
    function {guarded_fn}() external {mod}(msg.sig) {{ }}
}}
"""
    ac = _analyze(source)
    assert "authority_update" in _labels(ac, set_fn)


def test_q6_random_hook_var():
    """Random variable stores a hook contract called during transfers."""
    hook_var = f"_{_rand()}"
    set_fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IHook {{ function beforeTransfer(address, address, uint256) external; }}
contract Target {{
    mapping(address => uint256) public balances;
    address public owner;
    IHook public {hook_var};
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {set_fn}(IHook h) external onlyOwner {{ {hook_var} = h; }}
    function transfer(address to, uint256 amt) external {{
        if (address({hook_var}) != address(0)) {hook_var}.beforeTransfer(msg.sender, to, amt);
        balances[msg.sender] -= amt;
        balances[to] += amt;
    }}
}}
"""
    ac = _analyze(source)
    assert "hook_update" in _labels(ac, set_fn)


# =========================================================================
# COMPOUND SCENARIOS — multiple effects in one function
# =========================================================================


def test_compound_drain_and_selfdestruct():
    """Function drains ETH then selfdestructs — should get both labels."""
    fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn}(address payable to) external onlyOwner {{
        (bool ok,) = to.call{{value: address(this).balance}}("");
        require(ok);
        selfdestruct(to);
    }}
    receive() external payable {{}}
}}
"""
    ac = _analyze(source)
    labels = _labels(ac, fn)
    assert "asset_send" in labels
    assert "selfdestruct_capability" in labels


def test_compound_pause_and_ownership():
    """One function pauses AND transfers ownership — should get both labels."""
    bool_var = f"_{_rand()}"
    admin_var = f"_{_rand()}"
    mod_auth = _rand()
    mod_guard = _rand()
    fn = _rand()
    guarded_fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    bool public {bool_var};
    address public {admin_var};
    constructor() {{ {admin_var} = msg.sender; }}
    modifier {mod_auth}() {{ require(msg.sender == {admin_var}); _; }}
    modifier {mod_guard}() {{ require(!{bool_var}); _; }}
    function {fn}(address newAdmin) external {mod_auth} {{
        {bool_var} = true;
        {admin_var} = newAdmin;
    }}
    function {guarded_fn}() external payable {mod_guard} {{ }}
}}
"""
    ac = _analyze(source)
    labels = _labels(ac, fn)
    assert "pause_toggle" in labels
    assert "ownership_transfer" in labels


# =========================================================================
# Q6 RECURSIVE: Authority/hook hidden behind internal helpers
# =========================================================================


def test_q6_recursive_authority():
    """Auth check hidden behind internal helper, setter hidden behind internal helper."""
    auth_var = f"_{_rand()}"
    mod = _rand()
    set_fn = _rand()
    helper = f"_{_rand()}"
    auth_helper = f"_{_rand()}"
    guarded_fn = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IAuth {{ function canCall(address, address, bytes4) external view returns (bool); }}
contract Target {{
    address public owner;
    IAuth public {auth_var};
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {auth_helper}(bytes4 sig) internal view returns (bool) {{
        return address({auth_var}) == address(0) || {auth_var}.canCall(msg.sender, address(this), sig);
    }}
    modifier {mod}(bytes4 sig) {{ require({auth_helper}(sig)); _; }}
    function {helper}(IAuth a) internal {{ {auth_var} = a; }}
    function {set_fn}(IAuth a) external onlyOwner {{ {helper}(a); }}
    function {guarded_fn}() external {mod}(msg.sig) {{ }}
}}
"""
    ac = _analyze(source)
    assert "authority_update" in _labels(ac, set_fn)


def test_q6_recursive_hook():
    """Hook call hidden behind internal helper, setter hidden behind internal helper."""
    hook_var = f"_{_rand()}"
    set_fn = _rand()
    helper = f"_{_rand()}"
    call_helper = f"_{_rand()}"
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IHook {{ function beforeTransfer(address, address, uint256) external; }}
contract Target {{
    mapping(address => uint256) public balances;
    address public owner;
    IHook public {hook_var};
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {call_helper}(address from, address to, uint256 amt) internal {{
        if (address({hook_var}) != address(0)) {hook_var}.beforeTransfer(from, to, amt);
    }}
    function {helper}(IHook h) internal {{ {hook_var} = h; }}
    function {set_fn}(IHook h) external onlyOwner {{ {helper}(h); }}
    function transfer(address to, uint256 amt) external {{
        {call_helper}(msg.sender, to, amt);
        balances[msg.sender] -= amt;
        balances[to] += amt;
    }}
}}
"""
    ac = _analyze(source)
    assert "hook_update" in _labels(ac, set_fn)
