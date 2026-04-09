"""Adversarial effect label tests — simulating a malicious developer.

Names are randomized so the heuristic can't rely on naming conventions.
These test whether the detection works based on *what the code does*
(AST structure, data flow, IR) rather than *what things are called*.
"""

import random
import string
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slither.slither import Slither

from services.static.contract_analysis_pipeline.graph import build_permission_graph
from services.static.contract_analysis_pipeline.shared import _select_subject_contract
from services.static.contract_analysis_pipeline.summaries import _detect_access_control


def _rand(n: int = 8) -> str:
    """Generate a random lowercase identifier."""
    return "".join(random.choices(string.ascii_lowercase, k=n))


def _scaffold_and_analyze(solidity_source: str, contract_name: str = "Target") -> dict:
    with tempfile.TemporaryDirectory(prefix="psat_test_adv_") as tmp:
        project_dir = Path(tmp)
        sol_path = project_dir / f"{contract_name}.sol"
        sol_path.write_text(solidity_source)
        slither = Slither(str(sol_path))
        subject = _select_subject_contract(slither, contract_name)
        if subject is None:
            raise RuntimeError(f"Contract {contract_name} not found")
        pg = build_permission_graph(subject, project_dir)
        ac = _detect_access_control(subject, project_dir, pg)
        return {"access_control": ac}


def _get_function_labels(analysis: dict, function_name: str) -> set[str]:
    for pf in analysis.get("access_control", {}).get("privileged_functions", []):
        if pf.get("function", "").split("(")[0] == function_name:
            return set(pf.get("effect_labels", []))
    return set()


# =========================================================================
# 1. Randomized impl slot name + delegatecall fallback
#    The variable storing the implementation has a random name.
#    Detection must rely on: "writes var X" + "fallback reads X and delegatecalls"
# =========================================================================


def test_random_impl_slot_with_delegatecall():
    slot_name = f"_{_rand()}"
    setter_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address private {slot_name};
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {setter_name}(address a) external onlyOwner {{ {slot_name} = a; }}
    fallback() external payable {{
        address t = {slot_name};
        assembly {{ calldatacopy(0,0,calldatasize()) let r := delegatecall(gas(),t,0,calldatasize(),0,0) returndatacopy(0,0,returndatasize()) switch r case 0 {{ revert(0,returndatasize()) }} default {{ return(0,returndatasize()) }} }}
    }}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, setter_name)
    assert "implementation_update" in labels, (
        f"Random impl slot '{slot_name}', setter '{setter_name}': expected implementation_update, got {labels}"
    )


# =========================================================================
# 2. Randomized pause variable name
#    A bool with a random name gates a modifier, and a function flips it.
#    Detection must rely on: "writes a bool that gates other functions"
#    (Currently we expanded the name list, but random names will fail.)
# =========================================================================


def test_random_pause_variable():
    var_name = f"_{_rand()}"
    pause_fn = _rand()
    unpause_fn = _rand()
    guarded_fn = _rand()
    modifier_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    bool public {var_name};
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    modifier {modifier_name}() {{ require(!{var_name}); _; }}
    function {pause_fn}() external onlyOwner {{ {var_name} = true; }}
    function {unpause_fn}() external onlyOwner {{ {var_name} = false; }}
    function {guarded_fn}() external payable {modifier_name} {{ }}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, pause_fn)
    assert "pause_toggle" in labels, (
        f"Random pause var '{var_name}', fn '{pause_fn}': expected pause_toggle, got {labels}"
    )


# =========================================================================
# 3. Raw storage slot write via assembly (no named variable at all)
#    Malicious dev uses sstore to a hardcoded slot that the fallback
#    reads via sload and delegatecalls to.
# =========================================================================


def test_raw_assembly_storage_slot_impl():
    setter_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {setter_name}(address newImpl) external onlyOwner {{
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
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, setter_name)
    assert "implementation_update" in labels, (
        f"Assembly sstore impl setter '{setter_name}': expected implementation_update, got {labels}"
    )


# =========================================================================
# 4. ETH drain via selfdestruct (sends all ETH to an address)
#    Not a .call{value:} — uses selfdestruct as a value transfer mechanism.
# =========================================================================


def test_selfdestruct_value_drain():
    fn_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn_name}(address payable to) external onlyOwner {{
        selfdestruct(to);
    }}
    receive() external payable {{}}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, fn_name)
    assert "selfdestruct_capability" in labels, (
        f"selfdestruct fn '{fn_name}': expected selfdestruct_capability, got {labels}"
    )


# =========================================================================
# 5. Randomized function name for cross-contract mint
#    Calls token.mint() but the calling function has a random name.
#    Our fix checks effect_targets for .mint — this should still work.
# =========================================================================


def test_random_named_cross_contract_mint():
    fn_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IMintable {{ function mint(address to, uint256 amount) external; }}
contract Target {{
    address public owner;
    IMintable public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn_name}(address to, uint256 amount) external onlyOwner {{ token.mint(to, amount); }}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, fn_name)
    assert "mint" in labels, f"Cross-contract mint fn '{fn_name}': expected mint, got {labels}"


# =========================================================================
# 6. Cross-contract mint with randomized interface method name
#    The external function is NOT called "mint" — it's called something random.
#    This should fail because we only match .mint() in effect_targets.
# =========================================================================


@pytest.mark.xfail(reason="Randomized external mint function name bypasses .mint() target check")
def test_random_interface_mint_name():
    fn_name = _rand()
    mint_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
interface IToken {{
    function {mint_name}(address to, uint256 amount) external;
    function totalSupply() external view returns (uint256);
}}
contract Target {{
    address public owner;
    IToken public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn_name}(address to, uint256 amount) external onlyOwner {{
        uint256 before = token.totalSupply();
        token.{mint_name}(to, amount);
        require(token.totalSupply() > before, "supply must increase");
    }}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, fn_name)
    assert "mint" in labels, f"Randomized interface mint '{mint_name}', fn '{fn_name}': expected mint, got {labels}"


# =========================================================================
# 7. Value transfer hidden behind an internal helper with random name
#    The external function calls an internal function with a random name,
#    which does the actual .call{value:}.
# =========================================================================


def test_value_transfer_via_random_internal_helper():
    fn_name = _rand()
    helper_name = f"_{_rand()}"
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn_name}(address payable to, uint256 amt) external onlyOwner {{
        {helper_name}(to, amt);
    }}
    function {helper_name}(address payable to, uint256 amt) internal {{
        (bool ok,) = to.call{{value: amt}}("");
        require(ok);
    }}
    receive() external payable {{}}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, fn_name)
    assert "asset_send" in labels, (
        f"Value transfer via internal helper '{helper_name}', fn '{fn_name}': expected asset_send, got {labels}"
    )


# =========================================================================
# 8. ERC20 transfer via abi.encodeWithSelector (low-level obfuscation)
#    Instead of calling token.transfer(), uses address.call with encoded selector.
# =========================================================================


def test_erc20_transfer_via_encode_selector():
    fn_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public owner;
    address public token;
    modifier onlyOwner() {{ require(msg.sender == owner); _; }}
    function {fn_name}(address to, uint256 amount) external onlyOwner {{
        (bool ok,) = token.call(abi.encodeWithSelector(0xa9059cbb, to, amount));
        require(ok);
    }}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, fn_name)
    assert "asset_send" in labels, f"ERC20 via encodeWithSelector fn '{fn_name}': expected asset_send, got {labels}"


# =========================================================================
# 9. Ownership transfer with randomized variable name
#    The "owner" variable has a random name, so the target-name heuristic misses it.
#    But Slither should still detect the ownership pattern via the modifier.
# =========================================================================


def test_random_owner_variable_name():
    var_name = f"_{_rand()}"
    fn_name = _rand()
    source = f"""
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract Target {{
    address public {var_name};
    constructor() {{ {var_name} = msg.sender; }}
    modifier auth() {{ require(msg.sender == {var_name}); _; }}
    function {fn_name}(address newAdmin) external auth {{ {var_name} = newAdmin; }}
}}
"""
    analysis = _scaffold_and_analyze(source)
    labels = _get_function_labels(analysis, fn_name)
    assert "ownership_transfer" in labels, (
        f"Random owner var '{var_name}', fn '{fn_name}': expected ownership_transfer, got {labels}"
    )
