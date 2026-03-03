import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.privilege_analysis import _slither_scan, analyze_privileges


# ---------------------------------------------------------------------------
# Helpers — create minimal Foundry projects Slither can compile
# ---------------------------------------------------------------------------

def _make_foundry_project(tmp_path: Path, source: str, solc: str = "0.8.20") -> Path:
    """Create a minimal Foundry project with one source file."""
    src_dir = tmp_path / "src"
    src_dir.mkdir(parents=True)
    (src_dir / "Contract.sol").write_text(source)
    (tmp_path / "foundry.toml").write_text(
        f'[profile.default]\nsrc = "src"\nout = "out"\nlibs = ["lib"]\n'
        f'solc_version = "{solc}"\n'
    )
    return tmp_path


# ---------------------------------------------------------------------------
# Synthetic Solidity sources
# ---------------------------------------------------------------------------

OWNABLE_CONTRACT = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract MyToken {
    address public owner;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function mint(address to, uint256 amount) external onlyOwner {
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        return true;
    }

    function setFee(uint256 fee) public onlyOwner {
    }

    function balanceOf(address a) external view returns (uint256) {
        return 0;
    }
}
"""

INLINE_AUTH_CONTRACT = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract InlineAuth {
    address public owner;
    address public admin;

    constructor() {
        owner = msg.sender;
    }

    function withdraw(uint256 amount) external {
        require(msg.sender == owner, "not owner");
        payable(msg.sender).transfer(amount);
    }

    function setAdmin(address _admin) external {
        if (msg.sender != owner) revert("unauthorized");
        admin = _admin;
    }

    function emergencyAction() external {
        require(msg.sender == owner || msg.sender == admin, "not authorized");
    }

    function deposit() external payable {}
}
"""

NO_ACCESS_CONTROL = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract SimpleStorage {
    uint256 public value;

    function setValue(uint256 _value) external {
        value = _value;
    }

    function getValue() external view returns (uint256) {
        return value;
    }
}
"""


# ---------------------------------------------------------------------------
# Slither-only tests (no LLM)
# ---------------------------------------------------------------------------

class TestSlitherScan:
    def test_modifier_based_ownership(self, tmp_path):
        project = _make_foundry_project(tmp_path, OWNABLE_CONTRACT)
        result = _slither_scan(project)

        # Should detect onlyOwner modifier
        mod_names = [m["name"] for m in result["modifier_declarations"]]
        assert "onlyOwner" in mod_names

        # Should detect owner state variable
        assert "owner" in result["owner_state_variables"]

        # Should detect gated functions
        gated_names = [g["function"] for g in result["gated_functions"]]
        assert any("mint" in n for n in gated_names)
        assert any("setFee" in n for n in gated_names)

        # transfer is not gated but is state-changing
        ungated = result["ungated_state_changing_functions"]
        assert any("transfer" in u for u in ungated)

        # view functions should not appear in ungated
        assert not any("balanceOf" in u for u in ungated)

    def test_inline_access_control(self, tmp_path):
        project = _make_foundry_project(tmp_path, INLINE_AUTH_CONTRACT)
        result = _slither_scan(project)

        # All three gated functions should be detected via is_protected
        gated_names = [g["function"] for g in result["gated_functions"]]
        assert any("withdraw" in n for n in gated_names)
        assert any("setAdmin" in n for n in gated_names)
        assert any("emergencyAction" in n for n in gated_names)

        # deposit() has no access control
        ungated = result["ungated_state_changing_functions"]
        assert any("deposit" in u for u in ungated)

        # Should detect owner and admin as privileged addresses
        assert "owner" in result["privileged_roles"]
        assert "admin" in result["privileged_roles"]

        # Should have sender_checks with the specific comparisons
        check_funcs = [sc["function"] for sc in result["sender_checks"]]
        assert "withdraw" in check_funcs
        assert "setAdmin" in check_funcs

    def test_no_access_control(self, tmp_path):
        project = _make_foundry_project(tmp_path, NO_ACCESS_CONTROL)
        result = _slither_scan(project)

        assert result["access_control_model"] == ["none_detected"]
        assert result["gated_functions"] == []
        assert any("setValue" in u for u in result["ungated_state_changing_functions"])

    def test_view_functions_not_flagged(self, tmp_path):
        project = _make_foundry_project(tmp_path, NO_ACCESS_CONTROL)
        result = _slither_scan(project)

        ungated = result["ungated_state_changing_functions"]
        assert not any("getValue" in u for u in ungated)


# ---------------------------------------------------------------------------
# Integration test with monkeypatched LLM
# ---------------------------------------------------------------------------

class TestAnalyzePrivileges:
    @patch("services.privilege_analysis.chat")
    def test_full_analysis_writes_outputs(self, mock_chat, tmp_path):
        mock_chat.return_value = json.dumps({
            "access_control_model": ["custom"],
            "privileged_roles": ["owner"],
            "gated_functions": [
                {"function": "mint", "modifiers": ["onlyOwner"],
                 "risk_level": "high", "risk_reason": "Can create tokens"},
            ],
            "ungated_state_changing_functions": [
                {"function": "transfer", "risk_level": "low",
                 "risk_reason": "Standard transfer"},
            ],
            "custom_patterns": [],
            "summary": "Simple owner-gated contract.",
        })

        project = _make_foundry_project(tmp_path, OWNABLE_CONTRACT)
        result = analyze_privileges(project)

        # LLM enrichment should be merged in
        assert result.get("summary") == "Simple owner-gated contract."

        # Files should be written
        assert (project / "privilege_analysis.json").exists()
        assert (project / "privilege_analysis.md").exists()

        md = (project / "privilege_analysis.md").read_text()
        assert "Privilege" in md

    @patch("services.privilege_analysis.chat")
    def test_llm_failure_falls_back_to_slither(self, mock_chat, tmp_path):
        mock_chat.side_effect = RuntimeError("LLM unavailable")

        project = _make_foundry_project(tmp_path, INLINE_AUTH_CONTRACT)
        result = analyze_privileges(project)

        # Should still have Slither findings
        gated_names = [g["function"] for g in result["gated_functions"]]
        assert any("withdraw" in n for n in gated_names)
        assert "owner" in result["privileged_roles"]

    @patch("services.privilege_analysis.chat")
    def test_no_sources(self, mock_chat, tmp_path):
        result = analyze_privileges(tmp_path)
        assert result.get("error") == "No Solidity source files found"
        mock_chat.assert_not_called()
