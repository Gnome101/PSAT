import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.pausability_analysis import _slither_scan, analyze_pausability


# ---------------------------------------------------------------------------
# Helpers
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

MODIFIER_PAUSABLE = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract PausableToken {
    address public owner;
    bool public paused;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    modifier whenNotPaused() {
        require(!paused, "paused");
        _;
    }

    function pause() external onlyOwner {
        paused = true;
    }

    function unpause() external onlyOwner {
        paused = false;
    }

    function transfer(address to, uint256 amount) external whenNotPaused {
    }

    function approve(address spender, uint256 amount) external whenNotPaused {
    }

    function balanceOf(address a) external view returns (uint256) {
        return 0;
    }
}
"""

INLINE_PAUSABLE = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract InlinePause {
    address public owner;
    bool public paused;

    function pause() external {
        require(msg.sender == owner, "not owner");
        paused = true;
    }

    function unpause() external {
        require(msg.sender == owner, "not owner");
        paused = false;
    }

    function deposit(uint256 amount) external {
        require(!paused, "paused");
    }

    function withdraw(uint256 amount) external {
        if (paused) revert("paused");
    }
}
"""

NOT_PAUSABLE = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract SimpleToken {
    function transfer(address to, uint256 amount) external returns (bool) {
        return true;
    }
}
"""


# ---------------------------------------------------------------------------
# Slither-only tests
# ---------------------------------------------------------------------------

class TestSlitherScan:
    def test_modifier_based_pause(self, tmp_path):
        project = _make_foundry_project(tmp_path, MODIFIER_PAUSABLE)
        result = _slither_scan(project)

        assert result["is_pausable"] is True
        assert "paused" in result["pause_state_variables"]

        # pause() and unpause() should be detected as pause functions
        assert any("pause" in k for k in result["pause_functions"])
        assert any("unpause" in k for k in result["pause_functions"])

        # pause functions should show access control
        for name, info in result["pause_functions"].items():
            if "unpause" not in name and "pause" in name:
                assert info["access_control"] != ["unrestricted"]

        # transfer and approve should be affected
        affected_names = [af["function"] for af in result["affected_functions"]]
        assert any("transfer" in n for n in affected_names)
        assert any("approve" in n for n in affected_names)

    def test_inline_pause_checks(self, tmp_path):
        project = _make_foundry_project(tmp_path, INLINE_PAUSABLE)
        result = _slither_scan(project)

        assert result["is_pausable"] is True
        assert "paused" in result["pause_state_variables"]

        # deposit and withdraw should be detected as affected (inline check)
        affected_names = [af["function"] for af in result["affected_functions"]]
        assert any("deposit" in n for n in affected_names)
        assert any("withdraw" in n for n in affected_names)

        # Should detect inline guard type
        inline_guards = [
            af for af in result["affected_functions"]
            if af.get("guard_type") == "inline"
        ]
        assert len(inline_guards) >= 2

    def test_not_pausable(self, tmp_path):
        project = _make_foundry_project(tmp_path, NOT_PAUSABLE)
        result = _slither_scan(project)

        assert result["is_pausable"] is False
        assert result["pause_functions"] == {}
        assert result["affected_functions"] == []


# ---------------------------------------------------------------------------
# Integration test with monkeypatched LLM
# ---------------------------------------------------------------------------

class TestAnalyzePausability:
    @patch("services.pausability_analysis.chat")
    def test_full_analysis(self, mock_chat, tmp_path):
        mock_chat.return_value = json.dumps({
            "is_pausable": True,
            "pause_mechanism": "Bool state variable with modifier",
            "pause_functions": {
                "pause()": {"access_control": ["onlyOwner"], "description": "Pauses"},
                "unpause()": {"access_control": ["onlyOwner"], "description": "Unpauses"},
            },
            "affected_functions": [
                {"function": "transfer", "guard_type": "modifier", "impact": "Blocks transfers"},
            ],
            "custom_pause_patterns": [],
            "impact_summary": "Transfers and approvals blocked when paused.",
            "risk_assessment": "Owner can unilaterally pause.",
        })

        project = _make_foundry_project(tmp_path, MODIFIER_PAUSABLE)
        result = analyze_pausability(project)

        assert result["is_pausable"] is True
        assert (project / "pausability_analysis.json").exists()
        assert (project / "pausability_analysis.md").exists()

    @patch("services.pausability_analysis.chat")
    def test_llm_failure_falls_back_to_slither(self, mock_chat, tmp_path):
        mock_chat.side_effect = RuntimeError("LLM unavailable")

        project = _make_foundry_project(tmp_path, MODIFIER_PAUSABLE)
        result = analyze_pausability(project)

        assert result["is_pausable"] is True
        affected_names = [af["function"] for af in result["affected_functions"]]
        assert any("transfer" in n for n in affected_names)

    @patch("services.pausability_analysis.chat")
    def test_not_pausable_analysis(self, mock_chat, tmp_path):
        mock_chat.return_value = json.dumps({
            "is_pausable": False,
            "pause_mechanism": "None",
            "pause_functions": {},
            "affected_functions": [],
            "custom_pause_patterns": [],
            "impact_summary": "No pause mechanism.",
            "risk_assessment": "N/A",
        })

        project = _make_foundry_project(tmp_path, NOT_PAUSABLE)
        result = analyze_pausability(project)

        assert result["is_pausable"] is False
