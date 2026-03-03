import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.timelock_analysis import _slither_scan, analyze_timelocks


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

CUSTOM_TIMELOCK = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract CustomTimelock {
    address public admin;
    uint256 public delay;
    mapping(bytes32 => uint256) public queuedTransactions;

    modifier onlyAdmin() {
        require(msg.sender == admin, "not admin");
        _;
    }

    function queueTransaction(address target, uint256 value, bytes calldata data)
        external onlyAdmin
    {
        bytes32 txHash = keccak256(abi.encode(target, value, data));
        queuedTransactions[txHash] = block.timestamp + delay;
    }

    function executeTransaction(address target, uint256 value, bytes calldata data)
        external onlyAdmin
    {
        bytes32 txHash = keccak256(abi.encode(target, value, data));
        require(block.timestamp >= queuedTransactions[txHash], "not ready");
        delete queuedTransactions[txHash];
        (bool success,) = target.call{value: value}(data);
        require(success, "tx failed");
    }

    function setDelay(uint256 _delay) external onlyAdmin {
        delay = _delay;
    }

    function cancel(bytes32 txHash) external onlyAdmin {
        delete queuedTransactions[txHash];
    }
}
"""

DELAY_PATTERN = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract DelayGuard {
    address public owner;
    uint256 public unlockTime;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function lock(uint256 duration) external onlyOwner {
        unlockTime = block.timestamp + duration;
    }

    function withdraw(uint256 amount) external onlyOwner {
        require(block.timestamp >= unlockTime, "still locked");
        payable(msg.sender).transfer(amount);
    }

    function setOwner(address _owner) external onlyOwner {
        owner = _owner;
    }
}
"""

NO_TIMELOCK = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract DirectAdmin {
    address public owner;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function setFee(uint256 fee) external onlyOwner {
    }

    function withdraw(address to, uint256 amount) external onlyOwner {
        payable(to).transfer(amount);
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
}
"""


# ---------------------------------------------------------------------------
# Slither-only tests
# ---------------------------------------------------------------------------

class TestSlitherScan:
    def test_custom_timelock_detected(self, tmp_path):
        project = _make_foundry_project(tmp_path, CUSTOM_TIMELOCK)
        result = _slither_scan(project)

        assert result["has_timelock"] is True
        assert result["timelock_type"] == "custom"

        # queuedTransactions should be a time storage variable
        assert "queuedTransactions" in result["time_storage_variables"]

        # delay should be detected as a delay variable
        assert "delay" in result["delay_variables"]

        # queueTransaction should be a scheduling function
        sched_names = [sf["function"] for sf in result["scheduling_functions"]]
        assert any("queueTransaction" in n for n in sched_names)

        # executeTransaction should be a time-gated function
        gated_names = [gf["function"] for gf in result["time_gated_functions"]]
        assert any("executeTransaction" in n for n in gated_names)

        # setDelay should be a delay modifier function
        delay_mod_names = [df["function"] for df in result["delay_modifier_functions"]]
        assert any("setDelay" in n for n in delay_mod_names)

    def test_delay_pattern_detected(self, tmp_path):
        project = _make_foundry_project(tmp_path, DELAY_PATTERN)
        result = _slither_scan(project)

        assert result["has_timelock"] is True
        # Has time-gated function but no scheduling function that writes
        # block.timestamp + delay to a storage var in the same way,
        # so type depends on what lock() does
        assert result["timelock_type"] in ("custom", "delay_pattern")

        # unlockTime should be a time storage variable
        assert "unlockTime" in result["time_storage_variables"]

        # withdraw should be time-gated
        gated_names = [gf["function"] for gf in result["time_gated_functions"]]
        assert any("withdraw" in n for n in gated_names)

        # setOwner should show up as admin without timelock
        admin_names = [af["function"] for af in result["admin_functions_without_timelock"]]
        assert any("setOwner" in n for n in admin_names)

    def test_no_timelock(self, tmp_path):
        project = _make_foundry_project(tmp_path, NO_TIMELOCK)
        result = _slither_scan(project)

        assert result["has_timelock"] is False
        assert result["timelock_type"] == "none"
        assert result["timelock_contracts"] == []
        assert result["scheduling_functions"] == []
        assert result["time_gated_functions"] == []

        # Protected functions should show up as admin without timelock
        admin_names = [af["function"] for af in result["admin_functions_without_timelock"]]
        assert any("setFee" in n for n in admin_names)
        assert any("withdraw" in n for n in admin_names)

    def test_no_access_control_no_timelock(self, tmp_path):
        project = _make_foundry_project(tmp_path, NO_ACCESS_CONTROL)
        result = _slither_scan(project)

        assert result["has_timelock"] is False
        assert result["timelock_type"] == "none"
        assert result["admin_functions_without_timelock"] == []


# ---------------------------------------------------------------------------
# Integration test with monkeypatched LLM
# ---------------------------------------------------------------------------

class TestAnalyzeTimelocks:
    @patch("services.timelock_analysis.chat")
    def test_full_analysis(self, mock_chat, tmp_path):
        mock_chat.return_value = json.dumps({
            "has_timelock": True,
            "timelock_type": "custom",
            "timelock_parameters": {
                "min_delay": "configurable via setDelay",
                "max_delay": "none",
                "configurable": True,
            },
            "timelocked_functions": [
                {"function": "executeTransaction", "delay": "delay",
                 "description": "Executes after delay period"},
            ],
            "unprotected_admin_functions": [],
            "custom_patterns": [],
            "adequacy_assessment": "Custom timelock with configurable delay.",
            "summary": "Custom timelock with queue/execute pattern.",
        })

        project = _make_foundry_project(tmp_path, CUSTOM_TIMELOCK)
        result = analyze_timelocks(project)

        assert result["has_timelock"] is True
        assert result["timelock_type"] == "custom"
        assert (project / "timelock_analysis.json").exists()
        assert (project / "timelock_analysis.md").exists()

    @patch("services.timelock_analysis.chat")
    def test_llm_failure_falls_back_to_slither(self, mock_chat, tmp_path):
        mock_chat.side_effect = RuntimeError("LLM unavailable")

        project = _make_foundry_project(tmp_path, CUSTOM_TIMELOCK)
        result = analyze_timelocks(project)

        # Should still have Slither findings
        assert result["has_timelock"] is True
        assert "queuedTransactions" in result["time_storage_variables"]

    @patch("services.timelock_analysis.chat")
    def test_no_timelock_analysis(self, mock_chat, tmp_path):
        mock_chat.return_value = json.dumps({
            "has_timelock": False,
            "timelock_type": "none",
            "timelock_parameters": {},
            "timelocked_functions": [],
            "unprotected_admin_functions": [
                {"function": "setFee", "risk_level": "medium",
                 "reason": "Can change fee without delay"},
                {"function": "withdraw", "risk_level": "high",
                 "reason": "Can drain funds immediately"},
            ],
            "custom_patterns": [],
            "adequacy_assessment": "No timelock. Admin functions execute immediately.",
            "summary": "No timelock protection for admin functions.",
        })

        project = _make_foundry_project(tmp_path, NO_TIMELOCK)
        result = analyze_timelocks(project)

        assert result["has_timelock"] is False
        assert len(result["unprotected_admin_functions"]) == 2

    @patch("services.timelock_analysis.chat")
    def test_no_sources(self, mock_chat, tmp_path):
        result = analyze_timelocks(tmp_path)
        assert result.get("error") == "No Solidity source files found"
        mock_chat.assert_not_called()
