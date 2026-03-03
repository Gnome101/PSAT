import json
import sys
from pathlib import Path
from unittest.mock import patch, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.llm_big_picture import analyze_big_picture


SAMPLE_SOURCE = """\
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract MyToken is ERC20, Ownable, Pausable {
    function mint(address to, uint256 amount) external onlyOwner {
        _mint(to, amount);
    }
}
"""

SAMPLE_PRIVILEGE = {
    "access_control_model": ["Ownable"],
    "privileged_roles": ["owner"],
    "gated_functions": [{"function": "mint", "modifiers": ["onlyOwner"]}],
}

SAMPLE_PAUSABILITY = {
    "is_pausable": True,
    "pause_functions": {"pause": {"access_control": ["onlyOwner"]}},
}

SAMPLE_TIMELOCK = {
    "has_timelock": False,
    "timelock_type": "none",
}

SAMPLE_DEPS = {
    "root": "0x1234",
    "dependencies": ["0x5678", "0x9abc"],
}


class TestAnalyzeBigPicture:
    def _make_project(self, tmp_path: Path,
                      source: str = SAMPLE_SOURCE,
                      privilege: dict | None = SAMPLE_PRIVILEGE,
                      pausability: dict | None = SAMPLE_PAUSABILITY,
                      timelock: dict | None = SAMPLE_TIMELOCK,
                      deps: dict | None = SAMPLE_DEPS,
                      dynamic_deps: dict | None = None) -> Path:
        src_dir = tmp_path / "src"
        src_dir.mkdir(parents=True)
        (src_dir / "Contract.sol").write_text(source)

        meta = {"contract_name": "MyToken", "address": "0x1234", "compiler_version": "0.8.20"}
        (tmp_path / "contract_meta.json").write_text(json.dumps(meta))

        if privilege:
            (tmp_path / "privilege_analysis.json").write_text(json.dumps(privilege))
        if pausability:
            (tmp_path / "pausability_analysis.json").write_text(json.dumps(pausability))
        if timelock:
            (tmp_path / "timelock_analysis.json").write_text(json.dumps(timelock))
        if deps:
            (tmp_path / "dependencies.json").write_text(json.dumps(deps))
        if dynamic_deps:
            (tmp_path / "dynamic_dependencies.json").write_text(json.dumps(dynamic_deps))

        return tmp_path

    @patch("services.llm_big_picture.chat")
    def test_all_context_assembled(self, mock_chat, tmp_path):
        """Verify the LLM receives all prior analysis outputs."""
        mock_chat.return_value = "# Big Picture Analysis\n\nAll good."

        project_dir = self._make_project(tmp_path)
        result = analyze_big_picture(project_dir)

        assert result == "# Big Picture Analysis\n\nAll good."
        mock_chat.assert_called_once()

        # Check the user message contains all sections
        user_msg = mock_chat.call_args[1].get("messages") or mock_chat.call_args[0][0]
        if isinstance(user_msg, list):
            user_msg = user_msg[1]["content"]

        assert "MyToken" in user_msg
        assert "Source Code" in user_msg
        assert "Privilege Analysis" in user_msg
        assert "Pausability Analysis" in user_msg
        assert "Timelock Analysis" in user_msg
        assert "Static Dependencies" in user_msg

    @patch("services.llm_big_picture.chat")
    def test_missing_optional_data(self, mock_chat, tmp_path):
        """Verify it works without optional analysis files."""
        mock_chat.return_value = "# Big Picture Analysis\n\nMinimal analysis."

        project_dir = self._make_project(
            tmp_path,
            privilege=None,
            pausability=None,
            timelock=None,
            deps=None,
        )
        result = analyze_big_picture(project_dir)

        assert "Big Picture Analysis" in result
        mock_chat.assert_called_once()

        user_msg = mock_chat.call_args[1].get("messages") or mock_chat.call_args[0][0]
        if isinstance(user_msg, list):
            user_msg = user_msg[1]["content"]

        assert "Source Code" in user_msg
        assert "Privilege Analysis" not in user_msg

    @patch("services.llm_big_picture.chat")
    def test_system_prompt_contains_key_sections(self, mock_chat, tmp_path):
        """Verify system prompt requests the expected analysis sections."""
        mock_chat.return_value = "Analysis output."

        project_dir = self._make_project(tmp_path)
        analyze_big_picture(project_dir)

        messages = mock_chat.call_args[1].get("messages") or mock_chat.call_args[0][0]
        system_msg = messages[0]["content"]

        assert "Ecosystem Role" in system_msg
        assert "Cross-Contract Interaction Risks" in system_msg
        assert "Privileged User Threat Model" in system_msg
        assert "Pause Impact Analysis" in system_msg
        assert "Timelock Adequacy" in system_msg
        assert "Governance Risk Score" in system_msg
