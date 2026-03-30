from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.discovery.fetch import _detect_solc_version as detect_fetch_solc
from workers.static_worker import _detect_solc_version as detect_static_solc


def test_detect_solc_preserves_legacy_majors():
    sources = {"src/Legacy.sol": "pragma solidity ^0.4.24;\ncontract Legacy {}"}
    assert detect_fetch_solc(sources) == "0.4.24"
    assert detect_static_solc(sources) == "0.4.24"


def test_detect_solc_still_bumps_buggy_0_8_versions():
    sources = {"src/Modern.sol": "pragma solidity ^0.8.21;\ncontract Modern {}"}
    assert detect_fetch_solc(sources) == "0.8.24"
    assert detect_static_solc(sources) == "0.8.24"
