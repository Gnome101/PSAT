"""Anvil integration test for the proxy upgrade monitoring scanner.

Deploys a minimal ERC1967 proxy and two implementation contracts on a local
anvil node, watches the proxy, performs an upgrade, then verifies that
scan_for_upgrades detects the Upgraded event with the correct old/new
implementation addresses.

Requires:
  - anvil, cast, forge (from Foundry) on PATH

Run with:
    uv run pytest tests/test_proxy_watcher_anvil.py -v
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

_has_anvil = shutil.which("anvil") is not None
_has_cast = shutil.which("cast") is not None


def _free_port() -> int:
    """Find a free TCP port by binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> bool:
    """Wait until a TCP port accepts connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _cast(args: list[str], rpc_url: str) -> str:
    """Run a cast command and return stdout."""
    result = subprocess.run(
        ["cast"] + args + ["--rpc-url", rpc_url],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cast failed: {result.stderr}")
    return result.stdout.strip()


def _cast_send(to: str, sig: str, args: list[str], rpc_url: str, private_key: str) -> str:
    """Send a transaction via cast."""
    cmd = (
        ["cast", "send", to, sig]
        + args
        + [
            "--rpc-url",
            rpc_url,
            "--private-key",
            private_key,
        ]
    )
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"cast send failed: {result.stderr}")
    return result.stdout.strip()


def _deploy_raw(bytecode: str, rpc_url: str, private_key: str) -> str:
    """Deploy raw bytecode and return the contract address."""
    result = subprocess.run(
        [
            "cast",
            "send",
            "--create",
            bytecode,
            "--rpc-url",
            rpc_url,
            "--private-key",
            private_key,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Deploy failed: {result.stderr}")

    # Parse the contract address from cast output
    # cast send --create outputs various lines; extract contractAddress
    for line in result.stdout.strip().split("\n"):
        if "contractAddress" in line:
            parts = line.split()
            return parts[-1].strip().lower()

    # Fallback: get address from transaction receipt
    # Look for a hex address in the output
    import re

    addresses = re.findall(r"0x[0-9a-fA-F]{40}", result.stdout)
    if addresses:
        return addresses[0].lower()

    raise RuntimeError(f"Could not find contract address in output:\n{result.stdout}")


# ---------------------------------------------------------------------------
# Minimal Solidity contracts as raw bytecode
# ---------------------------------------------------------------------------

# We'll use inline Solidity compiled via `cast` at test time for clarity.
# The contracts are:
#
# Implementation V1 & V2: simple contracts that just exist (store a version).
# Proxy: stores implementation in the EIP-1967 slot and emits Upgraded(address).

IMPL_V1_SOURCE = """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract ImplV1 {
    uint256 public version = 1;
}
"""

IMPL_V2_SOURCE = """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract ImplV2 {
    uint256 public version = 2;
}
"""

# A minimal proxy that stores impl in the EIP-1967 slot and emits Upgraded(address).
# Has an upgradeTo(address) function callable by anyone (for testing simplicity).
PROXY_SOURCE = """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestProxy {
    // EIP-1967 implementation slot
    bytes32 internal constant _IMPLEMENTATION_SLOT =
        0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;

    event Upgraded(address indexed implementation);

    constructor(address impl) {
        _setImplementation(impl);
    }

    function upgradeTo(address newImpl) external {
        _setImplementation(newImpl);
    }

    function _setImplementation(address impl) internal {
        assembly {
            sstore(_IMPLEMENTATION_SLOT, impl)
        }
        emit Upgraded(impl);
    }

    fallback() external payable {
        address impl;
        assembly {
            impl := sload(_IMPLEMENTATION_SLOT)
        }
        (bool ok, bytes memory data) = impl.delegatecall(msg.data);
        require(ok);
        assembly {
            return(add(data, 0x20), mload(data))
        }
    }

    receive() external payable {}
}
"""


def _compile_and_deploy(
    source: str, contract_name: str, constructor_args: list[str], rpc_url: str, private_key: str, tmp_path: Path
) -> str:
    """Write solidity to a temp file, compile with forge, deploy with cast."""
    src_file = tmp_path / f"{contract_name}.sol"
    src_file.write_text(source)

    # Use forge create with --broadcast to compile, deploy, and broadcast
    cmd = [
        "forge",
        "create",
        f"{src_file}:{contract_name}",
        "--rpc-url",
        rpc_url,
        "--private-key",
        private_key,
        "--broadcast",
        "--no-cache",
    ]
    if constructor_args:
        cmd += ["--constructor-args"] + constructor_args

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=str(tmp_path))
    if result.returncode != 0:
        raise RuntimeError(f"forge create failed for {contract_name}: {result.stderr}\n{result.stdout}")

    # Parse "Deployed to: 0x..." line from forge output
    for line in result.stdout.split("\n"):
        if "Deployed to:" in line or "deployed to:" in line.lower():
            addr = line.split(":")[-1].strip()
            return addr.lower()

    raise RuntimeError(f"Could not parse address from forge create output:\n{result.stdout}")


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_anvil, reason="anvil not found on PATH")
@pytest.mark.skipif(not _has_cast, reason="cast not found on PATH")
@pytest.mark.skipif(not shutil.which("forge"), reason="forge not found on PATH")
def test_deploy_watch_upgrade_detect(tmp_path):
    """Full live test: deploy proxy, watch it, upgrade it, detect the upgrade."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from db.models import ProxyUpgradeEvent, WatchedProxy
    from services.monitoring.proxy_watcher import (
        resolve_current_implementation,
        scan_for_upgrades,
    )

    # -- 1. Start anvil on a random port --
    port = _free_port()
    rpc_url = f"http://127.0.0.1:{port}"
    # Anvil default account 0 private key
    private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

    anvil_proc = subprocess.Popen(
        ["anvil", "--port", str(port), "--silent"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        if not _wait_for_port(port, timeout=15):
            raise RuntimeError("anvil did not start in time")

        # -- 2. Deploy implementation contracts and proxy --
        # Create a minimal foundry project structure for compilation
        foundry_toml = tmp_path / "foundry.toml"
        foundry_toml.write_text("[profile.default]\nsrc = '.'\nout = 'out'\n")

        impl_v1_addr = _compile_and_deploy(IMPL_V1_SOURCE, "ImplV1", [], rpc_url, private_key, tmp_path)
        impl_v2_addr = _compile_and_deploy(IMPL_V2_SOURCE, "ImplV2", [], rpc_url, private_key, tmp_path)
        proxy_addr = _compile_and_deploy(PROXY_SOURCE, "TestProxy", [impl_v1_addr], rpc_url, private_key, tmp_path)

        assert impl_v1_addr.startswith("0x")
        assert impl_v2_addr.startswith("0x")
        assert proxy_addr.startswith("0x")

        # Verify the implementation slot is set correctly
        current_impl = resolve_current_implementation(proxy_addr, rpc_url)
        assert current_impl is not None
        assert current_impl.lower() == impl_v1_addr.lower()

        # Get current block number for last_scanned_block
        current_block_hex = _cast(["block-number"], rpc_url)
        current_block = int(current_block_hex)

        # -- 3. Create test database and register the proxy --
        db_path = tmp_path / "test.db"
        engine = create_engine(f"sqlite:///{db_path}")
        # Only create the tables needed for monitoring (not Job which uses
        # PostgreSQL-specific JSONB columns incompatible with SQLite)
        WatchedProxy.__table__.create(engine, checkfirst=True)  # type: ignore[attr-defined]
        ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)  # type: ignore[attr-defined]
        session = SASession(engine, expire_on_commit=False)

        proxy = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address=proxy_addr.lower(),
            chain="ethereum",
            label="test-proxy",
            last_known_implementation=impl_v1_addr.lower(),
            # Set to current_block - 1 so we pick up the next event
            last_scanned_block=current_block,
        )
        session.add(proxy)
        session.commit()

        # -- 4. Upgrade the proxy --
        _cast_send(
            proxy_addr,
            "upgradeTo(address)",
            [impl_v2_addr],
            rpc_url,
            private_key,
        )

        # -- 5. Run scan_for_upgrades --
        events = scan_for_upgrades(session, rpc_url)

        # -- 6. Assert the upgrade was detected --
        assert len(events) == 1

        evt = events[0]
        assert evt.event_type == "upgraded"
        assert evt.old_implementation is not None
        assert evt.old_implementation.lower() == impl_v1_addr.lower()
        assert evt.new_implementation.lower() == impl_v2_addr.lower()
        assert evt.block_number > current_block
        assert evt.tx_hash is not None and evt.tx_hash.startswith("0x")

        # Verify WatchedProxy was updated
        session.refresh(proxy)
        assert proxy.last_known_implementation is not None
        assert proxy.last_known_implementation.lower() == impl_v2_addr.lower()
        assert proxy.last_scanned_block >= evt.block_number

        # Verify the implementation slot also reflects the upgrade
        new_impl = resolve_current_implementation(proxy_addr, rpc_url)
        assert new_impl is not None
        assert new_impl.lower() == impl_v2_addr.lower()

        # Verify the event is persisted in the DB
        db_events = session.query(ProxyUpgradeEvent).filter(ProxyUpgradeEvent.watched_proxy_id == proxy.id).all()
        assert len(db_events) == 1
        assert db_events[0].new_implementation.lower() == impl_v2_addr.lower()

        # -- Bonus: a second scan should find no new events --
        events_2 = scan_for_upgrades(session, rpc_url)
        assert events_2 == []

        session.close()
        engine.dispose()

    finally:
        # -- 7. Clean up anvil process --
        anvil_proc.terminate()
        try:
            anvil_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            anvil_proc.kill()
            anvil_proc.wait()
