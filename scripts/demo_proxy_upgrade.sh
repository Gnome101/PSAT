#!/bin/bash
# Deploy and upgrade a test proxy on a local Anvil node.
#
# Setup:
#   1. Change ETH_RPC in .env to http://127.0.0.1:8546
#   2. anvil --port 8546                              (terminal 1)
#   3. bash start_local.sh                            (terminal 2)
#   4. cd site && npm run dev                         (terminal 3)
#
# Usage:
#   bash scripts/demo_proxy_upgrade.sh deploy         (terminal 4)
#   Copy the proxy address, paste it in http://localhost:5173/proxies, click Watch Proxy
#   bash scripts/demo_proxy_upgrade.sh upgrade <addr> (terminal 4)
#   Watch the upgrade event appear in the GUI
#
# Change ETH_RPC back to your Alchemy URL when done.
#
# Requires: anvil running on port 8546, forge + cast on PATH

set -euo pipefail

RPC="http://127.0.0.1:8546"
# Anvil default test account 0 — not a real key
PRIVATE_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Minimal foundry project
cat > "$TMPDIR/foundry.toml" <<'EOF'
[profile.default]
src = "."
out = "out"
EOF

cat > "$TMPDIR/ImplV1.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract ImplV1 { uint256 public version = 1; }
EOF

cat > "$TMPDIR/ImplV2.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract ImplV2 { uint256 public version = 2; }
EOF

cat > "$TMPDIR/TestProxy.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestProxy {
    bytes32 internal constant _IMPL_SLOT = 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;
    event Upgraded(address indexed implementation);
    constructor(address impl) { _set(impl); }
    function upgradeTo(address newImpl) external { _set(newImpl); }
    function _set(address impl) internal {
        assembly { sstore(_IMPL_SLOT, impl) }
        emit Upgraded(impl);
    }
    fallback() external payable {
        address impl; assembly { impl := sload(_IMPL_SLOT) }
        (bool ok, bytes memory data) = impl.delegatecall(msg.data);
        require(ok); assembly { return(add(data, 0x20), mload(data)) }
    }
    receive() external payable {}
}
EOF

forge_deploy() {
  local file="$1" name="$2"; shift 2
  forge create "$file:$name" \
    --rpc-url "$RPC" --private-key "$PRIVATE_KEY" --broadcast --no-cache \
    --root "$TMPDIR" "$@" 2>/dev/null | grep "Deployed to:" | awk '{print $3}'
}

CMD="${1:-help}"

case "$CMD" in
  deploy)
    IMPL_V1=$(forge_deploy "$TMPDIR/ImplV1.sol" "ImplV1")
    PROXY=$(forge_deploy "$TMPDIR/TestProxy.sol" "TestProxy" --constructor-args "$IMPL_V1")

    echo ""
    echo "Proxy deployed!"
    echo "  Address: $PROXY"
    echo "  Impl V1: $IMPL_V1"
    echo ""
    echo "Paste $PROXY in http://localhost:5173/proxies and click Watch Proxy."
    echo "Then run:  bash scripts/demo_proxy_upgrade.sh upgrade $PROXY"
    ;;

  upgrade)
    PROXY_ADDR="${2:-}"
    if [ -z "$PROXY_ADDR" ]; then
      echo "Usage: bash scripts/demo_proxy_upgrade.sh upgrade <proxy_address>"
      exit 1
    fi

    IMPL_V2=$(forge_deploy "$TMPDIR/ImplV2.sol" "ImplV2")
    cast send "$PROXY_ADDR" "upgradeTo(address)" "$IMPL_V2" \
      --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1

    echo ""
    echo "Proxy upgraded!"
    echo "  Proxy:    $PROXY_ADDR"
    echo "  New impl: $IMPL_V2"
    echo ""
    echo "The upgrade event should appear in the GUI within ~15s."
    ;;

  *)
    echo "Usage:"
    echo "  bash scripts/demo_proxy_upgrade.sh deploy          # deploy a test proxy"
    echo "  bash scripts/demo_proxy_upgrade.sh upgrade <addr>  # upgrade it"
    echo ""
    echo "See the header of this script for full setup instructions."
    ;;
esac
