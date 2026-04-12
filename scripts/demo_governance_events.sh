#!/bin/bash
# Deploy governance test contracts on Anvil and fire events to test monitoring.
#
# Setup:
#   1. Change ETH_RPC in .env to http://127.0.0.1:8546
#   2. anvil --port 8546                              (terminal 1)
#   3. bash start_local.sh                            (terminal 2)
#   4. cd site && npm run dev                         (terminal 3)
#
# Usage:
#   bash scripts/demo_governance_events.sh deploy           # deploy all contracts
#   bash scripts/demo_governance_events.sh watch            # register for monitoring
#   Add Discord webhook at http://localhost:5173/company/demo/monitoring
#   bash scripts/demo_governance_events.sh fire             # fire all event types
#
# This deploys:
#   - EIP-1967 proxy + two implementations (for upgrade events)
#   - Ownable contract (for ownership transfer)
#   - Pausable contract (for pause/unpause)
#   - Gnosis Safe mock (for signer add/remove, threshold change)
#   - Timelock mock (for schedule/execute, delay change)
#   - AccessControl mock (for role grant/revoke)
#
# Requires: anvil running on port 8546, forge + cast on PATH

set -euo pipefail

RPC="http://127.0.0.1:8546"
PRIVATE_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ACCOUNT0="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ACCOUNT1="0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
ACCOUNT2="0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

cat > "$TMPDIR/foundry.toml" <<'EOF'
[profile.default]
src = "."
out = "out"
EOF

# --- Contracts ---

cat > "$TMPDIR/ImplV1.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract ImplV1 {
    uint256 public version = 1;
}
EOF

cat > "$TMPDIR/ImplV2.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract ImplV2 {
    uint256 public version = 2;
}
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

cat > "$TMPDIR/TestOwnable.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestOwnable {
    address public owner;
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    constructor() {
        owner = msg.sender;
        emit OwnershipTransferred(address(0), msg.sender);
    }
    function transferOwnership(address newOwner) external {
        require(msg.sender == owner, "not owner");
        address old = owner;
        owner = newOwner;
        emit OwnershipTransferred(old, newOwner);
    }
}
EOF

cat > "$TMPDIR/TestPausable.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestPausable {
    bool public paused;
    address public owner;
    event Paused(address account);
    event Unpaused(address account);
    constructor() { owner = msg.sender; }
    function pause() external {
        require(msg.sender == owner);
        paused = true;
        emit Paused(msg.sender);
    }
    function unpause() external {
        require(msg.sender == owner);
        paused = false;
        emit Unpaused(msg.sender);
    }
}
EOF

cat > "$TMPDIR/TestSafe.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestSafe {
    address[] internal _owners;
    uint256 internal _threshold;
    event AddedOwner(address owner);
    event RemovedOwner(address owner);
    event ChangedThreshold(uint256 threshold);
    constructor() {
        _owners.push(msg.sender);
        _threshold = 1;
    }
    function getOwners() external view returns (address[] memory) { return _owners; }
    function getThreshold() external view returns (uint256) { return _threshold; }
    function addOwner(address o) external { _owners.push(o); emit AddedOwner(o); }
    function removeOwner(address o) external {
        for (uint i = 0; i < _owners.length; i++) {
            if (_owners[i] == o) { _owners[i] = _owners[_owners.length - 1]; _owners.pop(); break; }
        }
        emit RemovedOwner(o);
    }
    function changeThreshold(uint256 t) external { _threshold = t; emit ChangedThreshold(t); }
}
EOF

cat > "$TMPDIR/TestTimelock.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestTimelock {
    uint256 public minDelay;
    event CallScheduled(bytes32 indexed id, uint256 indexed index, address target, uint256 value, bytes data, bytes32 predecessor, uint256 delay);
    event CallExecuted(bytes32 indexed id, uint256 indexed index, address target, uint256 value, bytes data);
    event MinDelayChange(uint256 oldDuration, uint256 newDuration);
    constructor(uint256 d) { minDelay = d; }
    function getMinDelay() external view returns (uint256) { return minDelay; }
    function schedule(bytes32 id, uint256 index, address target, uint256 value, bytes calldata data, bytes32 predecessor, uint256 delay) external {
        emit CallScheduled(id, index, target, value, data, predecessor, delay);
    }
    function execute(bytes32 id, uint256 index, address target, uint256 value, bytes calldata data) external {
        emit CallExecuted(id, index, target, value, data);
    }
    function updateDelay(uint256 newDelay) external {
        uint256 old = minDelay;
        minDelay = newDelay;
        emit MinDelayChange(old, newDelay);
    }
}
EOF

cat > "$TMPDIR/TestAccessControl.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract TestAccessControl {
    event RoleGranted(bytes32 indexed role, address indexed account, address indexed sender);
    event RoleRevoked(bytes32 indexed role, address indexed account, address indexed sender);
    function grantRole(bytes32 role, address account) external {
        emit RoleGranted(role, account, msg.sender);
    }
    function revokeRole(bytes32 role, address account) external {
        emit RoleRevoked(role, account, msg.sender);
    }
}
EOF

# --- Helpers ---

forge_deploy() {
    local file="$1" name="$2"; shift 2
    forge create "$file:$name" \
        --rpc-url "$RPC" --private-key "$PRIVATE_KEY" --broadcast --no-cache \
        --root "$TMPDIR" "$@" 2>/dev/null | grep "Deployed to:" | awk '{print $3}'
}

STATE_FILE="/tmp/demo_governance_state.env"

CMD="${1:-help}"

case "$CMD" in
    deploy)
        echo "Deploying contracts to Anvil..."
        echo ""

        IMPL_V1=$(forge_deploy "$TMPDIR/ImplV1.sol" "ImplV1")
        PROXY=$(forge_deploy "$TMPDIR/TestProxy.sol" "TestProxy" --constructor-args "$IMPL_V1")
        OWNABLE=$(forge_deploy "$TMPDIR/TestOwnable.sol" "TestOwnable")
        PAUSABLE=$(forge_deploy "$TMPDIR/TestPausable.sol" "TestPausable")
        SAFE=$(forge_deploy "$TMPDIR/TestSafe.sol" "TestSafe")
        TIMELOCK=$(forge_deploy "$TMPDIR/TestTimelock.sol" "TestTimelock" --constructor-args 3600)
        ACCESS=$(forge_deploy "$TMPDIR/TestAccessControl.sol" "TestAccessControl")

        # Save state for the fire command
        cat > "$STATE_FILE" <<ENVEOF
PROXY=$PROXY
IMPL_V1=$IMPL_V1
OWNABLE=$OWNABLE
PAUSABLE=$PAUSABLE
SAFE=$SAFE
TIMELOCK=$TIMELOCK
ACCESS=$ACCESS
ENVEOF

        echo "All contracts deployed!"
        echo ""
        echo "  Proxy:          $PROXY  (impl: $IMPL_V1)"
        echo "  Ownable:        $OWNABLE"
        echo "  Pausable:       $PAUSABLE"
        echo "  Safe:           $SAFE"
        echo "  Timelock:       $TIMELOCK  (delay: 3600s)"
        echo "  AccessControl:  $ACCESS"
        echo ""
        echo "Next steps:"
        echo "  1. Run:  bash scripts/demo_governance_events.sh watch"
        echo "  2. Add Discord webhook at http://localhost:5173/company/demo/monitoring"
        echo "  3. Run:  bash scripts/demo_governance_events.sh fire"
        echo ""
        echo "State saved to $STATE_FILE"
        ;;

    watch)
        if [ ! -f "$STATE_FILE" ]; then
            echo "No state file found. Run 'deploy' first."
            exit 1
        fi
        source "$STATE_FILE"

        echo "Registering contracts for monitoring..."

        uv run python3 -c "
import uuid
from db.models import SessionLocal, MonitoredContract, WatchedProxy, Protocol, Contract, Job, JobStage, JobStatus
from utils.rpc import rpc_request

s = SessionLocal()
block = int(rpc_request('$RPC', 'eth_blockNumber', []), 16)

# 1. Create a Protocol row so the GUI can find these contracts
proto = Protocol(name='demo')
s.add(proto)
s.flush()  # assigns proto.id

contracts = [
    ('$PROXY',    'proxy',    'TestProxy',         True,  'eip1967', {'watch_upgrades': True, 'watch_ownership': True, 'watch_pause': False, 'watch_roles': False, 'watch_safe_signers': False, 'watch_timelock': False}),
    ('$OWNABLE',  'regular',  'TestOwnable',       False, None,      {'watch_upgrades': False, 'watch_ownership': True, 'watch_pause': False, 'watch_roles': False, 'watch_safe_signers': False, 'watch_timelock': False}),
    ('$PAUSABLE', 'pausable', 'TestPausable',      False, None,      {'watch_upgrades': False, 'watch_ownership': True, 'watch_pause': True, 'watch_roles': False, 'watch_safe_signers': False, 'watch_timelock': False}),
    ('$SAFE',     'safe',     'TestSafe',          False, None,      {'watch_upgrades': False, 'watch_ownership': True, 'watch_pause': False, 'watch_roles': False, 'watch_safe_signers': True, 'watch_timelock': False}),
    ('$TIMELOCK', 'timelock', 'TestTimelock',      False, None,      {'watch_upgrades': False, 'watch_ownership': True, 'watch_pause': False, 'watch_roles': False, 'watch_safe_signers': False, 'watch_timelock': True}),
    ('$ACCESS',   'regular',  'TestAccessControl', False, None,      {'watch_upgrades': False, 'watch_ownership': True, 'watch_pause': False, 'watch_roles': True, 'watch_safe_signers': False, 'watch_timelock': False}),
]

for addr, ctype, name, is_proxy, proxy_type, config in contracts:
    # 2. Create a Contract row linked to the protocol
    ct = Contract(
        address=addr.lower(),
        chain='anvil',
        contract_name=name,
        protocol_id=proto.id,
        is_proxy=is_proxy,
        proxy_type=proxy_type,
    )
    s.add(ct)
    s.flush()  # assigns ct.id

    # 3. Create a completed Job so enrollment filters see this contract
    job = Job(
        address=addr.lower(),
        name=name,
        status=JobStatus.completed,
        stage=JobStage.done,
        protocol_id=proto.id,
    )
    s.add(job)
    s.flush()  # assigns job.id

    # Link Contract to its Job
    ct.job_id = job.id

    # 4-5. Create MonitoredContract linked to protocol and contract
    mc = MonitoredContract(
        id=uuid.uuid4(),
        address=addr.lower(),
        chain='anvil',
        protocol_id=proto.id,
        contract_id=ct.id,
        contract_type=ctype,
        monitoring_config=config,
        last_known_state={},
        last_scanned_block=block,
        needs_polling=ctype in ('safe', 'timelock'),
        is_active=True,
        enrollment_source='demo',
    )
    s.add(mc)
    print(f'  {name:25s} {addr}  ({ctype})')

# Also create WatchedProxy for the proxy
wp = WatchedProxy(
    id=uuid.uuid4(),
    proxy_address='$PROXY'.lower(),
    chain='anvil',
    label='TestProxy',
    proxy_type='eip1967',
    last_known_implementation='$IMPL_V1'.lower(),
    last_scanned_block=block,
    needs_polling=False,
)
s.add(wp)
s.flush()

# Link WatchedProxy to MonitoredContract
from sqlalchemy import select
proxy_mc = s.execute(select(MonitoredContract).where(MonitoredContract.address == '$PROXY'.lower())).scalar_one()
proxy_mc.watched_proxy_id = wp.id

s.commit()
print()
print(f'All 6 contracts registered at block {block} (protocol_id={proto.id}).')
s.close()
"
        echo ""
        echo "Contracts are now being monitored."
        echo ""
        echo "Next steps:"
        echo "  1. Add Discord webhook at http://localhost:5173/company/demo/monitoring"
        echo "  2. Run:  bash scripts/demo_governance_events.sh fire"
        ;;

    fire)
        if [ ! -f "$STATE_FILE" ]; then
            echo "No state file found. Run 'deploy' first."
            exit 1
        fi
        source "$STATE_FILE"

        echo "Firing governance events..."
        echo ""

        # 1. Proxy upgrade
        echo "[1/9] Upgrading proxy..."
        IMPL_V2=$(forge_deploy "$TMPDIR/ImplV2.sol" "ImplV2")
        cast send "$PROXY" "upgradeTo(address)" "$IMPL_V2" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Proxy upgraded: $IMPL_V1 -> $IMPL_V2"

        # 2. Ownership transfer
        echo "[2/9] Transferring ownership..."
        cast send "$OWNABLE" "transferOwnership(address)" "$ACCOUNT1" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Owner: $ACCOUNT0 -> $ACCOUNT1"

        # 3. Pause
        echo "[3/9] Pausing..."
        cast send "$PAUSABLE" "pause()" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Paused"

        # 4. Unpause
        echo "[4/9] Unpausing..."
        cast send "$PAUSABLE" "unpause()" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Unpaused"

        # 5. Safe: add signer
        echo "[5/9] Adding safe signer..."
        cast send "$SAFE" "addOwner(address)" "$ACCOUNT1" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Signer added: $ACCOUNT1"

        # 6. Safe: change threshold
        echo "[6/9] Changing safe threshold..."
        cast send "$SAFE" "changeThreshold(uint256)" 2 \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Threshold: 1 -> 2"

        # 7. Timelock: schedule
        echo "[7/9] Scheduling timelock operation..."
        OP_ID="0x$(printf '%064x' 42)"
        cast send "$TIMELOCK" \
            "schedule(bytes32,uint256,address,uint256,bytes,bytes32,uint256)" \
            "$OP_ID" 0 "$OWNABLE" 0 "0x" "0x0000000000000000000000000000000000000000000000000000000000000000" 3600 \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Operation scheduled (delay: 3600s)"

        # 8. Timelock: change delay
        echo "[8/9] Changing timelock delay..."
        cast send "$TIMELOCK" "updateDelay(uint256)" 7200 \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Delay: 3600 -> 7200"

        # 9. AccessControl: grant + revoke role
        ADMIN_ROLE="0x0000000000000000000000000000000000000000000000000000000000000000"
        echo "[9/9] Granting and revoking role..."
        cast send "$ACCESS" "grantRole(bytes32,address)" "$ADMIN_ROLE" "$ACCOUNT2" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        cast send "$ACCESS" "revokeRole(bytes32,address)" "$ADMIN_ROLE" "$ACCOUNT2" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null 2>&1
        echo "  Role granted then revoked for $ACCOUNT2"

        echo ""
        echo "All 10 events fired. Check the monitoring tab — they should appear within ~15s."
        ;;

    *)
        echo "Usage:"
        echo "  bash scripts/demo_governance_events.sh deploy    # deploy all test contracts"
        echo "  bash scripts/demo_governance_events.sh watch     # register them for monitoring"
        echo "  bash scripts/demo_governance_events.sh fire      # fire all event types"
        echo ""
        echo "See the header of this script for full setup instructions."
        ;;
esac
