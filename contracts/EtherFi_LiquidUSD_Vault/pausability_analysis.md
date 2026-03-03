# Pausability Analysis

**Pausable:** Yes

**Mechanism:** Custom BeforeTransferHook delegation pattern. The contract implements a hook-based transfer restriction where an external contract's beforeTransfer() function is invoked before every transfer and transferFrom. If the hook is set to a contract that reverts (or to a non-zero address implementing restrictive logic), all token transfers are effectively paused. Setting the hook to address(0) disables the restriction.

## Pause/Unpause Functions
- **setBeforeTransferHook()** — access: owner, authority (via requiresAuth modifier)
  - Sets the BeforeTransferHook contract address. Can instantly pause all transfers by updating to a reverting contract, or unpause by setting to address(0). No timelock or delay mechanism present.

## Affected Functions
| Function | Modifier | Impact |
|----------|----------|--------|
| transfer | public override | Blocked if hook.beforeTransfer() reverts. Calls _callBeforeTransfer internally. |
| transferFrom | public override | Blocked if hook.beforeTransfer() reverts. Calls _callBeforeTransfer internally. |

## Impact Summary
Only ERC20 token transfers (transfer/transferFrom) can be paused. Primary vault operations including enter() (minting), exit() (burning/redemption), and manage() remain fully operational. Users can redeem shares even when transfers are paused, but cannot transfer vault tokens to other addresses.

## Risk Assessment
High centralization risk. Owner or authority can unilaterally freeze all secondary market transfers without notice or timelock. While this doesn't block withdrawals (exit), it effectively traps liquidity and prevents users from transferring vault positions. The same mechanism could be used for selective censorship if the hook implements address-specific blocking. No multi-sig requirement or governance delay visible in the Auth pattern.
