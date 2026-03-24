// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: selfdestruct capability is guarded by an owner check.

contract SelfDestructControl {
    address public owner;

    function destroy() external {
        require(msg.sender == owner, "not owner");
        selfdestruct(payable(owner));
    }
}
