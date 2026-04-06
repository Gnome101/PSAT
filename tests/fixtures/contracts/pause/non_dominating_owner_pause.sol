// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: a caller check exists on one branch, but it does not dominate the pause write.

contract NonDominatingOwnerPause {
    address public owner;
    bool public paused;

    function pause(bool enforceCheck) external {
        if (enforceCheck) {
            require(msg.sender == owner, "not owner");
        }
        paused = true;
    }
}
