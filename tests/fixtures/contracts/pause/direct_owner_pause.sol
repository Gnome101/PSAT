// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: pause is controlled by a direct msg.sender == owner check.

contract DirectOwnerPause {
    address public owner;
    bool public paused;

    function pause() external {
        require(msg.sender == owner, "not owner");
        paused = true;
    }
}
