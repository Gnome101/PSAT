// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: pause is controlled by a direct timelock address check.

contract TimelockPause {
    address public timelock;
    bool public paused;

    function pause() external {
        require(msg.sender == timelock, "not timelock");
        paused = true;
    }
}
