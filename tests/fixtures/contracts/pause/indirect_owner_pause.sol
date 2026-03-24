// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: an owner check in the entrypoint guards a pause write in an internal helper.

contract IndirectOwnerPause {
    address public owner;
    bool public paused;

    function pause() external {
        require(msg.sender == owner, "not owner");
        _setPaused();
    }

    function _setPaused() internal {
        paused = true;
    }
}
