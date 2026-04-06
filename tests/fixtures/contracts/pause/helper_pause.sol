// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: pause is controlled through an internal helper auth function.

contract HelperPause {
    address public owner;
    bool public paused;

    function pause() external {
        _checkAuth();
        paused = true;
    }

    function _checkAuth() internal view {
        require(msg.sender == owner, "not owner");
    }
}
