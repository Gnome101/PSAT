// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: pause is controlled by an authorization mapping.

contract MappingPause {
    mapping(address => uint256) public wards;
    bool public paused;

    function pause() external {
        require(wards[msg.sender] == 1, "not auth");
        paused = true;
    }
}
