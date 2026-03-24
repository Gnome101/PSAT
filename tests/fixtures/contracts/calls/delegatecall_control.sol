// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: a delegatecall sink is guarded by an owner check.

contract DelegateCallControl {
    address public owner;
    address public implementation;

    function execute(bytes calldata data) external {
        require(msg.sender == owner, "not owner");
        (bool ok,) = implementation.delegatecall(data);
        require(ok, "delegatecall failed");
    }
}
