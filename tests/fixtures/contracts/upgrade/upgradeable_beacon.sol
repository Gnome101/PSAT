// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: beacon-style upgrade path controlled by onlyOwner.

contract UpgradeableBeacon {
    address public owner;
    address public implementation;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function upgradeTo(address newImplementation) external onlyOwner {
        implementation = newImplementation;
    }
}
