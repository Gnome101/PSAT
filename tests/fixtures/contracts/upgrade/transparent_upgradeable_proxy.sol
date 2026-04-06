// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: transparent proxy style upgrade path controlled by admin.

contract TransparentUpgradeableProxy {
    address public admin;
    address public implementation;

    function upgradeTo(address newImplementation) external {
        require(msg.sender == admin, "not admin");
        implementation = newImplementation;
    }
}
