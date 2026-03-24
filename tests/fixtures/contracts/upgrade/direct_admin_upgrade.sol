// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: upgrade is controlled by a direct msg.sender == admin check.

contract DirectAdminUpgrade {
    address public admin;
    address public implementation;

    function upgradeTo(address newImplementation) external {
        require(msg.sender == admin, "not admin");
        implementation = newImplementation;
    }
}
