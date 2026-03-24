// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: UUPS-style upgrade control plus timelock-like functions and factory deployment.

abstract contract UUPSUpgradeable {
    function proxiableUUID() external pure virtual returns (bytes32) {
        return keccak256("eip1967.proxy.implementation");
    }
}

contract Child {}

contract UpgradeFactory is UUPSUpgradeable {
    address public owner;
    uint256 public minDelay;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function upgradeTo(address) external onlyOwner {}
    function _authorizeUpgrade(address) internal onlyOwner {}
    function schedule(address) external onlyOwner {}
    function execute(address) external onlyOwner {}

    function createChild() external onlyOwner returns (address) {
        Child child = new Child();
        return address(child);
    }
}
