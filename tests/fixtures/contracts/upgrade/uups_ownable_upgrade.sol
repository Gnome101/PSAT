// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: UUPS upgrade path controlled by onlyOwner.

abstract contract UUPSUpgradeable {
    function proxiableUUID() external pure virtual returns (bytes32) {
        return keccak256("eip1967.proxy.implementation");
    }
}

contract UUPSOwnableUpgrade is UUPSUpgradeable {
    address public owner;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function upgradeTo(address) external onlyOwner {}
    function _authorizeUpgrade(address) internal onlyOwner {}
}
