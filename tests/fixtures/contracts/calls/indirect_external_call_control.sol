// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: an internal helper both checks auth and performs the external call.

interface IIndirectCallTarget {
    function ping(uint256 amount) external;
}

contract IndirectExternalCallControl {
    address public owner;
    IIndirectCallTarget public target;

    function pingTarget(uint256 amount) external {
        _ping(amount);
    }

    function _ping(uint256 amount) internal {
        require(msg.sender == owner, "not owner");
        target.ping(amount);
    }
}
