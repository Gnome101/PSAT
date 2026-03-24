// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: a direct external call is guarded by an owner check.

interface ICallTarget {
    function ping(uint256 amount) external;
}

contract ExternalCallControl {
    address public owner;
    ICallTarget public target;

    function pingTarget(uint256 amount) external {
        require(msg.sender == owner, "not owner");
        target.ping(amount);
    }
}
