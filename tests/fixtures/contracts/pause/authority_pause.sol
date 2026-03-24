// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: pause is controlled by an external authority.canCall check.

interface IAuthority {
    function canCall(address src, address dst, bytes4 sig) external view returns (bool);
}

contract AuthorityPause {
    IAuthority public authority;
    bool public paused;

    function pause() external {
        require(authority.canCall(msg.sender, address(this), this.pause.selector), "not auth");
        paused = true;
    }
}
