// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/// @notice Vault with configurable deposit bounds.
/// Used to test that echidna discovers constraints from live on-chain state
/// (via RPC forking) rather than just source-level constants.
contract ConfigurableVault {
    uint256 public minDeposit;
    uint256 public maxDeposit;
    address public owner;
    mapping(address => uint256) public balances;

    constructor() {
        owner = msg.sender;
        minDeposit = 100;
        maxDeposit = 5000;
    }

    function setMinDeposit(uint256 _min) external {
        require(msg.sender == owner, "not owner");
        minDeposit = _min;
    }

    function setMaxDeposit(uint256 _max) external {
        require(msg.sender == owner, "not owner");
        maxDeposit = _max;
    }

    function deposit(uint256 amount) external {
        require(amount >= minDeposit, "below min");
        require(amount <= maxDeposit, "above max");
        balances[msg.sender] += amount;
    }

    function withdraw(uint256 amount) external {
        require(amount > 0, "zero");
        require(amount <= balances[msg.sender], "insufficient");
        balances[msg.sender] -= amount;
    }
}
