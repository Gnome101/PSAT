// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/// @notice Test contract for echidna integration tests.
/// Has various require() guards, access control, and pausability
/// to exercise echidna's fuzzing and symbolic execution.
contract Vault {
    mapping(address => uint256) public balances;
    uint256 public total;
    bool public paused;
    address public owner;

    constructor() { owner = msg.sender; }

    modifier whenNotPaused() { require(!paused, "paused"); _; }
    modifier onlyOwner() { require(msg.sender == owner, "not owner"); _; }

    function deposit(uint256 amount) external whenNotPaused {
        require(amount >= 100, "min");
        require(amount <= 10000, "max");
        balances[msg.sender] += amount;
        total += amount;
    }

    function withdraw(uint256 amount) external whenNotPaused {
        require(amount > 0, "zero");
        require(amount <= balances[msg.sender], "insufficient");
        balances[msg.sender] -= amount;
        total -= amount;
    }

    function setFee(uint256 bps) external onlyOwner {
        require(bps <= 10000, "fee > 100%");
    }

    function swap(uint256 amountIn, uint256 minOut) external whenNotPaused {
        require(amountIn >= 100, "dust");
        require(minOut <= amountIn, "minOut > in");
    }

    function pause() external onlyOwner { paused = true; }
    function unpause() external onlyOwner { paused = false; }
}
