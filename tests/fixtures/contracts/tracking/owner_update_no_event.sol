// Scenario: Owner changes through a writer function without emitting an ownership event.
pragma solidity ^0.8.19;

contract OwnerNoEvent {
    address public owner;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    constructor(address initialOwner) {
        owner = initialOwner;
    }

    function transferOwnership(address newOwner) external onlyOwner {
        owner = newOwner;
    }
}
