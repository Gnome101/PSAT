// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

// Scenario: Minimal ERC721-compatible NFT implementation.

interface IERC721 {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    function balanceOf(address owner) external view returns (uint256);
    function ownerOf(uint256 tokenId) external view returns (address);
    function safeTransferFrom(address from, address to, uint256 tokenId) external;
    function safeTransferFrom(address from, address to, uint256 tokenId, bytes calldata data) external;
    function transferFrom(address from, address to, uint256 tokenId) external;
    function approve(address to, uint256 tokenId) external;
    function getApproved(uint256 tokenId) external view returns (address);
    function setApprovalForAll(address operator, bool approved) external;
    function isApprovedForAll(address owner, address operator) external view returns (bool);
}

contract Collectible is IERC721 {
    function balanceOf(address) external pure returns (uint256) { return 0; }
    function ownerOf(uint256) external pure returns (address) { return address(0); }
    function safeTransferFrom(address, address, uint256) external pure {}
    function safeTransferFrom(address, address, uint256, bytes calldata) external pure {}
    function transferFrom(address, address, uint256) external pure {}
    function approve(address, uint256) external pure {}
    function getApproved(uint256) external pure returns (address) { return address(0); }
    function setApprovalForAll(address, bool) external pure {}
    function isApprovedForAll(address, address) external pure returns (bool) { return false; }
}
