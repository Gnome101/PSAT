"""Shared constants for contract analysis."""

from __future__ import annotations

import re

SEVERITY_ORDER = {"High": 0, "Medium": 1, "Low": 2, "Informational": 3, "Optimization": 4}
CONTROL_EFFECTS = {
    "pause_state_change",
    "upgrade_control",
    "ownership_change",
    "role_management",
    "mint_capability",
    "burn_capability",
    "timelock_control",
    "factory_deployment",
    "privileged_external_call",
    "delegatecall_control",
    "selfdestruct_capability",
}
STANDARD_SIGNATURES = {
    "ERC20": {
        "totalSupply()",
        "balanceOf(address)",
        "transfer(address,uint256)",
        "allowance(address,address)",
        "approve(address,uint256)",
        "transferFrom(address,address,uint256)",
    },
    "ERC721": {
        "balanceOf(address)",
        "ownerOf(uint256)",
        "safeTransferFrom(address,address,uint256)",
        "safeTransferFrom(address,address,uint256,bytes)",
        "transferFrom(address,address,uint256)",
        "approve(address,uint256)",
        "getApproved(uint256)",
        "setApprovalForAll(address,bool)",
        "isApprovedForAll(address,address)",
    },
    "ERC1155": {
        "balanceOf(address,uint256)",
        "balanceOfBatch(address[],uint256[])",
        "setApprovalForAll(address,bool)",
        "isApprovedForAll(address,address)",
        "safeTransferFrom(address,address,uint256,uint256,bytes)",
        "safeBatchTransferFrom(address,address,uint256[],uint256[],bytes)",
    },
}
STANDARD_EVENTS = {
    "ERC20": {"Transfer", "Approval"},
    "ERC721": {"Transfer", "Approval", "ApprovalForAll"},
    "ERC1155": {"TransferSingle", "TransferBatch", "ApprovalForAll"},
}
ADMIN_VAR_KEYWORDS = (
    "owner",
    "admin",
    "guardian",
    "governor",
    "operator",
    "manager",
    "authority",
    "timelock",
    "upgrader",
    "pauser",
)
ACCESS_GUARD_KEYWORDS = (
    "onlyowner",
    "onlyrole",
    "auth",
    "authorize",
    "authorized",
    "admin",
    "govern",
    "guardian",
    "operator",
    "manager",
    "timelock",
    "pauser",
    "minter",
)
ACCESS_CONTROL_INHERITANCE = ("ownable", "accesscontrol", "roles", "auth", "governor", "timelock")
PAUSE_MODIFIER_KEYWORDS = ("whennotpaused", "whenpaused", "onlywhenlive")
FACTORY_NAME_KEYWORDS = ("factory", "create", "deploy", "spawn", "clone")
ROLE_CONSTANT_PATTERN = re.compile(r"\b[A-Z][A-Z0-9_]*_ROLE\b")
ROLE_NAME_PATTERNS = (
    re.compile(r".*_role$", re.IGNORECASE),
    re.compile(r"^(wards|admins|authority|authorities)$", re.IGNORECASE),
)
