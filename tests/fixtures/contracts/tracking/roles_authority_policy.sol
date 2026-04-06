// Scenario: RolesAuthority-style canCall policy writes emit deterministic policy update events.
pragma solidity ^0.8.19;

interface AuthorityLike {
    function canCall(address user, address target, bytes4 functionSig) external view returns (bool);
}

contract AuthBase {
    event OwnershipTransferred(address indexed user, address indexed newOwner);
    event AuthorityUpdated(address indexed user, address indexed newAuthority);

    address public owner;
    AuthorityLike public authority;

    constructor(address initialOwner, AuthorityLike initialAuthority) {
        owner = initialOwner;
        authority = initialAuthority;
    }

    modifier requiresAuth() {
        require(isAuthorized(msg.sender, msg.sig), "UNAUTHORIZED");
        _;
    }

    function isAuthorized(address user, bytes4 functionSig) internal view returns (bool) {
        return user == owner || (address(authority) != address(0) && authority.canCall(user, address(this), functionSig));
    }

    function setAuthority(AuthorityLike newAuthority) public requiresAuth {
        authority = newAuthority;
        emit AuthorityUpdated(msg.sender, address(newAuthority));
    }

    function transferOwnership(address newOwner) public requiresAuth {
        owner = newOwner;
        emit OwnershipTransferred(msg.sender, newOwner);
    }
}

contract RolesAuthorityPolicy is AuthBase, AuthorityLike {
    event UserRoleUpdated(address indexed user, uint8 indexed role, bool enabled);
    event PublicCapabilityUpdated(address indexed target, bytes4 indexed functionSig, bool enabled);
    event RoleCapabilityUpdated(uint8 indexed role, address indexed target, bytes4 indexed functionSig, bool enabled);

    mapping(address => bytes32) public getUserRoles;
    mapping(address => mapping(bytes4 => bool)) public isCapabilityPublic;
    mapping(address => mapping(bytes4 => bytes32)) public getRolesWithCapability;

    constructor() AuthBase(msg.sender, AuthorityLike(address(0))) {}

    function canCall(address user, address target, bytes4 functionSig) public view override returns (bool) {
        return
            isCapabilityPublic[target][functionSig] ||
            bytes32(0) != getUserRoles[user] & getRolesWithCapability[target][functionSig];
    }

    function setPublicCapability(address target, bytes4 functionSig, bool enabled) public requiresAuth {
        isCapabilityPublic[target][functionSig] = enabled;
        emit PublicCapabilityUpdated(target, functionSig, enabled);
    }

    function setRoleCapability(uint8 role, address target, bytes4 functionSig, bool enabled) public requiresAuth {
        if (enabled) {
            getRolesWithCapability[target][functionSig] |= bytes32(1 << role);
        } else {
            getRolesWithCapability[target][functionSig] &= ~bytes32(1 << role);
        }
        emit RoleCapabilityUpdated(role, target, functionSig, enabled);
    }

    function setUserRole(address user, uint8 role, bool enabled) public requiresAuth {
        if (enabled) {
            getUserRoles[user] |= bytes32(1 << role);
        } else {
            getUserRoles[user] &= ~bytes32(1 << role);
        }
        emit UserRoleUpdated(user, role, enabled);
    }
}
