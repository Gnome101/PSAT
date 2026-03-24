// Scenario: Auth-like modifier expands into an internal helper that authorizes by owner or authority.canCall.
pragma solidity ^0.8.19;

interface AuthorityLike {
    function canCall(address user, address target, bytes4 functionSig) external view returns (bool);
}

interface PingTarget {
    function ping(uint256 value) external;
}

abstract contract AuthLike {
    event OwnershipTransferred(address indexed user, address indexed newOwner);
    event AuthorityUpdated(address indexed user, AuthorityLike indexed newAuthority);

    address public owner;
    AuthorityLike public authority;

    modifier requiresAuth() {
        require(isAuthorized(msg.sender, msg.sig), "UNAUTHORIZED");
        _;
    }

    function isAuthorized(address user, bytes4 functionSig) internal view returns (bool) {
        AuthorityLike auth = authority;
        return (address(auth) != address(0) && auth.canCall(user, address(this), functionSig)) || user == owner;
    }

    function setAuthority(AuthorityLike newAuthority) public {
        require(msg.sender == owner || authority.canCall(msg.sender, address(this), msg.sig), "UNAUTHORIZED");
        authority = newAuthority;
        emit AuthorityUpdated(msg.sender, newAuthority);
    }

    function transferOwnership(address newOwner) public requiresAuth {
        owner = newOwner;
        emit OwnershipTransferred(msg.sender, newOwner);
    }
}

contract AuthModifierController is AuthLike {
    address public hook;

    function setHook(address newHook) external requiresAuth {
        hook = newHook;
    }

    function manage(PingTarget target, uint256 value) external requiresAuth {
        target.ping(value);
    }
}
