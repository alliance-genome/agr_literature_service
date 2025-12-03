"""Cognito user permission utilities."""
from enum import Enum
from typing import Dict, Any, Set


class ModAccess(Enum):
    """Access levels for MOD-based permissions."""
    NO_ACCESS = 0
    ALL_ACCESS = 1
    SGD = 2
    RGD = 3
    MGI = 4
    ZFIN = 5
    XB = 6
    FB = 7
    WB = 8


# Mapping from ModAccess to MOD abbreviation strings
MOD_ACCESS_ABBR: Dict[ModAccess, str] = {
    ModAccess.NO_ACCESS: "no_access",
    ModAccess.ALL_ACCESS: "all_access",
    ModAccess.SGD: "SGD",
    ModAccess.RGD: "RGD",
    ModAccess.MGI: "MGI",
    ModAccess.ZFIN: "ZFIN",
    ModAccess.XB: "XB",
    ModAccess.FB: "FB",
    ModAccess.WB: "WB",
}

# Groups that grant full access
ADMIN_GROUPS: Set[str] = {
    "SuperAdmin",
    "AdminGroup",
    "AllianceDeveloper",
}

# Mapping from Cognito group names to ModAccess levels
GROUP_TO_ACCESS: Dict[str, ModAccess] = {
    # Curator groups
    "SGDCurator": ModAccess.SGD,
    "RGDCurator": ModAccess.RGD,
    "MGICurator": ModAccess.MGI,
    "ZFINCurator": ModAccess.ZFIN,
    "XenbaseCurator": ModAccess.XB,
    "FlyBaseCurator": ModAccess.FB,
    "WormBaseCurator": ModAccess.WB,
}


def get_mod_access(user: Dict[str, Any]) -> ModAccess:
    """
    Get MOD access level from a Cognito user object.

    Access token (service accounts): Returns ALL_ACCESS
    ID token (user login): Checks cognito:groups for permissions

    Args:
        user: Cognito user dict with keys like 'cognito:groups', 'token_type'

    Returns:
        ModAccess enum value indicating permission level
    """
    # Service accounts (access tokens from client_credentials) get full access
    if user.get("token_type") == "access":
        return ModAccess.ALL_ACCESS

    # Check Cognito groups for ID tokens
    groups = user.get("cognito:groups", [])

    for group in groups:
        # Check for admin/developer groups
        if group in ADMIN_GROUPS or group.endswith("Developer"):
            return ModAccess.ALL_ACCESS

        # Check for MOD-specific curator groups
        if group in GROUP_TO_ACCESS:
            return GROUP_TO_ACCESS[group]

    return ModAccess.NO_ACCESS


def has_mod_access(user: Dict[str, Any], required_mod: str) -> bool:
    """
    Check if user has access to a specific MOD.

    Args:
        user: Cognito user dict
        required_mod: MOD abbreviation (e.g., "SGD", "WB")

    Returns:
        True if user has access to the MOD or has ALL_ACCESS
    """
    access = get_mod_access(user)

    if access == ModAccess.ALL_ACCESS:
        return True

    return MOD_ACCESS_ABBR.get(access) == required_mod
