"""AGR Cognito Authentication Library."""
from .cognito_auth import (
    CognitoAuth,
    get_admin_token,
    get_authentication_token,
    generate_headers,
    clear_token_cache,
)
from .config import CognitoConfig, CognitoAdminConfig
from .dependencies import get_cognito_user_swagger, get_cognito_user, require_groups, get_cognito_auth
from .cognito_permissions import ModAccess, MOD_ACCESS_ABBR, get_mod_access, has_mod_access

__version__ = "1.0.0"

__all__ = [
    "CognitoAuth",
    "CognitoConfig",
    "CognitoAdminConfig",
    "get_cognito_user",
    "get_cognito_user_swagger",
    "require_groups",
    "get_cognito_auth",
    "get_admin_token",
    "get_authentication_token",
    "generate_headers",
    "clear_token_cache",
    "ModAccess",
    "MOD_ACCESS_ABBR",
    "get_mod_access",
    "has_mod_access",
]
