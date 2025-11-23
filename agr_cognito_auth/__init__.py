"""AGR Cognito Authentication Library."""
from .cognito_auth import CognitoAuth
from .config import CognitoConfig
from .dependencies import get_current_user_swagger, get_current_user, require_groups, get_cognito_auth

__version__ = "1.0.0"

__all__ = [
    "CognitoAuth",
    "CognitoConfig",
    "get_current_user",
    "get_current_user_swagger",
    "require_groups",
    "get_cognito_auth",
]
