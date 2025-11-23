"""FastAPI dependencies for Cognito authentication."""
from typing import Dict, Any
from fastapi import Request, HTTPException, Depends
from jose import JWTError

from .cognito_auth import CognitoAuth
from .config import CognitoConfig


# Global instance (initialized once at startup)
_cognito_auth: CognitoAuth = None


def get_cognito_auth() -> CognitoAuth:
    """Get or initialize global CognitoAuth instance."""
    global _cognito_auth
    if _cognito_auth is None:
        _cognito_auth = CognitoAuth()
    return _cognito_auth


async def get_token_from_request(request: Request) -> str:
    """
    Extract JWT token from request.

    Checks (in order):
    1. Authorization header (Bearer token)
    2. Cookie (cognito_token)

    Raises:
        HTTPException: If no token found
    """
    # Check Authorization header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header.replace("Bearer ", "")

    # Check cookies
    token = request.cookies.get("cognito_token")
    if token:
        return token

    raise HTTPException(
        status_code=401,
        detail="Not authenticated - no token provided"
    )


async def get_current_user(
    token: str = Depends(get_token_from_request),
    cognito: CognitoAuth = Depends(get_cognito_auth)
) -> Dict[str, Any]:
    """
    FastAPI dependency to get authenticated user from JWT token.

    Usage:
        @router.get('/me')
        async def get_me(user: Dict = Depends(get_current_user)):
            return {"email": user["email"], "name": user["name"]}

    Returns:
        Dict containing user information

    Raises:
        HTTPException: If token is invalid or expired
    """
    try:
        user = cognito.validate_token(token)
        return user
    except JWTError as e:
        raise HTTPException(
            status_code=401,
            detail=f"Invalid or expired token: {e}"
        )


# Optional: Dependency for checking user groups
def require_groups(*required_groups: str):
    """
    Create a dependency that requires user to be in specific groups.

    Usage:
        @router.post('/admin/action')
        async def admin_action(
            user: Dict = Depends(require_groups("SuperAdmin", "AdminGroup"))
        ):
            return {"message": "Admin action performed"}
    """
    async def check_groups(user: Dict = Depends(get_current_user)) -> Dict:
        user_groups = set(user.get("cognito:groups", []))
        required = set(required_groups)

        if not user_groups.intersection(required):
            raise HTTPException(
                status_code=403,
                detail=f"User must be in one of these groups: {', '.join(required_groups)}"
            )

        return user

    return Depends(check_groups)
