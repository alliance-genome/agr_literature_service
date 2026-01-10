"""Authentication endpoints for session-based browser access."""

import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from agr_cognito_py import CognitoAuth, CognitoConfig, get_cognito_auth, get_cognito_user_swagger

logger = logging.getLogger(__name__)

router = APIRouter(tags=['Authentication'], prefix='/auth')

try:
    cognito_config = CognitoConfig()
    auth = CognitoAuth(cognito_config)
except Exception as e:
    logger.error(f"Authentication failed: Exception {e}")

# In-memory session store (for production, consider Redis or database)
# Maps session_id -> {user_info, expires_at}
_session_store: Dict[str, Dict] = {}

# Session configuration
SESSION_COOKIE_NAME = "agr_session"
SESSION_EXPIRY_HOURS = int(os.environ.get("SESSION_EXPIRY_HOURS", "8"))


def _cleanup_expired_sessions():
    """Remove expired sessions from store."""
    now = datetime.utcnow()
    expired = [sid for sid, data in _session_store.items() if data["expires_at"] < now]
    for sid in expired:
        del _session_store[sid]


def get_session_user(session_id: str) -> Optional[Dict]:
    """Get user info from session if valid."""
    _cleanup_expired_sessions()
    session = _session_store.get(session_id)
    if session and session["expires_at"] > datetime.utcnow():
        return session["user_info"]
    return None


@router.post("/login")
async def login(
    response: Response,
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())
):
    """
    Exchange a valid Bearer token for a session cookie.

    After calling this endpoint, you can access API endpoints directly
    in your browser without passing the Authorization header.

    Usage:
    1. Call this endpoint with your Bearer token (use Swagger UI "Try it out")
    2. A session cookie will be set in your browser
    3. Now you can visit any API endpoint URL directly in your browser
    """
    try:
        # Validate the token with Cognito
        user_info = get_cognito_user_swagger(credentials, get_cognito_auth())

        # Create session
        session_id = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(hours=SESSION_EXPIRY_HOURS)

        _session_store[session_id] = {
            "user_info": user_info,
            "expires_at": expires_at
        }

        # Set cookie
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_id,
            httponly=True,
            secure=os.environ.get("ENV_STATE") != "test",  # Secure in prod
            samesite="lax",
            max_age=SESSION_EXPIRY_HOURS * 3600,
            path="/"
        )

        return {
            "status": "success",
            "message": "Session created. You can now access API endpoints directly in your browser.",
            "expires_in_hours": SESSION_EXPIRY_HOURS
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login failed: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")


@router.post("/logout")
async def logout(response: Response):
    """
    Clear the session cookie.

    Call this to end your browser session.
    """
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
    return {"status": "success", "message": "Session cleared"}


@router.get("/status")
async def auth_status():
    """
    Check current authentication status.

    Returns information about active sessions (for debugging).
    """
    _cleanup_expired_sessions()
    return {
        "active_sessions": len(_session_store),
        "session_expiry_hours": SESSION_EXPIRY_HOURS
    }
