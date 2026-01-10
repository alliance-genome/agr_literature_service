"""
VPN-aware authentication module for the AGR Literature Service.

This module provides authentication decorators and dependencies that allow:
- External requests: Always require Cognito authentication
- VPN requests: GET methods skip auth by default, mutations require auth
- Browser session: Cookie-based auth for direct browser access

Decorators:
- @skip_auth_on_vpn: Skip auth for VPN requests on non-GET endpoints
- @enforce_auth: Always require auth, even for VPN GET requests
"""

import os
from ipaddress import ip_address, ip_network
from typing import Any, Callable, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from agr_cognito_py import get_cognito_auth, get_cognito_user_swagger

# Session cookie name - must match authentication.py
SESSION_COOKIE_NAME = "agr_session"


# =============================================================================
# Route decorators - use these to flag special auth behavior
# =============================================================================

def skip_auth_on_vpn(func: Callable) -> Callable:
    """
    Decorator to skip auth for VPN requests on non-GET endpoints.
    Use this for POST/PATCH/DELETE endpoints that should be open on VPN.

    Example:
        @router.post('/show_all')
        @skip_auth_on_vpn
        def show_all(...):
            ...
    """
    setattr(func, "_skip_auth_for_vpn", True)  # noqa: B010
    return func


def enforce_auth(func: Callable) -> Callable:
    """
    Decorator to always require auth, even for VPN GET requests.
    Use this for sensitive GET endpoints that need protection.

    Example:
        @router.get('/sensitive_data')
        @enforce_auth
        def get_sensitive_data(...):
            ...
    """
    setattr(func, "_enforce_auth", True)  # noqa: B010
    return func


# =============================================================================
# VPN detection utilities
# =============================================================================

def get_internal_cidr_ranges() -> List[str]:
    """Get internal CIDR ranges from environment variable.

    Set INTERNAL_CIDR_RANGES environment variable with comma-separated CIDR ranges.
    Example: INTERNAL_CIDR_RANGES=10.0.0.0/8,172.16.0.0/12
    """
    ranges = os.environ.get("INTERNAL_CIDR_RANGES", "")
    if not ranges:
        return []
    return [r.strip() for r in ranges.split(",") if r.strip()]


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from request, handling load balancer scenarios.

    Priority order:
    1. X-Forwarded-For: First IP is the original client (set by ALB/CloudFront)
    2. request.client.host: Direct connection IP

    Note: X-Real-IP is NOT used because nginx sets it to the ALB's IP,
    which is always in the VPC range and would bypass auth for all requests.
    """
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()

    if request.client:
        return request.client.host
    return None


def is_internal_request(request: Request) -> bool:
    """Check if request originates from internal VPN/VPC network.

    Returns True if the client IP is within any of the configured CIDR ranges.
    Returns False if no CIDR ranges are configured or if IP doesn't match.
    """
    cidr_ranges = get_internal_cidr_ranges()
    client_ip = get_client_ip(request)

    # Debug logging - remove after testing
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"[AUTH DEBUG] X-Forwarded-For: {request.headers.get('X-Forwarded-For')}")
    logger.warning(f"[AUTH DEBUG] X-Real-IP: {request.headers.get('X-Real-IP')}")
    logger.warning(f"[AUTH DEBUG] request.client.host: {request.client.host if request.client else 'None'}")
    logger.warning(f"[AUTH DEBUG] Resolved client_ip: {client_ip}")
    logger.warning(f"[AUTH DEBUG] CIDR ranges: {cidr_ranges}")

    if not cidr_ranges:
        return False

    if not client_ip:
        return False

    try:
        client_addr = ip_address(client_ip)
        for cidr in cidr_ranges:
            if client_addr in ip_network(cidr):
                return True
    except ValueError:
        # Invalid IP address format
        return False

    return False


# =============================================================================
# Authentication dependency
# =============================================================================

class VPNAwareCognitoAuth:
    """
    Authentication dependency with VPN-aware bypass logic.

    Default behavior:
    - External requests: Always require Cognito auth (or valid session cookie)
    - VPN GET requests: Skip auth
    - VPN non-GET requests: Require auth

    Override with decorators:
    - @skip_auth_for_vpn: Skip auth for VPN on non-GET endpoints
    - @enforce_auth: Always require auth, even for VPN GET

    Session cookie support:
    - Call POST /auth/login with Bearer token to get a session cookie
    - Then access API endpoints directly in browser without Authorization header
    """

    def __init__(self):
        self.http_bearer = HTTPBearer(auto_error=False)

    def _get_user_from_session_cookie(self, request: Request) -> Optional[Dict[str, Any]]:
        """Check for valid session cookie and return user info if found."""
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            return None

        # Import here to avoid circular imports
        from agr_literature_service.api.routers.authentication import get_session_user
        return get_session_user(session_id)

    async def __call__(
        self,
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
    ) -> Optional[Dict[str, Any]]:
        # Get route endpoint to check for decorator flags
        route = request.scope.get("route")
        endpoint = route.endpoint if route else None

        # Check decorator flags
        skip_for_vpn = getattr(endpoint, "_skip_auth_for_vpn", False)
        force_auth = getattr(endpoint, "_enforce_auth", False)

        # Determine if auth should be skipped
        should_skip = self._should_skip_auth(request, skip_for_vpn, force_auth)

        if should_skip:
            return None

        # Auth required - first check for session cookie (for direct browser access)
        session_user = self._get_user_from_session_cookie(request)
        if session_user:
            return session_user

        # Fall back to Bearer token authentication
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing authentication token")
        return get_cognito_user_swagger(credentials, get_cognito_auth())

    def _should_skip_auth(
        self,
        request: Request,
        skip_for_vpn: bool,
        force_auth: bool
    ) -> bool:
        """
        Determine if authentication should be skipped.

        Rules:
        - External requests: NEVER skip
        - @enforce_auth: NEVER skip
        - VPN + GET: Skip (default)
        - VPN + @skip_auth_for_vpn: Skip
        - VPN + non-GET (no decorator): Require auth
        """
        # External requests always require auth
        if not is_internal_request(request):
            return False

        # @enforce_auth always requires auth
        if force_auth:
            return False

        # VPN GET requests skip auth by default
        if request.method == "GET":
            return True

        # VPN non-GET with @skip_auth_for_vpn skips auth
        if skip_for_vpn:
            return True

        # VPN non-GET requests require auth by default
        return False


# The dependency instance to use in routes
get_authenticated_user = VPNAwareCognitoAuth()
