"""
IP-aware authentication module for the AGR Literature Service.

This module provides authentication decorators and dependencies that allow:
- External requests: Always require Cognito authentication
- Trusted IP requests: Configurable auth bypass based on IP address
- Browser session: Cookie-based auth for direct browser access

Environment variables:
- SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP: Comma-separated IPs/CIDRs that skip auth on GET
- SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP: Comma-separated IPs/CIDRs that skip auth entirely

Decorators:
- @read_auth_bypass: Allow read-level IPs to bypass auth on non-GET endpoints
- @no_read_auth_bypass: Block read-level IPs from bypassing auth (full-bypass IPs still work)
"""

import os
from ipaddress import ip_address, ip_network
from typing import Any, Callable, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from agr_cognito_py import get_cognito_auth, get_cognito_user_swagger

# Session cookie name - must match authentication.py
SESSION_COOKIE_NAME = "agr_session"

# Default user dict returned when auth is completely bypassed (full-bypass IPs)
# Mimics an access token structure so set_global_user_from_cognito handles it correctly
# Note: default_user has no person_id in DB, so email and name are null
DEFAULT_BYPASS_USER: Dict[str, Any] = {
    "token_type": "access",
    "sub": "default_user",
    "cognito:groups": []
}


# =============================================================================
# Route decorators - use these to flag special auth behavior
# =============================================================================

def read_auth_bypass(func: Callable) -> Callable:
    """
    Decorator to allow read-level IPs to bypass auth on non-GET endpoints.
    Use this for POST/PATCH/DELETE endpoints that should be accessible
    to IPs in SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP.

    Example:
        @router.post('/show_all')
        @read_auth_bypass
        def show_all(...):
            ...
    """
    setattr(func, "_read_auth_bypass", True)  # noqa: B010
    return func


def no_read_auth_bypass(func: Callable) -> Callable:
    """
    Decorator to block read-level IPs from bypassing auth on this endpoint.

    Use this for sensitive endpoints that should require auth even for
    IPs in SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP. IPs in the full-bypass list
    (SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP) can still access without auth.

    Example:
        @router.get('/sensitive_data')
        @no_read_auth_bypass
        def get_sensitive_data(...):
            ...
    """
    setattr(func, "_no_read_auth_bypass", True)  # noqa: B010
    return func


# =============================================================================
# IP-based auth bypass utilities
# =============================================================================

def _parse_ip_list(env_var: str) -> List[str]:
    """Parse comma-separated IP/CIDR list from environment variable."""
    ranges = os.environ.get(env_var, "")
    if not ranges:
        return []
    return [r.strip() for r in ranges.split(",") if r.strip()]


def get_read_skip_ip_ranges() -> List[str]:
    """Get IPs/CIDRs that skip auth on read (GET) endpoints.

    Set SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP with comma-separated IPs or CIDR ranges.
    Examples:
        SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP=10.0.0.1
        SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP=10.0.0.0/8,172.16.0.0/12
        SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP=192.168.1.100,10.0.0.0/24
    """
    return _parse_ip_list("SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP")


def get_all_skip_ip_ranges() -> List[str]:
    """Get IPs/CIDRs that skip auth on ALL endpoints (read and write).

    Set SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP with comma-separated IPs or CIDR ranges.
    Examples:
        SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP=10.0.0.1
        SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP=10.0.0.0/8,172.16.0.0/12
    """
    return _parse_ip_list("SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP")


def get_client_ip(request: Request) -> Optional[str]:
    """Extract primary client IP from request, handling load balancer scenarios.

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


def get_all_client_ips(request: Request) -> List[str]:
    """Extract all possible client IPs from request for whitelist checking.

    Returns both the forwarded IP (original client) and the direct connection IP.
    This allows whitelisting both external IPs and internal Docker/container IPs.

    Returns:
        List of unique IP addresses (may be empty, 1, or 2 IPs)
    """
    ips = []

    # Get the forwarded IP (original client behind load balancer)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        forwarded_ip = forwarded_for.split(",")[0].strip()
        if forwarded_ip:
            ips.append(forwarded_ip)

    # Get the direct connection IP (e.g., Docker container IP)
    if request.client and request.client.host:
        direct_ip = request.client.host
        # Avoid duplicates
        if direct_ip not in ips:
            ips.append(direct_ip)

    return ips


def _ip_in_ranges(client_ips: List[str], ip_ranges: List[str]) -> bool:
    """Check if any client IP is within any of the given IP/CIDR ranges.

    Args:
        client_ips: List of client IP address strings
        ip_ranges: List of IP addresses or CIDR ranges

    Returns:
        True if any client_ip matches any range, False otherwise
    """
    if not ip_ranges or not client_ips:
        return False

    import logging
    logger = logging.getLogger(__name__)

    for client_ip in client_ips:
        try:
            client_addr = ip_address(client_ip)
            for cidr in ip_ranges:
                try:
                    # strict=False allows host addresses like 10.0.0.5/24
                    if client_addr in ip_network(cidr, strict=False):
                        return True
                except ValueError:
                    # Invalid CIDR format, skip this entry
                    logger.warning(f"[AUTH] Invalid IP/CIDR format: {cidr}")
                    continue
        except ValueError:
            # Invalid client IP address format, try next IP
            logger.debug(f"[AUTH] Invalid client IP format: {client_ip}")
            continue

    return False


def is_skip_all_auth_ip(request: Request) -> bool:
    """Check if request should skip ALL authentication (read and write).

    Returns True if any client IP matches SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP.
    Checks both the forwarded IP and the direct connection IP.
    """
    client_ips = get_all_client_ips(request)
    ip_ranges = get_all_skip_ip_ranges()

    # Debug logging
    import logging
    logger = logging.getLogger(__name__)
    logger.debug(f"[AUTH] Checking skip-all auth for IPs: {client_ips}, ranges: {ip_ranges}")

    return _ip_in_ranges(client_ips, ip_ranges)


def is_skip_read_auth_ip(request: Request) -> bool:
    """Check if request should skip auth on read (GET) endpoints.

    Returns True if any client IP matches SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP.
    Checks both the forwarded IP and the direct connection IP.
    """
    client_ips = get_all_client_ips(request)
    ip_ranges = get_read_skip_ip_ranges()

    # Debug logging
    import logging
    logger = logging.getLogger(__name__)
    logger.debug(f"[AUTH] Checking skip-read auth for IPs: {client_ips}, ranges: {ip_ranges}")

    return _ip_in_ranges(client_ips, ip_ranges)


# =============================================================================
# Authentication dependency
# =============================================================================

class IPAwareCognitoAuth:
    """
    Authentication dependency with IP-based bypass logic.

    Priority order:
    1. Bearer token: If provided, always authenticate with it (returns real user)
    2. Session cookie: If present, use session-based auth
    3. IP bypass rules: Only checked when no credentials provided
       - SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP: Returns DEFAULT_BYPASS_USER
       - SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP + GET: Returns DEFAULT_BYPASS_USER
    4. No credentials and not in bypass: Returns 401 Unauthorized

    Override with decorators:
    - @read_auth_bypass: Allow read-level IPs to bypass auth on non-GET endpoints
    - @no_read_auth_bypass: Block read-level IPs from bypassing (full-bypass still works)

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
        # Priority 1: If Bearer token is provided, authenticate with it
        # This takes precedence over IP bypass so authenticated users get their real identity
        if credentials is not None:
            return get_cognito_user_swagger(credentials, get_cognito_auth())

        # Priority 2: Check for session cookie (for direct browser access)
        session_user = self._get_user_from_session_cookie(request)
        if session_user:
            return session_user

        # Priority 3: No credentials provided - check IP bypass rules
        route = request.scope.get("route")
        endpoint = route.endpoint if route else None

        # Check decorator flags
        read_bypass = getattr(endpoint, "_read_auth_bypass", False)
        no_read_bypass = getattr(endpoint, "_no_read_auth_bypass", False)

        # Check auth skip conditions
        skip_result = self._check_auth_skip(request, read_bypass, no_read_bypass)

        if skip_result in ("full_bypass", "read_bypass"):
            # Bypass IP: return default user so endpoints have a valid user object
            return DEFAULT_BYPASS_USER

        # No credentials and not in bypass list - require auth
        raise HTTPException(status_code=401, detail="Missing authentication token")

    def _check_auth_skip(
        self,
        request: Request,
        read_bypass: bool,
        no_read_bypass: bool
    ) -> Optional[str]:
        """
        Check if authentication should be skipped and return the bypass type.

        Returns:
            "full_bypass" - IP in SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP (returns default user)
            "read_bypass" - IP in SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP on allowed endpoint
            None - Auth required

        Priority order:
        1. SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP: Full bypass (highest priority)
        2. @no_read_auth_bypass decorator: Block read-level bypass
        3. SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP + GET: Read bypass
        4. SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP + @read_auth_bypass: Read bypass
        5. Otherwise: Require auth
        """
        # Full-bypass IPs skip auth on ALL endpoints (highest priority)
        if is_skip_all_auth_ip(request):
            return "full_bypass"

        # @no_read_auth_bypass blocks read-level bypass
        if no_read_bypass:
            return None

        # Check if IP should skip read-only auth
        if is_skip_read_auth_ip(request):
            # GET requests skip auth
            if request.method == "GET":
                return "read_bypass"

            # Non-GET with @read_auth_bypass decorator skips auth
            if read_bypass:
                return "read_bypass"

        # All other cases require auth
        return None


# The dependency instance to use in routes
get_authenticated_user = IPAwareCognitoAuth()
