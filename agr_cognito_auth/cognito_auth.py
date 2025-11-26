"""Cognito JWT token validation and admin token generation."""
from typing import Dict, Any, Optional
import logging
import time
import requests
from jose import jwt, JWTError
from jwt import PyJWKClient

from .config import CognitoConfig, CognitoAdminConfig

logger = logging.getLogger(__name__)

# Module-level cache for admin tokens
_admin_token_cache: Dict[str, Any] = {
    "token": None,
    "expires_at": 0
}


class CognitoAuth:
    """Cognito authentication and token validation."""

    def __init__(self, config: Optional[CognitoConfig] = None):
        """Initialize Cognito auth with configuration."""
        self.config = config or CognitoConfig()
        self.jwks_client = PyJWKClient(self.config.jwks_url)
        logger.info(f"Initialized Cognito auth for client_id: {self.config.client_id}")

    def validate_token(self, token: str) -> Dict[str, Any]:
        """
        Validate a Cognito JWT token (ID token or access token).

        Supports both:
        - ID tokens: From user login (Authorization Code flow)
        - Access tokens: From machine-to-machine auth (client_credentials flow)

        Args:
            token: JWT token string

        Returns:
            Dict containing user/client information:
            - For ID tokens: sub, email, name, cognito:groups, cognito:username
            - For access tokens: sub, client_id, scope, token_type

        Raises:
            JWTError: If token is invalid, expired, or has invalid claims
        """
        try:
            # Get signing key from JWKS
            signing_key = self.jwks_client.get_signing_key_from_jwt(token)

            # First decode without audience verification to check token_use
            unverified_claims = jwt.get_unverified_claims(token)
            token_use = unverified_claims.get("token_use")

            if token_use == "id":
                # ID token: decode without automatic audience check, then validate manually
                # This supports tokens from multiple clients in the same user pool
                # (e.g., UI client + API client)
                decoded_token = jwt.decode(
                    token,
                    signing_key.key,
                    algorithms=["RS256"],
                    issuer=self.config.issuer,
                    options={"verify_aud": False, "verify_at_hash": False}
                )
                # Manually validate audience against allowed client IDs
                token_audience = decoded_token.get("aud")
                if token_audience not in self.config.allowed_client_ids:
                    raise JWTError(
                        f"Invalid audience: '{token_audience}' not in allowed clients"
                    )
                return self._extract_id_token_info(decoded_token)

            elif token_use == "access":
                # Access token: no audience claim, validate issuer only
                decoded_token = jwt.decode(
                    token,
                    signing_key.key,
                    algorithms=["RS256"],
                    issuer=self.config.issuer,
                    options={"verify_aud": False, "verify_at_hash": False}
                )
                return self._extract_access_token_info(decoded_token)

            else:
                raise JWTError(f"Invalid token_use: '{token_use}' (expected 'id' or 'access')")

        except JWTError as e:
            logger.warning(f"Token validation failed: {e}")
            raise

    def _extract_id_token_info(self, decoded_token: Dict[str, Any]) -> Dict[str, Any]:
        """Extract user information from an ID token."""
        user = {
            "sub": decoded_token.get("sub"),
            "email": decoded_token.get("email"),
            "name": decoded_token.get("name") or decoded_token.get("email"),
            "cognito:groups": decoded_token.get("cognito:groups", []),
            "cognito:username": decoded_token.get("cognito:username"),
            "token_type": "id",
        }
        logger.debug(f"Validated ID token for user: {user['email']}")
        return user

    def _extract_access_token_info(self, decoded_token: Dict[str, Any]) -> Dict[str, Any]:
        """Extract client information from an access token (client_credentials flow)."""
        client_id = decoded_token.get("client_id")
        scope = decoded_token.get("scope", "")

        user = {
            "sub": decoded_token.get("sub"),
            "client_id": client_id,
            "scope": scope,
            "token_type": "access",
            # For compatibility with code expecting user fields:
            "email": f"{client_id}@service.local",
            "name": f"Service Client ({client_id})",
            "cognito:groups": ["ServiceAccount"],  # Treat service accounts as a special group
            "cognito:username": client_id,
        }
        logger.debug(f"Validated access token for client: {client_id}")
        return user


def get_admin_token(config: Optional[CognitoAdminConfig] = None, force_refresh: bool = False) -> str:
    """
    Get admin access token using client_credentials OAuth flow.

    Uses module-level caching to avoid unnecessary token requests.
    Tokens are refreshed when they expire or when force_refresh=True.

    Args:
        config: Optional CognitoAdminConfig instance. If not provided,
                uses environment variables.
        force_refresh: If True, forces a new token request regardless of cache.

    Returns:
        Access token string

    Raises:
        ValueError: If required configuration is missing
        requests.HTTPError: If token request fails
    """
    global _admin_token_cache

    # Check if cached token is still valid (with 60-second buffer)
    if not force_refresh and _admin_token_cache["token"]:
        if time.time() < _admin_token_cache["expires_at"] - 60:
            logger.debug("Using cached admin token")
            return _admin_token_cache["token"]

    # Get configuration
    cfg = config or CognitoAdminConfig()

    if not cfg.token_url:
        raise ValueError("COGNITO_TOKEN_URL environment variable is not set")

    logger.info("Requesting new Cognito admin token")
    response = requests.post(
        cfg.token_url,
        auth=(cfg.client_id, cfg.client_secret),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "scope": cfg.scope
        }
    )
    response.raise_for_status()

    token_data = response.json()
    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)

    # Cache the token
    _admin_token_cache["token"] = access_token
    _admin_token_cache["expires_at"] = time.time() + expires_in

    logger.info(f"Obtained new admin token, expires in {expires_in} seconds")
    return access_token


def generate_headers(token: str) -> Dict[str, str]:
    """
    Generate HTTP headers for authenticated API requests.

    Args:
        token: Bearer token string

    Returns:
        Dict containing Authorization, Content-Type, and Accept headers
    """
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def get_authentication_token(config: Optional[CognitoAdminConfig] = None) -> str:
    """
    Get authentication token for API requests using client credentials flow.

    This is a convenience wrapper around get_admin_token() for backward compatibility.

    Args:
        config: Optional CognitoAdminConfig instance

    Returns:
        Access token string
    """
    return get_admin_token(config)


def clear_token_cache() -> None:
    """Clear the cached admin token. Useful for testing."""
    global _admin_token_cache
    _admin_token_cache["token"] = None
    _admin_token_cache["expires_at"] = 0
    logger.debug("Admin token cache cleared")
