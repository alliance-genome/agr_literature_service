"""Cognito JWT token validation."""
from typing import Dict, Any, Optional
import logging
from jose import jwt, JWTError
from jwt import PyJWKClient

from .config import CognitoConfig

logger = logging.getLogger(__name__)


class CognitoAuth:
    """Cognito authentication and token validation."""

    def __init__(self, config: Optional[CognitoConfig] = None):
        """Initialize Cognito auth with configuration."""
        self.config = config or CognitoConfig()
        self.jwks_client = PyJWKClient(self.config.jwks_url)
        logger.info(f"Initialized Cognito auth for client_id: {self.config.client_id}")

    def validate_token(self, token: str) -> Dict[str, Any]:
        """
        Validate a Cognito JWT token.

        Args:
            token: JWT token string

        Returns:
            Dict containing user information (sub, email, name, groups)

        Raises:
            JWTError: If token is invalid, expired, or for wrong audience
        """
        try:
            # Get signing key from JWKS
            signing_key = self.jwks_client.get_signing_key_from_jwt(token)

            # Decode and validate token
            decoded_token = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=self.config.client_id,
                issuer=self.config.issuer,
                options={"verify_at_hash": False}
            )

            # Validate token_use claim
            token_use = decoded_token.get("token_use")
            if token_use != "id":
                raise JWTError(f"Invalid token_use: '{token_use}' (expected 'id')")

            # Extract and return user information
            user = {
                "sub": decoded_token.get("sub"),
                "email": decoded_token.get("email"),
                "name": decoded_token.get("name") or decoded_token.get("email"),
                "cognito:groups": decoded_token.get("cognito:groups", []),
                "cognito:username": decoded_token.get("cognito:username"),
            }

            logger.debug(f"Validated token for user: {user['email']}")
            return user

        except JWTError as e:
            logger.warning(f"Token validation failed: {e}")
            raise
