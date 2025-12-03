"""Configuration for Cognito authentication."""
import os
from typing import Optional, List


class CognitoConfig:
    """Cognito configuration settings."""

    def __init__(
        self,
        region: Optional[str] = None,
        user_pool_id: Optional[str] = None,
        client_id: Optional[str] = None,
        allowed_client_ids: Optional[List[str]] = None
    ):
        self.region = region or os.getenv("COGNITO_REGION", "us-east-1")
        self.user_pool_id = user_pool_id or os.getenv("COGNITO_USER_POOL_ID", "us-east-1_d3eK6SYpI")
        self.client_id = client_id or os.getenv("COGNITO_CLIENT_ID")

        if not self.client_id:
            raise ValueError("COGNITO_CLIENT_ID must be set in environment or passed to CognitoConfig")

        # Build list of allowed client IDs for audience validation
        # Supports multiple clients from the same user pool (e.g., UI + API clients)
        self.allowed_client_ids = self._build_allowed_client_ids(allowed_client_ids)

        self.issuer = f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"
        self.jwks_url = f"{self.issuer}/.well-known/jwks.json"

    def _build_allowed_client_ids(self, allowed_client_ids: Optional[List[str]]) -> List[str]:
        """Build list of allowed client IDs from parameter and environment."""
        if allowed_client_ids:
            return allowed_client_ids

        # Start with the primary client ID (validated as non-None before this is called)
        assert self.client_id is not None
        client_ids: List[str] = [self.client_id]

        # Add additional allowed client IDs from environment (comma-separated)
        # e.g., COGNITO_ALLOWED_CLIENT_IDS=client1,client2,client3
        additional_ids = os.getenv("COGNITO_ALLOWED_CLIENT_IDS", "")
        if additional_ids:
            client_ids.extend([cid.strip() for cid in additional_ids.split(",") if cid.strip()])

        return list(set(client_ids))  # Remove duplicates


class CognitoAdminConfig:
    """Configuration for Cognito admin/machine-to-machine authentication.

    Used for API unit tests and service-to-service communication.
    """

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token_url: Optional[str] = None
    ):
        self.client_id = client_id or os.getenv("COGNITO_ADMIN_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("COGNITO_ADMIN_CLIENT_SECRET")
        self.token_url = token_url or os.getenv(
            "COGNITO_TOKEN_URL",
            "https://auth.alliancegenome.org/oauth2/token"
        )
        self.scope = "curation-api/admin"

        if not self.client_id:
            raise ValueError("COGNITO_ADMIN_CLIENT_ID must be set in environment or passed to CognitoAdminConfig")
        if not self.client_secret:
            raise ValueError("COGNITO_ADMIN_CLIENT_SECRET must be set in environment or passed to CognitoAdminConfig")
