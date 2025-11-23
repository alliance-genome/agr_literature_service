"""Configuration for Cognito authentication."""
import os
from typing import Optional


class CognitoConfig:
    """Cognito configuration settings."""

    def __init__(
        self,
        region: Optional[str] = None,
        user_pool_id: Optional[str] = None,
        client_id: Optional[str] = None
    ):
        self.region = region or os.getenv("COGNITO_REGION", "us-east-1")
        self.user_pool_id = user_pool_id or os.getenv("COGNITO_USER_POOL_ID", "us-east-1_d3eK6SYpI")
        self.client_id = client_id or os.getenv("COGNITO_CLIENT_ID")

        if not self.client_id:
            raise ValueError("COGNITO_CLIENT_ID must be set in environment or passed to CognitoConfig")

        self.issuer = f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"
        self.jwks_url = f"{self.issuer}/.well-known/jwks.json"
