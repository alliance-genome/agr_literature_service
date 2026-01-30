"""
Global configuration module for AGR Literature Service.

TEST: Claude Code Review Verification - Jan 30, 2026
This PR tests the Claude Code v1 migration with actions:read permission fix.
Please delete this comment block after verifying the review works.
"""
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from agr_literature_service.api.schemas import EnvStateSchema


class GlobalConfig(BaseSettings):
    """Global configurations."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_file_encoding='utf-8',
        extra='ignore',
    )

    ENV_STATE: EnvStateSchema = Field(..., env="ENV_STATE")
    HOST: Optional[str] = Field(None, env="HOST")
    PROD_HOST: Optional[str] = Field(None, env="PROD_HOST")
    BUCKET_NAME: str = Field("agr-literature", env="BUCKET_NAME")

    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    AWS_ACCESS_KEY_ID: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")

    # Cognito authentication settings
    COGNITO_REGION: Optional[str] = Field(None, env="COGNITO_REGION")
    COGNITO_USER_POOL_ID: Optional[str] = Field(None, env="COGNITO_USER_POOL_ID")
    COGNITO_CLIENT_ID: Optional[str] = Field(None, env="COGNITO_CLIENT_ID")
    COGNITO_ADMIN_CLIENT_ID: Optional[str] = Field(None, env="COGNITO_ADMIN_CLIENT_ID")
    COGNITO_ADMIN_CLIENT_SECRET: Optional[str] = Field(None, env="COGNITO_ADMIN_CLIENT_SECRET")
    COGNITO_TOKEN_URL: Optional[str] = Field(None, env="COGNITO_TOKEN_URL")

    PSQL_USERNAME: str = Field(..., env="PSQL_USERNAME")
    PSQL_PASSWORD: str = Field(..., env="PSQL_PASSWORD")
    PSQL_HOST: str = Field(..., env="PSQL_HOST")
    PSQL_PORT: str = Field(..., env="PSQL_PORT")
    PSQL_DATABASE: str = Field(..., env="PSQL_DATABASE")

    RESOURCE_DESCRIPTOR_URL: str = Field(..., env="RESOURCE_DESCRIPTOR_URL")
    ELASTICSEARCH_HOST: str = Field(..., env="ELASTICSEARCH_HOST")
    ELASTICSEARCH_PORT: str = Field(..., env="ELASTICSEARCH_PORT")
    ELASTICSEARCH_INDEX: str = Field(..., env="ELASTICSEARCH_INDEX")


# instantiate once for application‑wide use
config = GlobalConfig()
