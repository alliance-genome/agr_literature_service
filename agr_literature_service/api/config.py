from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from agr_literature_service.api.schemas import EnvStateSchema


class GlobalConfig(BaseSettings):
    """Global configurations."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    ENV_STATE: EnvStateSchema = Field(..., env="ENV_STATE")
    HOST: Optional[str] = Field(None, env="HOST")
    PROD_HOST: Optional[str] = Field(None, env="PROD_HOST")
    BUCKET_NAME: str = Field("agr-literature", env="BUCKET_NAME")

    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    AWS_ACCESS_KEY_ID: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")

    OKTA_DOMAIN: str = Field(..., env="OKTA_DOMAIN")
    OKTA_API_AUDIENCE: str = Field(..., env="OKTA_API_AUDIENCE")

    OKTA_CLIENT_ID: Optional[str] = Field(None, env="OKTA_CLIENT_ID")
    OKTA_CLIENT_SECRET: Optional[str] = Field(None, env="OKTA_CLIENT_SECRET")

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
