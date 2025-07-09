from typing import Optional

from pydantic import  Field
from pydantic_settings import BaseSettings
from agr_literature_service.api.schemas import EnvStateSchema


class GlobalConfig(BaseSettings):
    """Global configurations."""

    # This variable will be loaded from the .env file. However, if there is a
    # shell environment variable having the same name, that will take precedence.

    # the class Field is necessary while defining the global variables
    ENV_STATE: EnvStateSchema = Field(..., env="ENV_STATE")
    HOST: Optional[str] = Field(..., env="HOST")
    PROD_HOST: Optional[str] = Field(..., env="HOST")
    BUCKET_NAME: str = Field(default="agr-literature", env="BUCKET_NAME")

    # AWS Creds
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(..., env="AWS_SECRET_ACCESS_KEY")
    AWS_ACCESS_KEY_ID: Optional[str] = Field(..., env="AWS_ACCESS_KEY_ID")

    # environment specific configs
    API_USERNAME: Optional[str] = None
    API_PASSWORD: Optional[str] = None

    OKTA_DOMAIN: str = Field(..., env="OKTA_DOMAIN")
    OKTA_API_AUDIENCE: str = Field(..., env="OKTA_API_AUDIENCE")

    PSQL_USERNAME: str = Field(..., env="PSQL_USERNAME")
    PSQL_PASSWORD: str = Field(..., env="PSQL_PASSWORD")
    PSQL_HOST: str = Field(..., env="PSQL_HOST")
    PSQL_PORT: str = Field(..., env="PSQL_PORT")
    PSQL_DATABASE: str = Field(..., env="PSQL_DATABASE")

    RESOURCE_DESCRIPTOR_URL: str = Field(..., env="RESOURCE_DESCRIPTOR_URL")
    ELASTICSEARCH_HOST: str = Field(..., env="ELASTICSEARCH_HOST")
    ELASTICSEARCH_PORT: str = Field(..., env="ELASTICSEARCH_PORT")
    ELASTICSEARCH_INDEX: str = Field(..., env="ELASTICSEARCH_INDEX")


config = GlobalConfig()
