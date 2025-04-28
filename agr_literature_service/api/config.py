from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings

from agr_literature_service.api.schemas import EnvStateSchema


class GlobalConfig(BaseSettings):
    """Global configurations."""

    # This variable will be loaded from the .env file. However, if there is a
    # shell environment variable having the same name, that will take precedence.

    # the class Field is necessary while defining the global variables
    ENV_STATE: EnvStateSchema = Field(..., validation_alias='ENV_STATE')
    HOST: Optional[str] = Field(..., validation_alias='HOST')
    PROD_HOST: Optional[str] = Field(..., validation_alias='HOST')
    BUCKET_NAME: Optional[str] = Field(..., validation_alias='BUCKET_NAME')

    # AWS Creds
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(..., validation_alias='AWS_SECRET_ACCESS_KEY')
    AWS_ACCESS_KEY_ID: Optional[str] = Field(..., validation_alias='AWS_ACCESS_KEY_ID')

    # environment specific configs
    API_USERNAME: Optional[str] = None
    API_PASSWORD: Optional[str] = None

    OKTA_DOMAIN: str = Field(..., validation_alias="OKTA_DOMAIN")
    OKTA_API_AUDIENCE: str = Field(..., validation_alias="OKTA_API_AUDIENCE")

    PSQL_USERNAME: str = Field(..., validation_alias="PSQL_USERNAME")
    PSQL_PASSWORD: str = Field(..., validation_alias="PSQL_PASSWORD")
    PSQL_HOST: str = Field(..., validation_alias="PSQL_HOST")
    PSQL_PORT: str = Field(..., validation_alias="PSQL_PORT")
    PSQL_DATABASE: str = Field(..., validation_alias="PSQL_DATABASE")

    RESOURCE_DESCRIPTOR_URL: str = Field(..., validation_alias="RESOURCE_DESCRIPTOR_URL")
    ELASTICSEARCH_HOST: str = Field(..., validation_alias="ELASTICSEARCH_HOST")
    ELASTICSEARCH_PORT: str = Field(..., validation_alias="ELASTICSEARCH_PORT")
    ELASTICSEARCH_INDEX: str = Field(..., validation_alias="ELASTICSEARCH_INDEX")


config = GlobalConfig()
