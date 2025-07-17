from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from agr_literature_service.api.schemas import EnvStateSchema


class GlobalConfig(BaseSettings):
    """Global configurations."""

    # load .env automatically; ignore any extra keys in the environment
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',         # <— allow unknown vars
    )

    # always required
    ENV_STATE: EnvStateSchema = Field(..., env='ENV_STATE')

    # make these optional
    HOST: Optional[str] = Field(None, env='HOST')
    PROD_HOST: Optional[str] = Field(None, env='PROD_HOST')

    BUCKET_NAME: str = Field('agr-literature', env='BUCKET_NAME')

    # AWS Creds (optional if, e.g., running in ECS with IAM role)
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(None, env='AWS_SECRET_ACCESS_KEY')
    AWS_ACCESS_KEY_ID: Optional[str] = Field(None, env='AWS_ACCESS_KEY_ID')

    # environment specific configs (now correctly optional)
    API_USERNAME: Optional[str] = Field(None, env='API_USERNAME')
    API_PASSWORD: Optional[str] = Field(None, env='API_PASSWORD')

    OKTA_DOMAIN: str = Field(..., env='OKTA_DOMAIN')
    OKTA_API_AUDIENCE: str = Field(..., env='OKTA_API_AUDIENCE')

    PSQL_USERNAME: str = Field(..., env='PSQL_USERNAME')
    PSQL_PASSWORD: str = Field(..., env='PSQL_PASSWORD')
    PSQL_HOST: str = Field(..., env='PSQL_HOST')
    PSQL_PORT: str = Field(..., env='PSQL_PORT')
    PSQL_DATABASE: str = Field(..., env='PSQL_DATABASE')

    RESOURCE_DESCRIPTOR_URL: str = Field(..., env='RESOURCE_DESCRIPTOR_URL')
    ELASTICSEARCH_HOST: str = Field(..., env='ELASTICSEARCH_HOST')
    ELASTICSEARCH_PORT: str = Field(..., env='ELASTICSEARCH_PORT')
    ELASTICSEARCH_INDEX: str = Field(..., env='ELASTICSEARCH_INDEX')


# instantiate once for application-wide use
config = GlobalConfig()
