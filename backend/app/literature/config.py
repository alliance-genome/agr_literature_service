from typing import Optional

from pydantic import BaseSettings, Field

from literature.schemas import EnvStateSchema
from os import path, environ
import sys


class GlobalConfig(BaseSettings):
    """Global configurations."""

    # This variable will be loaded from the .env file. However, if there is a
    # shell environment variable having the same name, that will take precedence.

    # the class Field is necessary while defining the global variables
    ENV_STATE: Optional[EnvStateSchema] = Field(..., env="ENV_STATE")
    HOST: Optional[str] = Field(..., env="HOST")
    PROD_HOST: Optional[str] = Field(..., env="HOST")

    # AWS Creds
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(..., env="AWS_SECRET_ACCESS_KEY")
    AWS_ACCESS_KEY_ID: Optional[str] = Field(..., env="AWS_ACCESS_KEY_ID")

    # environment specific configs
    API_USERNAME: Optional[str] = None
    API_PASSWORD: Optional[str] = None

    OKTA_DOMAIN: Optional[str] = Field(..., env="OKTA_DOMAIN")
    OKTA_API_AUDIENCE: Optional[str] = Field(..., env="OKTA_API_AUDIENCE")

    PSQL_USERNAME: Optional[str] = Field(..., env="PSQL_USERNAME")
    PSQL_PASSWORD: Optional[str] = Field(..., env="PSQL_PASSWORD")
    PSQL_HOST: Optional[str] = Field(..., env="PSQL_HOST")
    PSQL_PORT: Optional[str] = Field(..., env="PSQL_PORT")
    PSQL_DATABASE: Optional[str] = Field(..., env="PSQL_DATABASE")

    RESOURCE_DESCRIPTOR_URL: str = Field(..., env="RESOURCE_DESCRIPTOR_URL")

    class Config:
        """Loads the dotenv file."""
        env_state = environ.get('ENV_STATE', 'prod')
        print("State is {}".format(env_state))
        if env_state == "prod":
            env_file = path.dirname((sys.modules[__name__].__file__)) + "/.env"
        elif env_state == "test":
            env_file = path.dirname((sys.modules[__name__].__file__)) + "/.test_env"


config = GlobalConfig()
