from typing import Optional

from pydantic import BaseSettings, Field

from literature.schemas import EnvStateSchema


class GlobalConfig(BaseSettings):
    """Global configurations."""

    # This variable will be loaded from the .env file. However, if there is a
    # shell environment variable having the same name, that will take precedence.

    # the class Field is necessary while defining the global variables
    ENV_STATE: Optional[EnvStateSchema] = Field(..., env="ENV_STATE")
    HOST: Optional[str] = Field(..., env="HOST")

    # AWS Creds
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(..., env="AWS_SECRET_ACCESS_KEY")
    AWS_ACCESS_KEY_ID: Optional[str] = Field(..., env="AWS_ACCESS_KEY_ID")

    # environment specific configs
    API_USERNAME: Optional[str] = None
    API_PASSWORD: Optional[str] = None

    OKTA_DOMAIN: Optional[str] = None
    OKTA_API_AUDIENCE: Optional[str] = None

    PSQL_USERNAME: Optional[str] = None
    PSQL_PASSWORD: Optional[str] = None
    PSQL_HOST: Optional[str] = None
    PSQL_PORT: Optional[str] = None
    PSQL_DATABASE: Optional[str] = None

    RESOURCE_DESCRIPTOR_URL: str

    class Config:
        """Loads the dotenv file."""

        env_file: str = ".env"
        print("env_file: {}".format(env_file))


class DevConfig(GlobalConfig):
    """Development configurations."""

    class Config:
        env_prefix: str = ""


class ProdConfig(GlobalConfig):
    """Production configurations."""

    class Config:
        env_prefix: str = "PROD_"


class TestConfig(GlobalConfig):
    """Production configurations."""

    class Config:
        env_file: str = "test.env"


class FactoryConfig:
    """Returns a config instance dependending on the ENV_STATE variable."""

    def __init__(self, env_state: Optional[str]):
        self.env_state = env_state
        print("State: {}".format(env_state))

    def __call__(self):
        if self.env_state == "build":
            return DevConfig()
        elif self.env_state == "prod":
            return ProdConfig()
        elif self.env_state == "test":
            return TestConfig()


config = FactoryConfig(GlobalConfig().ENV_STATE)()
