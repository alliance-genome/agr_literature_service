
from typing import Set
import inspect
from pydantic import (
    BaseModel,
    Field
)
from pydantic_settings import BaseSettings
from os import environ
from agr_literature_service.api.schemas import EnvStateSchema


class SubModel(BaseModel):
    foo = 'bar'
    apple = 1


class Settings(BaseSettings):
    api_key: str = Field(..., env='my_api_key')
    FROM_ENV_FILE: str
    OVERRIDE: str = Field(..., env='OVERRIDE')

    # to override domains:
    # export my_prefix_domains='["foo.com", "bar.com"]'
    domains: Set[str] = set()

    # to override more_settings:
    # export my_prefix_more_settings='{"foo": "x", "apple": 1}'
    more_settings: SubModel = SubModel()

    class Config:
        # env_prefix = 'my_prefix_'  # defaults to no prefix, i.e. ""
        # env_file = ".env"
        env_file = environ.get('env_file', '.env')
        print(env_file)


print(inspect.getfile(EnvStateSchema))
print(Settings().dict())
