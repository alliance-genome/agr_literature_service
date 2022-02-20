
from typing import Set
import inspect
from pydantic import (
    BaseModel,
    BaseSettings,
    PyObject,
    RedisDsn,
    PostgresDsn,
    # AmqpDsn,
    Field,
)
from os import environ, path
from literature.schemas import EnvStateSchema
import sys

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
        print( __loader__.name)
        env_file = environ.get('env_file', '.env')
        print(env_file)

print(inspect.getfile(EnvStateSchema))
print(Settings().dict())
print( __loader__.name)
print(path.dirname((sys.modules[__name__].__file__)))