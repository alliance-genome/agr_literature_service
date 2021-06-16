from enum import Enum

class EnvStateSchema(str, Enum):
    prod = "prod"
    develop = "build"
