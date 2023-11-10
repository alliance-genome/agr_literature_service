from typing import Dict, List

from pydantic import BaseModel


class AteamApiSchemaShow(BaseModel):
    checks: List = []


class DatabaseSchemaShow(BaseModel):
    db_details: Dict = {}


class EnvironmentsSchemaShow(BaseModel):
    envs: Dict = {}
