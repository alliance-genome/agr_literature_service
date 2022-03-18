from typing import List, Optional

from pydantic import BaseModel, validator

from literature.schemas import BaseModelShow


class ModSchemaPost(BaseModel):
    abbreviation: str
    short_name: str
    full_name: str

    class Config():
        orm_mode = True
        extra = "forbid"


class ModSchemaShow(BaseModel):
    mod_id: int
    abbreviation: str
    short_name: str
    full_name: str


class ModSchemaCreate(ModSchemaPost):

    class Config():
        orm_mode = True
        extra = "forbid"
