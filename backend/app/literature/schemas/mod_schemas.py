from typing import Optional
from pydantic import BaseModel
from literature.schemas import BaseModelShow


class ModSchemaPost(BaseModel):
    abbreviation: str
    short_name: str
    full_name: str

    class Config():
        orm_mode = True
        extra = "forbid"


class ModSchemaShow(BaseModelShow):
    mod_id: int
    abbreviation: str
    short_name: str
    full_name: str


class ModSchemaUpdate(BaseModel):
    abbreviation: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None


class ModSchemaCreate(ModSchemaPost):

    class Config():
        orm_mode = True
        extra = "forbid"
