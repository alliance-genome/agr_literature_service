from typing import List, Optional

from pydantic import BaseModel, validator

from literature.schemas import BaseModelShow, CrossReferenceSchemaShow


class ModSchemaPost(BaseModel):
    abbreviation: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ModSchemaShow(BaseModelShow):
    mod_id: int

    abbreviation: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None


    class Config():
        orm_mode = True
        extra = "forbid"


class ModSchemaCreate(ModSchemaPost):

    class Config():
        orm_mode = True
        extra = "forbid"
