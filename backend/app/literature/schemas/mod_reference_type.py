from typing import List
from typing import Optional

from pydantic import BaseModel


class ModReferenceTypeSchemaCreate(BaseModel):
    reference_type: str
    source: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ModReferenceTypeSchemaPost(ModReferenceTypeSchemaCreate):
    reference_curie: str

    class Config():
        orm_mode = True
        extra = "forbid"


class ModReferenceTypeSchemaShow(ModReferenceTypeSchemaCreate):
    mod_reference_type_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ModReferenceTypeSchemaUpdate(ModReferenceTypeSchemaShow):
    reference_curie: str

    class Config():
        orm_mode = True
        extra = "forbid"
