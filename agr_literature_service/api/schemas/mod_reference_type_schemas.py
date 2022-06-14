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


class ModReferenceTypeSchemaShow(ModReferenceTypeSchemaPost):
    mod_reference_type_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ModReferenceTypeSchemaRelated(ModReferenceTypeSchemaCreate):
    mod_reference_type_id: int

    class Config():
        orm_mode = True
        extra = "forbid"


class ModReferenceTypeSchemaUpdate(BaseModel):
    reference_curie: Optional[str]
    reference_type: Optional[str]
    source: Optional[str]

    class Config():
        orm_mode = True
        extra = "forbid"
