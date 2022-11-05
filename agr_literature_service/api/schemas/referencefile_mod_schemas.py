from typing import Optional

from pydantic import BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ReferencefileModSchemaPost(BaseModel):
    referencefile_id: int
    mod_abbreviation: str

    class Config:
        orm_mode = True
        extra = "forbid"


class ReferencefileModSchemaShow(AuditedObjectModelSchema, ReferencefileModSchemaPost):
    referencefile_mod_id: int


class ReferencefileModSchemaUpdate(BaseModel):
    referencefile_id: Optional[int]
    mod_abbreviation: Optional[str]


class ReferencefileModSchemaRelated(ReferencefileModSchemaShow):
    pass
