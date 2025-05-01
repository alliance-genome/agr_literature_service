from typing import Optional

from pydantic import BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class CurationStatusSchemaBase(AuditedObjectModelSchema):
    mod_abbreviation: str
    reference_curie: str


class CurationStatusSchemaPost(CurationStatusSchemaBase):
    topic: str
    curation_status: Optional[str]
    controlled_note: Optional[str]
    note: Optional[str]


class CurationStatusSchemaUpdate(BaseModel):
    curation_status: Optional[str]
    controlled_note: Optional[str]
    note: Optional[str]

    class Config:
        orm_mode = True
        extra = "forbid"
