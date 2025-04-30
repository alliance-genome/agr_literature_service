from typing import Optional

from pydantic import BaseModel, constr

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
    date_created: Optional[constr(min_length=1)]  # type: ignore
    date_updated: Optional[constr(min_length=1)]  # type: ignore
    created_by: Optional[constr(min_length=1)]  # type: ignore
    updated_by: Optional[constr(min_length=1)]  # type: ignore

    class Config:
        orm_mode = True
        extra = "forbid"
