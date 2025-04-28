from typing import Optional

from pydantic import ConfigDict, BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ReferencefileModSchemaPost(BaseModel):
    referencefile_id: int
    mod_abbreviation: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ReferencefileModSchemaShow(AuditedObjectModelSchema, ReferencefileModSchemaPost):
    referencefile_mod_id: int


class ReferencefileModSchemaUpdate(BaseModel):
    referencefile_id: Optional[int] = None
    mod_abbreviation: Optional[str] = None


class ReferencefileModSchemaRelated(AuditedObjectModelSchema):
    referencefile_mod_id: int
    mod_abbreviation: Optional[str] = None
