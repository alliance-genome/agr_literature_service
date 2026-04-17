from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class PersonNoteSchemaCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    note: str


class PersonNoteSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    note: Optional[str] = None


class PersonNoteSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_note_id: int
    person_id: int
    note: str


class PersonNoteSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_note_id: int
    note: str
