from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class PersonNoteSchemaCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    note: str

    @field_validator("note")
    @classmethod
    def _validate_note(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("note cannot be empty or whitespace")
        return v.strip()


class PersonNoteSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    note: Optional[str] = None

    @field_validator("note")
    @classmethod
    def _validate_note(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not v.strip():
            raise ValueError("note cannot be empty or whitespace")
        return v.strip()


class PersonNoteSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_note_id: int
    person_id: int
    note: str


class PersonNoteSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_note_id: int
    note: str
