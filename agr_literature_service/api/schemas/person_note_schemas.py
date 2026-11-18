from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class PersonNoteSchemaCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    note: str
    # internal: a private curator note; false (the default) is public
    internal: bool = False
    obsolete: bool = False

    @field_validator("note")
    @classmethod
    def _validate_note(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("note cannot be empty or whitespace")
        return v.strip()


class PersonNoteSchemaPost(PersonNoteSchemaCreate):
    """Standalone create payload — names the owning person by curie (or id) in the body."""
    person_curie: str


class PersonNoteSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    note: Optional[str] = None
    internal: Optional[bool] = None
    obsolete: Optional[bool] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_null(cls, data: Any) -> Any:
        # note, internal and obsolete are NOT NULL in the DB — reject explicit null
        # from clients. Omitting a field is still fine (PATCH no-op for that field).
        if isinstance(data, dict):
            for field in ("note", "internal", "obsolete"):
                if field in data and data[field] is None:
                    raise ValueError(f"{field} cannot be null; omit the field to leave it unchanged")
        return data

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
    person_curie: Optional[str] = None
    note: str
    internal: bool
    obsolete: bool


class PersonNoteSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_note_id: int
    person_curie: Optional[str] = None
    note: str
    internal: bool
    obsolete: bool
