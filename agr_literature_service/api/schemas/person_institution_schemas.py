from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _normalize_institution(v: str) -> str:
    """Trim surrounding whitespace and reject a blank value. Casing is preserved
    for storage; institutions are uncontrolled free text (see SCRUM-5910), so
    there is no further normalization to apply."""
    v = (v or "").strip()
    if not v:
        raise ValueError("institution must not be blank")
    return v


class _InstitutionMixin(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    institution: str

    @field_validator("institution")
    @classmethod
    def _validate_institution(cls, v: str) -> str:
        return _normalize_institution(v)


class PersonInstitutionSchemaCreate(_InstitutionMixin):
    """Payload for creating a person_institution row."""
    date_made_old_institution: Optional[datetime] = None


class PersonInstitutionSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    institution: Optional[str] = None
    date_made_old_institution: Optional[datetime] = None

    @field_validator("institution")
    @classmethod
    def _validate_institution(cls, v: Optional[str]) -> Optional[str]:
        return _normalize_institution(v) if v is not None else None


class PersonInstitutionSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_institution_id: int
    person_id: int
    person_curie: Optional[str] = None
    institution: str
    date_made_old_institution: Optional[datetime] = None


class PersonInstitutionSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_institution_id: int
    institution: str
    date_made_old_institution: Optional[datetime] = None
