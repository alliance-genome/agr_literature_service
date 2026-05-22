from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _normalize_email(v: str) -> str:
    """Trim and validate. Casing is preserved for storage; lookups
    normalize via lower() at query time."""
    v = (v or "").strip()
    if "@" not in v or v.startswith("@") or v.endswith("@"):
        raise ValueError("invalid email format")
    return v


class _EmailAddressMixin(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_address: str

    @field_validator("email_address")
    @classmethod
    def _validate_email(cls, v: str) -> str:
        return _normalize_email(v)


class PersonEmailSchemaCreate(_EmailAddressMixin):
    """Payload for creating a person_email row."""
    date_made_old_email: Optional[datetime] = None


class PersonEmailSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_address: Optional[str] = None
    date_made_old_email: Optional[datetime] = None

    @field_validator("email_address")
    @classmethod
    def _validate_email(cls, v: Optional[str]) -> Optional[str]:
        return _normalize_email(v) if v is not None else None


class PersonEmailSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_id: int
    person_id: int
    email_address: str
    date_made_old_email: Optional[datetime] = None


class PersonEmailSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_id: int
    email_address: str
    date_made_old_email: Optional[datetime] = None
