from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _normalize_email(v: str) -> str:
    v = (v or "").strip().lower()
    if "@" not in v or v.startswith("@") or v.endswith("@"):
        raise ValueError("invalid email format")
    return v


class _EmailAddressMixin(BaseModel):
    """Shared normalization for email address fields."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_address: str

    @field_validator("email_address")
    @classmethod
    def _validate_email(cls, v: str) -> str:
        return _normalize_email(v)


class EmailSchemaCreate(_EmailAddressMixin):
    """
    Payload for creating an Email row.
    `person_id` is supplied by the route (path param) or owning context.
    """
    date_invalidated: Optional[datetime] = None


class EmailSchemaUpdate(BaseModel):
    """
    Partial update payload for Email rows.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_address: Optional[str] = None
    date_invalidated: Optional[datetime] = None

    @field_validator("email_address")
    @classmethod
    def _validate_email(cls, v: Optional[str]) -> Optional[str]:
        return _normalize_email(v) if v is not None else None


class EmailSchemaShow(AuditedObjectModelSchema):
    """
    Full Email representation with audit fields (for detail endpoints).
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_id: int
    person_id: int
    email_address: str
    date_invalidated: Optional[datetime] = None


class EmailSchemaRelated(AuditedObjectModelSchema):
    """
    Compact Email representation (embedded under PersonSchemaShow).
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_id: int
    email_address: str
    date_invalidated: Optional[datetime] = None
