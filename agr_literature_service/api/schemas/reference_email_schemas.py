from __future__ import annotations
from typing import List

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _normalize_email(v: str) -> str:
    """Trim and validate. Casing is preserved for storage; the
    case-insensitive unique index handles dedup at the DB level."""
    v = (v or "").strip()
    if "@" not in v or v.startswith("@") or v.endswith("@"):
        raise ValueError("invalid email format")
    return v


class ReferenceEmailSchemaCreate(BaseModel):
    """
    Payload for attaching emails to a reference.

    POST /reference/{reference_id}/emails

    Example body:
        {
          "emails": ["a@b.org", "c@d.org"]
        }
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    emails: List[str]

    @field_validator("emails")
    @classmethod
    def _validate_emails(cls, v: List[str]) -> List[str]:
        return [_normalize_email(x) for x in v]


class ReferenceEmailSchemaUpdate(BaseModel):
    """
    Update payload for reference-email link rows.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_address: str

    @field_validator("email_address")
    @classmethod
    def _validate_email(cls, v: str) -> str:
        return _normalize_email(v)


class ReferenceEmailSchemaShow(AuditedObjectModelSchema):
    """Full reference-email row with audit fields."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    reference_email_id: int
    reference_id: int
    email_address: str


class ReferenceEmailSchemaRelated(BaseModel):
    """Compact reference-email row for embedding under ReferenceSchemaShow."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    reference_email_id: int
    email_address: str
