from __future__ import annotations
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


def _normalize_email(v: str) -> str:
    """
    Local email normalizer, same semantics as email_schemas._normalize_email,
    but defined here to avoid importing CRUD and causing circular imports.
    """
    v = (v or "").strip().lower()
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

    Currently a placeholder – you might not need this if you only support
    create/delete of links.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    email_id: Optional[int] = None


class ReferenceEmailSchemaShow(AuditedObjectModelSchema):
    """
    Full representation of a reference-email link (detail view),
    including audit fields from AuditedObjectModelSchema.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    reference_email_id: int
    reference_id: int
    email_id: int
    email_address: str


class ReferenceEmailSchemaRelated(BaseModel):
    """
    Compact nested representation when embedded in ReferenceSchemaShow.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    reference_email_id: int
    email_id: int
    email_address: str
