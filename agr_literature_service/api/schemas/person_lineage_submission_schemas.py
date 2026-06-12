from __future__ import annotations
from datetime import datetime
from typing import Any, Literal, Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .base_schemas import AuditedObjectModelSchema
from .person_lineage_relationship_enum import PersonPersonRole
from .validation_utils import validate_non_empty

# Controlled vocabulary enforced by the API (curator-managed); no DB CheckConstraint.
SubmissionStatus = Literal["pending", "partially_resolved", "validated", "rejected", "duplicate"]


class PersonLineageSubmissionSchemaCreate(BaseModel):
    """Create payload for a submitted person-to-person relationship claim.

    Names, relationship and who_sent_this are required; the person id links and
    dates are optional (the persons may not be known/resolved yet). status is
    server-managed and defaults to 'pending' on create.
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_one_name: str
    person_two_name: str
    relationship: PersonPersonRole
    who_sent_this: str
    person_one_id: Optional[int] = None
    person_two_id: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @field_validator("person_one_name")
    @classmethod
    def _validate_person_one_name(cls, v: str) -> str:
        return validate_non_empty(v, "person_one_name")

    @field_validator("person_two_name")
    @classmethod
    def _validate_person_two_name(cls, v: str) -> str:
        return validate_non_empty(v, "person_two_name")

    @field_validator("who_sent_this")
    @classmethod
    def _validate_who_sent_this(cls, v: str) -> str:
        return validate_non_empty(v, "who_sent_this")


class PersonLineageSubmissionSchemaUpdate(BaseModel):
    """Curator edits: resolve person ids, set status, adjust the claim/dates."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_one_name: Optional[str] = None
    person_two_name: Optional[str] = None
    relationship: Optional[PersonPersonRole] = None
    who_sent_this: Optional[str] = None
    person_one_id: Optional[int] = None
    person_two_id: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: Optional[SubmissionStatus] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_null_required(cls, data: Any) -> Any:
        # These columns are NOT NULL — reject explicit null. Omitting the field
        # leaves it unchanged.
        if isinstance(data, dict):
            for field in ("person_one_name", "person_two_name", "relationship",
                          "who_sent_this", "status"):
                if field in data and data[field] is None:
                    raise ValueError(
                        f"{field} cannot be null; omit the field to leave it unchanged"
                    )
        return data


class PersonLineageSubmissionSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_lineage_submission_id: int
    person_one_name: str
    person_two_name: str
    relationship: str
    who_sent_this: str
    person_one_id: Optional[int] = None
    person_two_id: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: str = "pending"
    person_lineage_id: Optional[int] = None


class PersonLineageSubmissionSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_lineage_submission_id: int
    person_one_name: str
    person_two_name: str
    relationship: str
    who_sent_this: str
    person_one_id: Optional[int] = None
    person_two_id: Optional[int] = None
    status: str = "pending"
    person_lineage_id: Optional[int] = None
