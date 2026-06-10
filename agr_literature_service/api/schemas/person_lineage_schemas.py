from __future__ import annotations
from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .base_schemas import AuditedObjectModelSchema
from .person_lineage_relationship_enum import PersonPersonRole


def _validate_non_empty(v: str, field: str) -> str:
    if not v or not v.strip():
        raise ValueError(f"{field} cannot be empty or whitespace")
    return v.strip()


class PersonLineageSchemaCreate(BaseModel):
    """Create payload for a person-to-person lineage relationship.

    Names and the relationship are required; the person object links and dates
    are optional (the persons may not exist as objects yet).
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_one_name: str
    person_two_name: str
    relationship: PersonPersonRole
    who_sent_this: str
    person_one: Optional[int] = None
    person_two: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @field_validator("person_one_name")
    @classmethod
    def _validate_person_one_name(cls, v: str) -> str:
        return _validate_non_empty(v, "person_one_name")

    @field_validator("person_two_name")
    @classmethod
    def _validate_person_two_name(cls, v: str) -> str:
        return _validate_non_empty(v, "person_two_name")

    @field_validator("who_sent_this")
    @classmethod
    def _validate_who_sent_this(cls, v: str) -> str:
        return _validate_non_empty(v, "who_sent_this")


class PersonLineageSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_one_name: Optional[str] = None
    person_two_name: Optional[str] = None
    relationship: Optional[PersonPersonRole] = None
    who_sent_this: Optional[str] = None
    person_one: Optional[int] = None
    person_two: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_null_required(cls, data: Any) -> Any:
        # person_one_name, person_two_name, relationship and who_sent_this are
        # NOT NULL — reject explicit null. Omitting the field leaves it unchanged.
        if isinstance(data, dict):
            for field in ("person_one_name", "person_two_name", "relationship", "who_sent_this"):
                if field in data and data[field] is None:
                    raise ValueError(
                        f"{field} cannot be null; omit the field to leave it unchanged"
                    )
        return data


class PersonLineageSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_lineage_id: int
    person_one_name: str
    person_two_name: str
    relationship: str
    who_sent_this: str
    person_one: Optional[int] = None
    person_two: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
