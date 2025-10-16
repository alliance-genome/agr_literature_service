from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, field_validator

from .base_schemas import AuditedObjectModelSchema


class PersonCrossReferenceSchemaRelated(AuditedObjectModelSchema):
    """Related cross-reference details (embedded under Person)."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_cross_reference_id: int
    curie: str
    curie_prefix: str
    pages: Optional[List[str]] = None
    is_obsolete: bool = False

    @field_validator("curie")
    @classmethod
    def validate_curie(cls, v: str) -> str:
        if v.count(":") != 1:
            raise ValueError("curie must contain exactly one colon (e.g., 'PREFIX:ID').")
        return v


class PersonCrossReferenceSchemaCreate(BaseModel):
    """Create payload for a new person cross-reference."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    curie: str
    pages: Optional[List[str]] = None
    is_obsolete: bool = False

    @field_validator("curie")
    @classmethod
    def validate_curie(cls, v: str) -> str:
        if v.count(":") != 1:
            raise ValueError("curie must contain exactly one colon (e.g., 'PREFIX:ID').")
        return v


class PersonCrossReferenceSchemaUpdate(BaseModel):
    """Partial update payload for a person cross-reference."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    curie: Optional[str] = None
    pages: Optional[List[str]] = None
    is_obsolete: Optional[bool] = None

    @field_validator("curie")
    @classmethod
    def validate_curie(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if v.count(":") != 1:
            raise ValueError("curie must contain exactly one colon (e.g., 'PREFIX:ID').")
        return v


class PersonCrossReferenceSchemaShow(AuditedObjectModelSchema):
    """Full cross-reference record for detail endpoints."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_cross_reference_id: int
    curie: str
    curie_prefix: str
    pages: Optional[List[str]] = None
    is_obsolete: bool = False

    @field_validator("curie")
    @classmethod
    def validate_curie(cls, v: str) -> str:
        if v.count(":") != 1:
            raise ValueError("curie must contain exactly one colon (e.g., 'PREFIX:ID').")
        return v
