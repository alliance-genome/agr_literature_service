"""
tag_source_schemas.py
=====================

Schemas for the shared tag_source table (SCRUM-6518). Split out of
topic_entity_tag_schemas.py because the source is no longer TET-specific.
"""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, constr

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class TagSourceSchemaCreate(AuditedObjectModelSchema):
    """Schema for creating a tag source."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    source_evidence_assertion: str = Field(..., min_length=1)
    source_method: str = Field(..., min_length=1)
    validation_type: Optional[constr(min_length=1)] = None  # type: ignore
    description: str
    data_provider: str
    secondary_data_provider_abbreviation: str


class TagSourceSchemaShow(TagSourceSchemaCreate):
    """Schema for showing a tag source."""
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    tag_source_id: int
    source_evidence_assertion_name: Optional[str] = None


class TagSourceSchemaUpdate(BaseModel):
    """Schema for updating a tag source."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    source_evidence_assertion: Optional[constr(min_length=1)] = None  # type: ignore
    source_method: Optional[constr(min_length=1)] = None  # type: ignore
    validation_type: Optional[constr(min_length=1)] = None  # type: ignore
    description: Optional[constr(min_length=1)] = None  # type: ignore
    data_provider: Optional[constr(min_length=1)] = None  # type: ignore
    secondary_data_provider_abbreviation: Optional[constr(min_length=1)] = None  # type: ignore
    date_created: Optional[constr(min_length=1)] = None  # type: ignore
    date_updated: Optional[constr(min_length=1)] = None  # type: ignore
    created_by: Optional[constr(min_length=1)] = None  # type: ignore
    updated_by: Optional[constr(min_length=1)] = None  # type: ignore
