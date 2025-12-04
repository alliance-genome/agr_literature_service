from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator, confloat
from datetime import datetime
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ConfidenceMixin(BaseModel):
    """Mixin that adds a rounded confidence_score field (0.0–1.0)."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        from_attributes=True,
    )

    confidence_score: Optional[confloat(ge=0.0, le=1.0)] = None  # type: ignore

    @field_validator('confidence_score')
    def _round_confidence_score(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        return round(v, 2)


class IndexingPrioritySchemaCreate(ConfidenceMixin):
    """Schema for creating an indexing priority tag association."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    indexing_priority: str
    mod_abbreviation: str

    @field_validator('indexing_priority')
    def _check_atp_prefix(cls, v: str) -> str:
        if not v.startswith('ATP:'):
            raise ValueError("indexing_priority must start with 'ATP:'")
        return v


class IndexingPrioritySchemaPost(IndexingPrioritySchemaCreate):
    """Schema for posting an indexing priority tag with reference context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: str


class IndexingPrioritySchemaRelated(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for related indexing priority entries with audit fields."""
    model_config = ConfigDict(
        extra='ignore',
        from_attributes=True
    )

    indexing_priority_id: Optional[int] = None
    indexing_priority: str
    mod_abbreviation: Optional[str] = None
    updated_by_email: Optional[str] = None
    updated_by_name: Optional[str] = None
    date_updated: Optional[datetime] = None  # include for output


class IndexingPrioritySchemaShow(IndexingPrioritySchemaRelated):
    """Schema for showing an indexing priority tag with its reference context."""
    reference_curie: str


class IndexingPrioritySchemaUpdate(ConfidenceMixin):
    """Schema for updating an indexing priority tag association."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    indexing_priority: Optional[str] = None

    @field_validator('indexing_priority')
    def _check_atp_prefix_optional(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith('ATP:'):
            raise ValueError("indexing_priority must start with 'ATP:'")
        return v
