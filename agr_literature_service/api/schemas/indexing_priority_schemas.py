from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator, confloat
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

    predicted_indexing_priority: Optional[str] = None
    curator_indexing_priority: Optional[str] = None
    mod_abbreviation: str

    @field_validator('predicted_indexing_priority')
    def _check_predicted_atp_prefix(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith('ATP:'):
            raise ValueError("predicted_indexing_priority must start with 'ATP:'")
        return v

    @field_validator('curator_indexing_priority')
    def _check_curator_atp_prefix(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith('ATP:'):
            raise ValueError("curator_indexing_priority must start with 'ATP:'")
        return v

    @model_validator(mode='after')
    def _at_least_one_priority(self):
        if self.predicted_indexing_priority is None and self.curator_indexing_priority is None:
            raise ValueError(
                "At least one of predicted_indexing_priority or "
                "curator_indexing_priority must be provided"
            )
        return self


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
    predicted_indexing_priority: Optional[str] = None
    curator_indexing_priority: Optional[str] = None
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
    predicted_indexing_priority: Optional[str] = None
    curator_indexing_priority: Optional[str] = None

    @field_validator('predicted_indexing_priority')
    def _check_predicted_atp_prefix(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith('ATP:'):
            raise ValueError("predicted_indexing_priority must start with 'ATP:'")
        return v

    @field_validator('curator_indexing_priority')
    def _check_curator_atp_prefix(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith('ATP:'):
            raise ValueError("curator_indexing_priority must start with 'ATP:'")
        return v
