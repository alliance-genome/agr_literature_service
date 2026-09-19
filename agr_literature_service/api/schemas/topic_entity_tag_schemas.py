
from typing import Optional, List

from pydantic import BaseModel, ConfigDict, Field, field_validator, constr, confloat

from agr_literature_service.api.schemas import AuditedObjectModelSchema
from agr_literature_service.api.schemas.tag_source_schemas import TagSourceSchemaShow


class ConfidenceMixin(BaseModel):
    """Mixin that adds a rounded confidence_score field (0.0–1.0)."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        from_attributes=True,
    )

    confidence_score: Optional[confloat(ge=0.0, le=1.0)] = None  # type: ignore

    @field_validator('confidence_score')
    def _round_confidence_score(cls, v):
        if v is None:
            return None
        return round(v, 2)


class TopicEntityTagSchemaCreate(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for creating a topic entity tag."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    @field_validator('negated', mode='before')
    def convert_none_to_false(cls, v):
        return v if v is not None else False

    topic: str = Field(..., min_length=1)
    entity_type: Optional[constr(min_length=1)] = None  # type: ignore
    entity: Optional[constr(min_length=1)] = None  # type: ignore
    entity_id_validation: Optional[constr(min_length=1)] = None  # type: ignore
    species: Optional[constr(min_length=1)] = None  # type: ignore
    display_tag: Optional[constr(min_length=1)] = None  # type: ignore
    tag_source_id: int
    negated: Optional[bool] = False
    data_novelty: Optional[constr(min_length=1)] = None  # type: ignore
    data_context: Optional[constr(min_length=1)] = None  # type: ignore
    confidence_level: Optional[constr(min_length=1)] = None  # type: ignore
    note: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_author: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None  # type: ignore
    ml_model_id: Optional[int] = None


class TopicEntityTagSchemaPost(TopicEntityTagSchemaCreate):
    """Schema for posting a topic entity tag with reference context."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    reference_curie: str
    force_insertion: bool = False
    index_wft: Optional[str] = None


class TopicEntityTagSchemaRelated(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for related topic entity tags with audit fields."""
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    @field_validator('negated', mode='before')
    def convert_none_to_false(cls, v):
        return v if v is not None else False

    topic_entity_tag_id: int
    topic: str
    topic_name: Optional[str] = None
    entity_type: Optional[str] = None
    entity_type_name: Optional[str] = None
    entity: Optional[str] = None
    entity_name: Optional[str] = None
    entity_id_validation: Optional[str] = None
    species: Optional[str] = None
    species_name: Optional[str] = None
    display_tag: Optional[str] = None
    display_tag_name: Optional[str] = None
    tag_source_id: int
    tag_source: Optional[TagSourceSchemaShow] = None
    negated: Optional[bool] = False
    data_novelty: Optional[str] = None
    data_context: Optional[str] = None
    data_context_name: Optional[str] = None
    confidence_level: Optional[str] = None
    note: Optional[str] = None
    validation_by_author: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None  # type: ignore
    validating_users: List[str] = Field(default_factory=list)
    validating_tags: List[int] = Field(default_factory=list)
    ml_model_id: Optional[int] = None
    ml_model_version: Optional[int] = None


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    """Schema for showing a topic entity tag with reference context."""
    reference_curie: str


class TopicEntityTagSchemaUpdate(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for updating a topic entity tag."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    @field_validator('negated', mode='before')
    def convert_none_to_false(cls, v):
        return v if v is not None else False

    topic: Optional[constr(min_length=1)] = None  # type: ignore
    entity_type: Optional[constr(min_length=1)] = None  # type: ignore
    entity: Optional[constr(min_length=1)] = None  # type: ignore
    entity_id_validation: Optional[constr(min_length=1)] = None  # type: ignore
    species: Optional[constr(min_length=1)] = None  # type: ignore
    display_tag: Optional[constr(min_length=1)] = None  # type: ignore
    negated: Optional[bool] = False
    data_novelty: Optional[constr(min_length=1)] = None  # type: ignore
    data_context: Optional[constr(min_length=1)] = None  # type: ignore
    confidence_level: Optional[constr(min_length=1)] = None  # type: ignore
    note: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_author: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None  # type: ignore
