from typing import Optional, Union, List

from pydantic import BaseModel, ConfigDict, Field, field_validator, constr, confloat

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ConfidenceMixin(BaseModel):
    """Mixin that adds a rounded confidence_score field (0.0–1.0)."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        from_attributes=True,
    )

    confidence_score: Optional[confloat(ge=0.0, le=1.0)] = None

    @field_validator('confidence_score')
    def _round_confidence_score(cls, v):
        if v is None:
            return None
        return round(v, 2)


class TopicEntityTagSourceSchemaCreate(AuditedObjectModelSchema):
    """Schema for creating a topic entity tag source."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    source_evidence_assertion: str = Field(..., min_length=1)
    source_method: str = Field(..., min_length=1)
    validation_type: Optional[constr(min_length=1)] = None
    description: str
    data_provider: str
    secondary_data_provider_abbreviation: str


class TopicEntityTagSourceSchemaShow(TopicEntityTagSourceSchemaCreate):
    """Schema for showing a topic entity tag source."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    topic_entity_tag_source_id: int
    source_evidence_assertion_name: Optional[str] = None


class TopicEntityTagSourceSchemaUpdate(BaseModel):
    """Schema for updating a topic entity tag source."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    source_evidence_assertion: Optional[constr(min_length=1)] = None
    source_method: Optional[constr(min_length=1)] = None
    validation_type: Optional[constr(min_length=1)] = None
    description: Optional[constr(min_length=1)] = None
    data_provider: Optional[constr(min_length=1)] = None
    secondary_data_provider_abbreviation: Optional[constr(min_length=1)] = None
    date_created: Optional[constr(min_length=1)] = None
    date_updated: Optional[constr(min_length=1)] = None
    created_by: Optional[constr(min_length=1)] = None
    updated_by: Optional[constr(min_length=1)] = None


class TopicEntityTagSchemaCreate(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for creating a topic entity tag."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    topic: str = Field(..., min_length=1)
    entity_type: Optional[constr(min_length=1)] = None
    entity: Optional[constr(min_length=1)] = None
    entity_id_validation: Optional[constr(min_length=1)] = None
    entity_published_as: Optional[constr(min_length=1)] = None
    species: Optional[constr(min_length=1)] = None
    display_tag: Optional[constr(min_length=1)] = None
    topic_entity_tag_source_id: int
    negated: bool = False
    novel_topic_data: bool = False
    data_novelty: Optional[constr(min_length=1)] = None
    confidence_level: Optional[constr(min_length=1)] = None
    note: Optional[constr(min_length=1)] = None
    validation_by_author: Optional[constr(min_length=1)] = None
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None


class TopicEntityTagSchemaPost(TopicEntityTagSchemaCreate):
    """Schema for posting a topic entity tag with reference context."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    reference_curie: str
    force_insertion: bool = False
    index_wft: Optional[str] = None


class TopicEntityTagSchemaRelated(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for related topic entity tags with audit fields."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    topic_entity_tag_id: int
    topic: str
    topic_name: Optional[str] = None
    entity_type: Optional[str] = None
    entity_type_name: Optional[str] = None
    entity: Optional[str] = None
    entity_name: Optional[str] = None
    entity_id_validation: Optional[str] = None
    entity_published_as: Optional[str] = None
    species: Optional[str] = None
    species_name: Optional[str] = None
    display_tag: Optional[str] = None
    display_tag_name: Optional[str] = None
    topic_entity_tag_source_id: int
    topic_entity_tag_source: Optional[TopicEntityTagSourceSchemaShow] = None
    negated: bool = False
    novel_topic_data: bool = False
    data_novelty: Optional[str] = None
    confidence_level: Optional[str] = None
    note: Optional[str] = None
    validation_by_author: Optional[constr(min_length=1)] = None
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None
    validating_users: List[str] = Field(default_factory=list)
    validating_tags: List[int] = Field(default_factory=list)


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    """Schema for showing a topic entity tag with reference context."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    reference_curie: str


class TopicEntityTagSchemaUpdate(ConfidenceMixin, AuditedObjectModelSchema):
    """Schema for updating a topic entity tag."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    topic: Optional[constr(min_length=1)] = None
    entity_type: Optional[constr(min_length=1)] = None
    entity: Optional[constr(min_length=1)] = None
    entity_id_validation: Optional[constr(min_length=1)] = None
    entity_published_as: Optional[constr(min_length=1)] = None
    species: Optional[constr(min_length=1)] = None
    display_tag: Optional[constr(min_length=1)] = None
    negated: bool = False
    novel_topic_data: bool = False
    data_novelty: Optional[constr(min_length=1)] = None
    confidence_level: Optional[constr(min_length=1)] = None
    note: Optional[constr(min_length=1)] = None
    validation_by_author: Optional[constr(min_length=1)] = None
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None
