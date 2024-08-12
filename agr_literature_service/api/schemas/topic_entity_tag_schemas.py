from typing import Optional, Union
from pydantic import BaseModel, Field, constr
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class TopicEntityTagSourceSchemaCreate(AuditedObjectModelSchema):
    source_evidence_assertion: str = Field(..., min_length=1)
    source_method: str = Field(..., min_length=1)
    validation_type: Optional[constr(min_length=1)] = None  # type: ignore
    description: str
    data_provider: str
    secondary_data_provider_abbreviation: str


class TopicEntityTagSourceSchemaShow(TopicEntityTagSourceSchemaCreate):
    topic_entity_tag_source_id: int


class TopicEntityTagSourceSchemaUpdate(BaseModel):
    source_evidence_assertion: Optional[constr(min_length=1)]  # type: ignore
    source_method: Optional[constr(min_length=1)]  # type: ignore
    validation_type: Optional[constr(min_length=1)]  # type: ignore
    description: Optional[constr(min_length=1)]  # type: ignore
    data_provider_abbreviation: Optional[constr(min_length=1)]  # type: ignore
    secondary_data_provider_abbreviation: Optional[constr(min_length=1)]  # type: ignore
    date_created: Optional[constr(min_length=1)]  # type: ignore
    date_updated: Optional[constr(min_length=1)]  # type: ignore
    created_by: Optional[constr(min_length=1)]  # type: ignore
    updated_by: Optional[constr(min_length=1)]  # type: ignore

    class Config:
        orm_mode = True
        extra = "forbid"


class TopicEntityTagSchemaCreate(AuditedObjectModelSchema):
    topic: str = Field(..., min_length=1)
    entity_type: Optional[constr(min_length=1)] = None  # type: ignore
    entity: Optional[constr(min_length=1)] = None  # type: ignore
    entity_id_validation: Optional[constr(min_length=1)] = None  # type: ignore
    entity_published_as: Optional[constr(min_length=1)] = None  # type: ignore
    species: Optional[constr(min_length=1)] = None  # type: ignore
    display_tag: Optional[constr(min_length=1)] = None  # type: ignore
    topic_entity_tag_source_id: int
    negated: Optional[Union[bool, None]] = False
    novel_topic_data: Optional[bool] = False
    confidence_level: Optional[constr(min_length=1)] = None  # type: ignore
    note: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_author: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None  # type: ignore


class TopicEntityTagSchemaPost(TopicEntityTagSchemaCreate):
    reference_curie: str
    force_insertion: Optional[bool] = False


class TopicEntityTagSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_id: int
    topic: str
    entity_type: Optional[str] = None
    entity: Optional[str] = None
    entity_name: Optional[str] = None
    entity_id_validation: Optional[str] = None
    entity_published_as: Optional[str] = None
    species: Optional[str] = None
    display_tag: Optional[str] = None
    topic_entity_tag_source_id: int
    negated: Optional[Union[bool, None]] = False
    novel_topic_data: Optional[bool] = False
    confidence_level: Optional[str] = None
    note: Optional[str] = None
    validation_by_author: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None  # type: ignore


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    reference_curie: str


class TopicEntityTagSchemaUpdate(AuditedObjectModelSchema):
    topic: Optional[constr(min_length=1)] = None  # type: ignore
    entity_type: Optional[constr(min_length=1)] = None  # type: ignore
    entity: Optional[constr(min_length=1)] = None  # type: ignore
    entity_id_validation: Optional[constr(min_length=1)] = None  # type: ignore
    entity_published_as: Optional[constr(min_length=1)] = None  # type: ignore
    species: Optional[constr(min_length=1)] = None  # type: ignore
    display_tag: Optional[constr(min_length=1)] = None  # type: ignore
    negated: Optional[Union[bool, None]] = False
    novel_topic_data: Optional[bool] = False
    confidence_level: Optional[constr(min_length=1)] = None  # type: ignore
    note: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_author: Optional[constr(min_length=1)] = None  # type: ignore
    validation_by_professional_biocurator: Optional[constr(min_length=1)] = None  # type: ignore
