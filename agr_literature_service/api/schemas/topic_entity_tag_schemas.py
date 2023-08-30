from typing import Optional, Union
from pydantic import BaseModel, Field, constr
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class TopicEntityTagSourceSchemaCreate(AuditedObjectModelSchema):
    source_type: str = Field(..., min_length=1)
    source_method: str = Field(..., min_length=1)
    validation_type: Optional[constr(min_length=1)] = None
    evidence: str = Field(..., min_length=1)
    description: str
    mod_abbreviation: str


class TopicEntityTagSourceSchemaShow(TopicEntityTagSourceSchemaCreate):
    topic_entity_tag_source_id: int


class TopicEntityTagSourceSchemaUpdate(BaseModel):
    source_type: Optional[constr(min_length=1)]
    source_method: Optional[constr(min_length=1)]
    validation_type: Optional[constr(min_length=1)]
    evidence: Optional[constr(min_length=1)]
    description: Optional[constr(min_length=1)]
    mod_abbreviation: Optional[constr(min_length=1)]
    date_created: Optional[constr(min_length=1)]
    date_updated: Optional[constr(min_length=1)]
    created_by: Optional[constr(min_length=1)]
    updated_by: Optional[constr(min_length=1)]

    class Config:
        orm_mode = True
        extra = "forbid"


class TopicEntityTagSchemaCreate(AuditedObjectModelSchema):
    topic: str = Field(..., min_length=1)
    entity_type: Optional[constr(min_length=1)] = None
    entity: Optional[constr(min_length=1)] = None
    entity_source: Optional[constr(min_length=1)] = None
    entity_published_as: Optional[constr(min_length=1)] = None
    species: Optional[constr(min_length=1)] = None
    display_tag: Optional[constr(min_length=1)] = None
    topic_entity_tag_source_id: int
    negated: Optional[bool] = False
    confidence_level: Optional[constr(min_length=1)] = None
    note: Optional[constr(min_length=1)] = None


class TopicEntityTagSchemaPost(TopicEntityTagSchemaCreate):
    reference_curie: str


class TopicEntityTagSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_id: int
    topic: str
    entity_type: Optional[str] = None
    entity: Optional[str] = None
    entity_source: Optional[str] = None
    entity_published_as: Optional[str] = None
    species: Optional[str] = None
    display_tag: Optional[str] = None
    topic_entity_tag_source_id: int
    negated: Optional[bool] = False
    confidence_level: Optional[str] = None
    note: Optional[str] = None
    validation_value_author: Union[bool, None]
    validation_value_curator: Union[bool, None]
    validation_value_curation_tools: Union[bool, None]


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    reference_curie: str


class TopicEntityTagSchemaUpdate(AuditedObjectModelSchema):
    topic: Optional[constr(min_length=1)] = None
    entity_type: Optional[constr(min_length=1)] = None
    entity: Optional[constr(min_length=1)] = None
    entity_source: Optional[constr(min_length=1)] = None
    entity_published_as: Optional[constr(min_length=1)] = None
    species: Optional[constr(min_length=1)] = None
    display_tag: Optional[constr(min_length=1)] = None
    negated: Optional[bool] = False
    confidence_level: Optional[constr(min_length=1)] = None
    note: Optional[constr(min_length=1)] = None
