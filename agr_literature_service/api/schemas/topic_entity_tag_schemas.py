from typing import Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class TopicEntityTagSourceSchemaCreate(AuditedObjectModelSchema):
    source_type: str
    source_method: str
    validation_type: Optional[str] = None
    evidence: str
    description: str
    mod_abbreviation: str


class TopicEntityTagSourceSchemaShow(TopicEntityTagSourceSchemaCreate):
    topic_entity_tag_source_id: int


class TopicEntityTagSourceSchemaUpdate(BaseModel):
    source_type: Optional[str]
    source_method: Optional[str]
    validation_type: Optional[str]
    evidence: Optional[str]
    description: Optional[str]
    mod_abbreviation: Optional[str]
    date_created: Optional[str]
    date_updated: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]

    class Config:
        orm_mode = True
        extra = "forbid"


class TopicEntityTagSchemaCreate(AuditedObjectModelSchema):
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


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    reference_curie: str


class TopicEntityTagSchemaUpdate(AuditedObjectModelSchema):
    topic: Optional[str] = None
    entity_type: Optional[str] = None
    entity: Optional[str] = None
    entity_source: Optional[str] = None
    entity_published_as: Optional[str] = None
    species: Optional[str] = None
    display_tag: Optional[str] = None
    negated: Optional[bool] = False
    confidence_level: Optional[str] = None
    note: Optional[str] = None
