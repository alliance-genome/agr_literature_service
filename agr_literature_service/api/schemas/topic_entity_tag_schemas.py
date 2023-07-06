from typing import List, Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class TopicEntityTagSourceSchemaCreate(BaseModel):
    source: str
    negated: Optional[bool] = False
    confidence_level: Optional[str] = None
    mod_abbreviation: str
    validation_value_author: Optional[bool] = None
    validation_value_curator: Optional[bool] = None
    validation_value_curation_tools: Optional[bool] = None
    note: Optional[str] = None


class TopicEntityTagSourceSchemaPost(TopicEntityTagSourceSchemaCreate):
    topic_entity_tag_id: int  # required as here topic_entity_tag_prop created separate from topic_entity_tag


class TopicEntityTagSourceSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_source_id: int
    source: str
    negated: Optional[bool] = False
    confidence_level: Optional[str] = None
    mod_abbreviation: str
    validation_value_author: Optional[bool] = None
    validation_value_curator: Optional[bool] = None
    validation_value_curation_tools: Optional[bool] = None
    note: Optional[str] = None


class TopicEntityTagSourceSchemaShow(AuditedObjectModelSchema):
    topic_entity_tag_source_id: int
    source: str
    negated: Optional[bool] = False
    confidence_level: Optional[str] = None
    mod_abbreviation: str
    validation_value_author: Optional[bool] = None
    validation_value_curator: Optional[bool] = None
    validation_value_curation_tools: Optional[bool] = None
    note: Optional[str] = None


class TopicEntityTagSourceSchemaUpdate(BaseModel):
    source: Optional[str]
    negated: Optional[bool]
    confidence_level: Optional[str]
    mod_abbreviation: Optional[str]
    validation_value_author: Optional[bool]
    validation_value_curator: Optional[bool]
    validation_value_curation_tools: Optional[bool]
    note: Optional[str]

    class Config:
        orm_mode = True
        extra = "forbid"


class TopicEntityTagSchemaCreate(BaseModel):
    topic: str
    entity_type: Optional[str] = None
    entity: Optional[str] = None
    entity_source: Optional[str] = None
    entity_published_as: Optional[str] = None
    species: Optional[str] = None
    display_tag: Optional[str] = None
    sources: List[TopicEntityTagSourceSchemaCreate]


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
    sources: List[TopicEntityTagSourceSchemaRelated]


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    reference_curie: str


class TopicEntityTagSchemaUpdate(BaseModel):
    topic: Optional[str] = None
    entity_type: Optional[str] = None
    entity: Optional[str] = None
    entity_source: Optional[str] = None
    entity_published_as: Optional[str] = None
    display_tag: Optional[str] = None
    species: Optional[str] = None
