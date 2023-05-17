from typing import List, Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class TopicEntityTagQualifierSchemaCreate(BaseModel):
    qualifier: str
    qualifier_type: str
    mod_abbreviation: str


class TopicEntityTagQualifierSchemaPost(TopicEntityTagQualifierSchemaCreate):
    topic_entity_tag_id: int  # required as here topic_entity_tag_prop created separate from topic_entity_tag


class TopicEntityTagQualifierSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_qualifier_id: int
    qualifier: str
    qualifier_type: str
    mod_abbreviation: str


class TopicEntityTagQualifierSchemaShow(AuditedObjectModelSchema):
    topic_entity_tag_prop_id: int
    qualifier: str
    qualifier_type: str
    mod_abbreviation: str


class TopicEntityTagQualifierSchemaUpdate(BaseModel):
    qualifier: Optional[str] = None
    qualifier_type: Optional[str] = None
    mod_abbreviation: Optional[str] = None

    class Config:
        orm_mode = True
        extra = "forbid"


class TopicEntityTagSourceSchemaCreate(BaseModel):
    source: str
    confidence_level: Optional[str] = None
    mod_abbreviation: str
    validated: Optional[bool] = False
    validation_type: Optional[str] = None
    note: Optional[str] = None


class TopicEntityTagSourceSchemaPost(TopicEntityTagSourceSchemaCreate):
    topic_entity_tag_id: int  # required as here topic_entity_tag_prop created separate from topic_entity_tag


class TopicEntityTagSourceSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_source_id: int
    source: str
    confidence_level: Optional[str] = None
    mod_abbreviation: str
    validated: Optional[bool] = False
    validation_type: Optional[str] = None
    note: Optional[str] = None


class TopicEntityTagSourceSchemaShow(AuditedObjectModelSchema):
    topic_entity_tag_source_id: int
    source: str
    confidence_level: Optional[str] = None
    mod_abbreviation: str
    validated: Optional[bool] = False
    validation_type: Optional[str] = None
    note: Optional[str] = None


class TopicEntityTagSourceSchemaUpdate(BaseModel):
    source: Optional[str]
    confidence_level: Optional[str]
    mod_abbreviation: Optional[str]
    validated: Optional[bool]
    validation_type: Optional[str]
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
    species: str
    qualifiers: Optional[List[TopicEntityTagQualifierSchemaCreate]] = None
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
    species: str
    qualifiers: Optional[List[TopicEntityTagQualifierSchemaRelated]] = None
    sources: List[TopicEntityTagSourceSchemaRelated]


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    reference_curie: str


class TopicEntityTagSchemaUpdate(BaseModel):
    topic: Optional[str] = None
    entity_type: Optional[str] = None
    entity: Optional[str] = None
    entity_source: Optional[str] = None
    entity_published_as: Optional[str] = None
    species: Optional[str] = None
