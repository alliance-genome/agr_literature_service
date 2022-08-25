from typing import List, Optional
from pydantic import BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class TopicEntityTagPropSchemaCreate(AuditedObjectModelSchema):
    topic_entity_tag_id: Optional[int] = None  # created on the fly so not needed here
    qualifier: str


class TopicEntityTagPropSchemaShow(AuditedObjectModelSchema):
    topic_entity_tag_prop_id: int
    topic_entity_tag_id: int
    qualifier: str


class TopicEntityTagPropSchemaUpdate(BaseModel):
    qualifier: str
    topic_entity_tag_prop_id: Optional[int] = 0

    class Config():
        orm_mode = True
        extra = "forbid"
        schema_extra = {
            "example": {
                "topic_entity_tag_prop_id": 1,
                "qualifier": "Q1"
            }
        }


class TopicEntityTagSchemaCreate(AuditedObjectModelSchema):
    reference_curie: str
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None


class TopicEntityTagSchemaRelated(AuditedObjectModelSchema):
    reference_curie: Optional[str]
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None


class TopicEntityTagSchemaUpdate(AuditedObjectModelSchema):
    reference_curie: Optional[str] = None
    topic: Optional[str] = ""
    entity_type: Optional[str] = ""
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str]
    taxon: Optional[str]
    note: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaUpdate]]


class TopicEntityTagSchemaShow(AuditedObjectModelSchema):
    topic_entity_tag_id: int
    reference_curie: str
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaShow]] = None
