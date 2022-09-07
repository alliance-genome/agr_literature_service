from typing import List, Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


# use by topic_entity_tag_crud.create
class TopicEntityTagPropSchemaCreate(BaseModel):
    qualifier: str


# use by topic_entity_tag_crud.create_prop
class TopicEntityTagPropSchemaPost(TopicEntityTagPropSchemaCreate):
    topic_entity_tag_id: int  # required as here topic_entity_tag_prop created separate from topic_entity_tag


# use by topic_entity_tag_crud.show, topic_enty_tag_prop as children of topic_entity_tag
class TopicEntityTagPropSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_prop_id: int
    qualifier: str


# use by topic_entity_tag_crud.show_prop, topic_entity_tag_prop as independent show
class TopicEntityTagPropSchemaShow(TopicEntityTagPropSchemaRelated):
    topic_entity_tag_id: int


class TopicEntityTagPropSchemaUpdate(BaseModel):
    qualifier: str

    class Config():
        orm_mode = True
        extra = "forbid"
        schema_extra = {
            "example": {
                "qualifier": "Q1"
            }
        }


# use by reference_crud
class TopicEntityTagSchemaCreate(BaseModel):
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None


# use by topic_entity_tag_crud
class TopicEntityTagSchemaPost(TopicEntityTagSchemaCreate):
    reference_curie: str


class TopicEntityTagSchemaRelated(AuditedObjectModelSchema):
    topic_entity_tag_id: int
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaRelated]] = None


class TopicEntityTagSchemaShow(TopicEntityTagSchemaRelated):
    reference_curie: str


class TopicEntityTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    topic: Optional[str] = ""
    entity_type: Optional[str] = ""
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str]
    taxon: Optional[str]
    note: Optional[str]
