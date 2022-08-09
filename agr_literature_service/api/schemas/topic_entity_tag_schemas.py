from typing import List, Optional
from pydantic import BaseModel


class TopicEntityTagPropSchemaCreate(BaseModel):
    topic_entity_tag_id: Optional[int] = None  # created on the fly so not needed here
    qualifier: str
    date_created: Optional[str]
    date_updated: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]


class TopicEntityTagPropSchemaShow(BaseModel):
    topic_entity_tag_prop_id: int
    topic_entity_tag_id: int
    qualifier: str
    date_created: str
    date_updated: Optional[str]
    created_by: str
    updated_by: Optional[str]


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


class TopicEntityTagSchemaCreate(BaseModel):
    reference_curie: str
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    date_created: Optional[str]
    date_updated: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None


class TopicEntityTagSchemaRelated(BaseModel):
    reference_curie: Optional[str]
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    date_created: Optional[str]
    date_updated: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None


class TopicEntityTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    topic: Optional[str] = ""
    entity_type: Optional[str] = ""
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str]
    taxon: Optional[str]
    note: Optional[str]
    date_created: Optional[str]
    date_updated: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaUpdate]]


class TopicEntityTagSchemaShow(BaseModel):
    topic_entity_tag_id: int
    reference_curie: str
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    taxon: str
    note: Optional[str]
    date_created: str
    date_updated: Optional[str]
    created_by: str
    updated_by: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaShow]] = None
