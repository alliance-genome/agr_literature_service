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


class TopicEntityTagSchemaCreate(BaseModel):
    reference_curie: str
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    species_id: int
    note: Optional[str]
    date_created: Optional[str]
    date_updated: Optional[str]
    created_by: Optional[str]
    updated_by: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None


class TopicEntityTagSchemaShow(BaseModel):
    reference_curie: str
    topic: str
    entity_type: str
    alliance_entity: Optional[str] = None
    mod_entity: Optional[str] = None
    new_entity: Optional[str] = None
    species_id: int
    note: Optional[str]
    date_created: str
    date_updated: Optional[str]
    created_by: str
    updated_by: Optional[str]
    props: Optional[List[TopicEntityTagPropSchemaCreate]] = None
