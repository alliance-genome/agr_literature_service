from typing import Optional, List, Union, Dict

from pydantic import BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaRelated


class DatasetSchemaBase(AuditedObjectModelSchema):
    mod_abbreviation: str
    data_type_topic: str
    dataset_type: str


class DatasetSchemaPost(DatasetSchemaBase):
    title: str
    description: str


class DatasetSchemaShow(DatasetSchemaPost):
    dataset_id: int
    version: Union[int, None]
    topic_entity_tags: [List[TopicEntityTagSchemaRelated]]


class DatasetSchemaDownload(DatasetSchemaPost):
    dataset_id: int
    document_data_training: List[Dict[str, int]]
    document_data_testing: List[Dict[str, int]]
    entity_data_training: List[Dict[str, List[str]]]
    entity_data_testing: List[Dict[str, List[str]]]


class DatasetSchemaUpdate(BaseModel):
    title: Optional[const(min_length=1)]  # type: ignore
    description: Optional[constr(min_length=1)]  # type: ignore
    date_created: Optional[constr(min_length=1)]  # type: ignore
    date_updated: Optional[constr(min_length=1)]  # type: ignore
    created_by: Optional[constr(min_length=1)]  # type: ignore
    updated_by: Optional[constr(min_length=1)]  # type: ignore

    class Config:
        orm_mode = True
        extra = "forbid"
