from pydantic import BaseModel
from typing import Optional, List

from agr_literature_service.api.schemas import AuditedObjectModelSchema
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaRelated


class DatasetSchemaBase(AuditedObjectModelSchema):
    mod_abbreviation: str
    data_type_topic: str
    dataset_type: str


class DatasetSchemaPost(DatasetSchemaBase):
    notes: str


class DatasetSchemaShow(DatasetSchemaPost):
    dataset_id: int
    topic_entity_tags: [List[TopicEntityTagSchemaRelated]]


class DatasetSchemaDownload(DatasetSchemaPost):
    dataset_id: int
    data: List[str]


class DatasetSchemaUpdate(BaseModel):
    mod_abbreviation: Optional[constr(min_length=1)]  # type: ignore
    data_type_topic: Optional[constr(min_length=1)]  # type: ignore
    dataset_type: Optional[constr(min_length=1)]  # type: ignore
    notes: Optional[constr(min_length=1)]  # type: ignore
    date_created: Optional[constr(min_length=1)]  # type: ignore
    date_updated: Optional[constr(min_length=1)]  # type: ignore
    created_by: Optional[constr(min_length=1)]  # type: ignore
    updated_by: Optional[constr(min_length=1)]  # type: ignore

    class Config:
        orm_mode = True
        extra = "forbid"
