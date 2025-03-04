from typing import Optional, List, Union, Dict

from pydantic import BaseModel, constr

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class DatasetSchemaBase(AuditedObjectModelSchema):
    mod_abbreviation: str
    data_type: str
    dataset_type: str


class DatasetSchemaPost(DatasetSchemaBase):
    title: str
    description: str


class DatasetSchemaShow(DatasetSchemaPost):
    dataset_id: int
    version: int


class DatasetSchemaDownload(DatasetSchemaPost):
    dataset_id: int
    data_training: Union[Dict[str, str], Dict[str, List[str]]]
    data_testing: Union[Dict[str, str], Dict[str, List[str]]]


class DatasetSchemaUpdate(BaseModel):
    title: Optional[constr(min_length=1)]  # type: ignore
    description: Optional[constr(min_length=1)]  # type: ignore
    date_created: Optional[constr(min_length=1)]  # type: ignore
    date_updated: Optional[constr(min_length=1)]  # type: ignore
    created_by: Optional[constr(min_length=1)]  # type: ignore
    updated_by: Optional[constr(min_length=1)]  # type: ignore

    class Config:
        orm_mode = True
        extra = "forbid"


class DatasetEntrySchemaPost(DatasetSchemaBase):
    version: int
    reference_curie: str
    entity: Optional[str] = None
    classification_value: Optional[str] = None
    set_type: Optional[str] = "training"
    supporting_topic_entity_tag_id: Optional[int] = None
    supporting_workflow_tag_id: Optional[int] = None


class DatasetEntrySchemaDelete(DatasetSchemaBase):
    version: int
    reference_curie: str
    entity: Optional[str] = None
