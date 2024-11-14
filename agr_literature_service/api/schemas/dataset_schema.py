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
    version: Union[int, None]


class DatasetSchemaDownload(DatasetSchemaPost):
    dataset_id: int
    data_training: Union[Dict[str, int], Dict[str, List[str]]]
    data_testing: Union[Dict[str, int], Dict[str, List[str]]]


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
