from typing import Optional, List, Union, Dict

from pydantic import StringConstraints, ConfigDict, BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema
from typing_extensions import Annotated


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
    title: Optional[Annotated[str, StringConstraints(min_length=1)]] = None  # type: ignore
    description: Optional[Annotated[str, StringConstraints(min_length=1)]] = None  # type: ignore
    date_created: Optional[Annotated[str, StringConstraints(min_length=1)]] = None  # type: ignore
    date_updated: Optional[Annotated[str, StringConstraints(min_length=1)]] = None  # type: ignore
    created_by: Optional[Annotated[str, StringConstraints(min_length=1)]] = None  # type: ignore
    updated_by: Optional[Annotated[str, StringConstraints(min_length=1)]] = None  # type: ignore
    model_config = ConfigDict(from_attributes=True, extra="forbid")


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
