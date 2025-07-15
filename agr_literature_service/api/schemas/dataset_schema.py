```python
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, constr

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class DatasetSchemaBase(AuditedObjectModelSchema):
    """Base schema for datasets with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    mod_abbreviation: str
    data_type: str
    dataset_type: str


class DatasetSchemaPost(DatasetSchemaBase):
    """Schema for posting new dataset metadata."""
    title: str
    description: str


class DatasetSchemaShow(DatasetSchemaPost):
    """Schema for showing dataset metadata including identifiers."""
    dataset_id: int
    version: int


class DatasetSchemaDownload(DatasetSchemaPost):
    """Schema for dataset download payload."""
    dataset_id: int
    data_training: Union[Dict[str, Union[str, List[str]]], Dict[str, Any]]
    data_testing: Union[Dict[str, Union[str, List[str]]], Dict[str, Any]]


class DatasetSchemaUpdate(BaseModel):
    """Schema for updating dataset metadata."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    title: Optional[constr(min_length=1)] = None
    description: Optional[constr(min_length=1)] = None
    date_created: Optional[constr(min_length=1)] = None
    date_updated: Optional[constr(min_length=1)] = None
    created_by: Optional[constr(min_length=1)] = None
    updated_by: Optional[constr(min_length=1)] = None


class DatasetEntrySchemaPost(DatasetSchemaBase):
    """Schema for posting entries to a dataset."""
    version: int
    reference_curie: str
    entity: Optional[str] = None
    classification_value: Optional[str] = None
    set_type: Optional[str] = Field('training', min_length=1)
    supporting_topic_entity_tag_id: Optional[int] = None
    supporting_workflow_tag_id: Optional[int] = None


class DatasetEntrySchemaDelete(DatasetSchemaBase):
    """Schema for deleting entries from a dataset."""
    version: int
    reference_curie: str
    entity: Optional[str] = None
```
