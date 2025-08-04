from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class CurationStatusSchemaBase(AuditedObjectModelSchema):
    """Base schema for curation status with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    mod_abbreviation: str
    reference_curie: str


class CurationStatusSchemaPost(CurationStatusSchemaBase):
    """Schema for posting a new curation status."""
    topic: str
    curation_status: Optional[str] = None
    controlled_note: Optional[str] = None
    note: Optional[str] = None


class CurationStatusSchemaUpdate(BaseModel):
    """Schema for updating an existing curation status."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    curation_status: Optional[str] = None
    controlled_note: Optional[str] = None
    note: Optional[str] = None


class AggregatedCurationStatusAndTETInfoSchema(BaseModel):
    """Aggregated curation status and TET info, for combined views."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    curst_curation_status_id: Optional[int] = None
    curst_curation_status: Optional[str] = None
    curst_controlled_note: Optional[str] = None
    curst_note: Optional[str] = None
    curst_updated_by: Optional[str] = None
    curst_updated_by_email: Optional[str] = None
    curst_date_updated: Optional[str] = None

    topic_curie: str
    topic_name: str

    tet_info_date_created: Optional[str] = None
    tet_info_topic_source: List[str] = Field(default_factory=list)
    tet_info_has_data: bool = False
    tet_info_novel_data: bool = False
    tet_info_no_data: bool = False
