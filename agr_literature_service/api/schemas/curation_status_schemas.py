from typing import Optional, Union, List

from pydantic import BaseModel

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class CurationStatusSchemaBase(AuditedObjectModelSchema):
    mod_abbreviation: str
    reference_curie: str


class CurationStatusSchemaPost(CurationStatusSchemaBase):
    topic: str
    curation_status: Optional[str]
    controlled_note: Optional[str]
    note: Optional[str]


class CurationStatusSchemaUpdate(BaseModel):
    curation_status: Optional[str]
    controlled_note: Optional[str]
    note: Optional[str]

    class Config:
        orm_mode = True
        extra = "forbid"


class AggregatedCurationStatusAndTETInfoSchema(BaseModel):
    curst_curation_status_id: Union[int, None] = None
    curst_curation_status: Union[str, None] = None
    curst_controlled_note: Union[str, None] = None
    curst_note: Union[str, None] = None
    curst_updated_by: Union[str, None] = None
    curst_updated_by_email: Union[str, None] = None
    curst_date_updated: Union[str, None] = None
    topic_curie: str
    topic_name: str
    tet_info_date_created: Union[str, None] = None
    tet_info_topic_source: List[str] = []
    tet_info_has_data: Union[bool, None] = None
    tet_info_novel_data: Union[bool, None] = None
    tet_info_no_data: Union[bool, None] = None

