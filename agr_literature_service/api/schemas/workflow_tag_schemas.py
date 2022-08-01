from typing import Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import BaseModelShow


class WorkflowTagSchemaCreate(BaseModel):
    reference_curie: str
    workflow_tag_id: str
    mod_abbreviation: str
    created_by: Optional[str]


class WorkflowTagSchemaShow(BaseModelShow):
    reference_workflow_tag_id: int
    reference_curie: str
    workflow_tag_id: str
    mod_abbreviation: Optional[str]
    date_created: str
    date_updated: Optional[str]
    created_by: str
    updated_by: Optional[str]


class WorkflowTagSchemaRelated(BaseModel):
    reference_workflow_tag_id: Optional[int]
    date_created: Optional[str]
    workflow_tag_id: str
    mod_abbreviation: str
    date_updated: Optional[str]
    created_by: Optional[str]

    class Config():
        orm_mode = True
        extra = "forbid"


class WorkflowTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    workflow_tag_id: Optional[str] = None
    date_updated: Optional[str] = None
    updated_by: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
