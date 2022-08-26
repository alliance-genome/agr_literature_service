from typing import Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


class WorkflowTagSchemaCreate(BaseModel):
    reference_curie: str
    workflow_tag_id: str
    mod_abbreviation: str
    created_by: Optional[str]


class WorkflowTagSchemaShow(AuditedObjectModelSchema):
    reference_workflow_tag_id: int
    reference_curie: str
    workflow_tag_id: str
    mod_abbreviation: Optional[str]


class WorkflowTagSchemaRelated(AuditedObjectModelSchema):
    reference_workflow_tag_id: Optional[int]
    workflow_tag_id: str
    mod_abbreviation: str

    class Config():
        orm_mode = True
        extra = "forbid"


class WorkflowTagSchemaUpdate(AuditedObjectModelSchema):
    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    workflow_tag_id: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
