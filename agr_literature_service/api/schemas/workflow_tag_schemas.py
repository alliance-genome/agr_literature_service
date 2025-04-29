from typing import Optional
from pydantic import ConfigDict, BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema


# use by reference crud
class WorkflowTagSchemaCreate(BaseModel):
    workflow_tag_id: str
    mod_abbreviation: str


# use by workflow_tag crud
class WorkflowTagSchemaPost(BaseModel):
    reference_curie: str
    workflow_tag_id: str
    mod_abbreviation: str

# used by parents, such as reference to show workflow_tag
class WorkflowTagSchemaRelated(AuditedObjectModelSchema):
    reference_workflow_tag_id: Optional[int]
    workflow_tag_id: str
    mod_abbreviation: Optional[str]
    updated_by_email: Optional[str]


# used by workflow_tag_crud
class WorkflowTagSchemaShow(WorkflowTagSchemaRelated):
    reference_curie: str


class WorkflowTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    workflow_tag_id: Optional[str] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class WorkflowTransitionSchemaPost(BaseModel):
    curie_or_reference_id: str
    mod_abbreviation: str
    new_workflow_tag_atp_id: str
    transition_type: str = "manual"
