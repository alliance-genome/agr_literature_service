from typing import Optional
from pydantic import BaseModel
from agr_literature_service.api.schemas import AuditedObjectModelSchema

#use by reference_crud
class WorkflowTagSchemaPost(BaseModel):
    workflow_tag_id: str
    mod_abbreviation: str

class WorkflowTagSchemaCreate(WorkflowTagSchemaPost):
    reference_curie: str
    
#used by parents, such as reference to show workflow_tag
class WorkflowTagSchemaShowRelated(AuditedObjectModelSchema):
    reference_workflow_tag_id: int
    workflow_tag_id: str
    mod_abbreviation: Optional[str]

#used by workflow_tag_crud
class WorkflowTagSchemaShow(WorkflowTagSchemaShowRelated):
    reference_curie: str

class WorkflowTagSchemaRelated(AuditedObjectModelSchema):
    reference_workflow_tag_id: Optional[int]
    workflow_tag_id: str
    mod_abbreviation: str

    class Config():
        orm_mode = True
        extra = "forbid"

class WorkflowTagSchemaUpdate(BaseModel):
    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    workflow_tag_id: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"
