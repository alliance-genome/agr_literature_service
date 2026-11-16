from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class WorkflowTagSchemaCreate(BaseModel):
    """Schema for creating a workflow tag association."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # enable ORM->model initialization
    )

    workflow_tag_id: str
    mod_abbreviation: str


class WorkflowTagSchemaPost(WorkflowTagSchemaCreate):
    """Schema for posting a workflow tag with reference context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: str
    date_updated: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    date_created: Optional[datetime] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class WorkflowTagSchemaRelated(AuditedObjectModelSchema):
    """Schema for related workflow tag entries with audit fields."""
    model_config = ConfigDict(
        extra='ignore',
        from_attributes=True
    )

    reference_workflow_tag_id: Optional[int] = None
    workflow_tag_id: str
    mod_abbreviation: Optional[str] = None
    updated_by_email: Optional[str] = None
    updated_by_name: Optional[str] = None
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class WorkflowTagSchemaShow(WorkflowTagSchemaRelated):
    """Schema for showing a workflow tag with its reference context."""
    reference_curie: str
    curation_tag: Optional[str] = None
    note: Optional[str] = None


class WorkflowTagSchemaUpdate(BaseModel):
    """Schema for updating a workflow tag: only its note and curation tag (SCRUM-6845); its status
    changes through /workflow_tag/transition_to_workflow_status."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    curation_tag: Optional[str] = None
    note: Optional[str] = None


class WorkflowTagSchemaMerge(BaseModel):
    """The workflow tags of a reference merge: the ids to delete and the ids to move to the
    surviving reference (SCRUM-6845)."""
    model_config = ConfigDict(extra='forbid')

    delete_ids: List[int] = []
    move_ids: List[int] = []


class WorkflowTransitionSchemaPost(BaseModel):
    """Schema for posting a workflow transition event."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    curie_or_reference_id: str
    mod_abbreviation: str
    # A state, or the workflow process term itself to remove the paper's tag in that process
    new_workflow_tag_atp_id: str
    transition_type: str = "manual"
    # Set on the tag when given; a transition without them keeps the tag's current values
    curation_tag: Optional[str] = None
    note: Optional[str] = None
