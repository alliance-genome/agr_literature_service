from typing import Optional
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


class WorkflowTagSchemaShow(WorkflowTagSchemaRelated):
    """Schema for showing a workflow tag with its reference context."""
    reference_curie: str


class WorkflowTagSchemaUpdate(BaseModel):
    """Schema for updating a workflow tag association."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: Optional[str] = None
    mod_abbreviation: Optional[str] = None
    workflow_tag_id: Optional[str] = None


class WorkflowTransitionSchemaPost(BaseModel):
    """Schema for posting a workflow transition event."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    curie_or_reference_id: str
    mod_abbreviation: str
    new_workflow_tag_atp_id: str
    transition_type: str = "manual"
