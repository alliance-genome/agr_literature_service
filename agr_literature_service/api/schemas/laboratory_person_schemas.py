from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict

from .base_schemas import AuditedObjectModelSchema
from .laboratory_position_enum import LabPosition


class LaboratoryPersonSchemaCreate(BaseModel):
    """Create payload linking a person to a laboratory (laboratory from path)."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_id: int
    is_pi: Optional[datetime] = None
    former_pi: Optional[datetime] = None
    alum: Optional[datetime] = None
    is_lab_contact: bool = False
    can_edit_lab: bool = False
    lab_position: Optional[LabPosition] = None


class LaboratoryPersonSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    is_pi: Optional[datetime] = None
    former_pi: Optional[datetime] = None
    alum: Optional[datetime] = None
    is_lab_contact: Optional[bool] = None
    can_edit_lab: Optional[bool] = None
    lab_position: Optional[LabPosition] = None


class LaboratoryPersonSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    laboratory_person_id: int
    laboratory_id: int
    person_id: int
    is_pi: Optional[datetime] = None
    former_pi: Optional[datetime] = None
    alum: Optional[datetime] = None
    is_lab_contact: bool = False
    can_edit_lab: bool = False
    lab_position: Optional[str] = None


class LaboratoryPersonSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    laboratory_person_id: int
    person_id: int
    is_pi: Optional[datetime] = None
    former_pi: Optional[datetime] = None
    alum: Optional[datetime] = None
    is_lab_contact: bool = False
    can_edit_lab: bool = False
    lab_position: Optional[str] = None
