from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict

from .base_schemas import AuditedObjectModelSchema
from .person_lineage_relationship_enum import PersonPersonRole


class PersonLineageSchemaCreate(BaseModel):
    """Create payload for a validated (canonical) person-to-person relationship."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_one_id: int
    person_two_id: int
    relationship: PersonPersonRole
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class PersonLineageSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    relationship: Optional[PersonPersonRole] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class PersonLineageSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_lineage_id: int
    person_one_id: int
    person_two_id: int
    relationship: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
