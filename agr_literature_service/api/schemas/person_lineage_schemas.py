from __future__ import annotations
from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, ConfigDict

from .base_schemas import AuditedObjectModelSchema
from .person_lineage_relationship_enum import PersonPersonRole


class PersonLineageSchemaCreate(BaseModel):
    """Create payload for a validated (canonical) person-to-person relationship.

    Both people are required and given by curie OR integer id (resolved server-side).
    """
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_subject_curie_or_id: Union[str, int]
    person_object_curie_or_id: Union[str, int]
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
    person_subject_id: int
    person_subject_curie: Optional[str] = None
    person_object_id: int
    person_object_curie: Optional[str] = None
    relationship: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
