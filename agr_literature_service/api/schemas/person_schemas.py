from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, ConfigDict

from .base_schemas import AuditedObjectModelSchema
from .email_schemas import EmailSchemaCreate, EmailSchemaRelated
from .person_cross_reference_schemas import (
    PersonCrossReferenceSchemaCreate,
    PersonCrossReferenceSchemaRelated,
)

_types = {
    "EmailSchemaCreate": EmailSchemaCreate,
    "EmailSchemaRelated": EmailSchemaRelated,
    "PersonCrossReferenceSchemaCreate": PersonCrossReferenceSchemaCreate,
    "PersonCrossReferenceSchemaRelated": PersonCrossReferenceSchemaRelated,
}


class PersonSchemaPost(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    display_name: str
    curie: Optional[str] = None
    okta_id: Optional[str] = None
    mod_roles: Optional[List[str]] = None
    # forward-ref strings
    emails: Optional[List["EmailSchemaCreate"]] = None
    cross_references: Optional[List["PersonCrossReferenceSchemaCreate"]] = None


# Back-compat alias
PersonSchemaCreate = PersonSchemaPost


class PersonSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    display_name: Optional[str] = None
    curie: Optional[str] = None
    okta_id: Optional[str] = None
    mod_roles: Optional[List[str]] = None


class PersonSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_id: int
    display_name: str
    curie: Optional[str] = None
    okta_id: Optional[str] = None
    mod_roles: Optional[List[str]] = None
    emails: Optional[List["EmailSchemaRelated"]] = None
    cross_references: Optional[List["PersonCrossReferenceSchemaRelated"]] = None
