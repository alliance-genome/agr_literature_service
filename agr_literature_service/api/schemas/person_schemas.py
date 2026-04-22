from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional
from pydantic import BaseModel, ConfigDict

from .base_schemas import AuditedObjectModelSchema
from .email_schemas import EmailSchemaCreate, EmailSchemaRelated
from .person_cross_reference_schemas import (
    PersonCrossReferenceSchemaCreate,
    PersonCrossReferenceSchemaRelated,
)
from .person_name_schemas import PersonNameSchemaCreate, PersonNameSchemaRelated
from .person_note_schemas import PersonNoteSchemaCreate, PersonNoteSchemaRelated

ActiveStatus = Literal["active", "retired", "deceased"]

_types = {
    "EmailSchemaCreate": EmailSchemaCreate,
    "EmailSchemaRelated": EmailSchemaRelated,
    "PersonCrossReferenceSchemaCreate": PersonCrossReferenceSchemaCreate,
    "PersonCrossReferenceSchemaRelated": PersonCrossReferenceSchemaRelated,
    "PersonNameSchemaCreate": PersonNameSchemaCreate,
    "PersonNameSchemaRelated": PersonNameSchemaRelated,
    "PersonNoteSchemaCreate": PersonNoteSchemaCreate,
    "PersonNoteSchemaRelated": PersonNoteSchemaRelated,
}


class PersonSchemaPost(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    display_name: str
    curie: Optional[str] = None
    okta_id: Optional[str] = None
    mod_roles: Optional[List[str]] = None
    webpage: Optional[List[str]] = None
    active_status: ActiveStatus = "active"
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    street_address: Optional[str] = None
    biography_research_interest: Optional[str] = None
    # forward-ref strings
    emails: Optional[List["EmailSchemaCreate"]] = None
    cross_references: Optional[List["PersonCrossReferenceSchemaCreate"]] = None
    names: Optional[List["PersonNameSchemaCreate"]] = None
    notes: Optional[List["PersonNoteSchemaCreate"]] = None


# Back-compat alias
PersonSchemaCreate = PersonSchemaPost


class PersonSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    display_name: Optional[str] = None
    curie: Optional[str] = None
    okta_id: Optional[str] = None
    mod_roles: Optional[List[str]] = None
    webpage: Optional[List[str]] = None
    active_status: Optional[ActiveStatus] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    street_address: Optional[str] = None
    biography_research_interest: Optional[str] = None


class PersonSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_id: int
    display_name: str
    curie: Optional[str] = None
    okta_id: Optional[str] = None
    mod_roles: Optional[List[str]] = None
    webpage: Optional[List[str]] = None
    active_status: str = "active"
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    street_address: Optional[str] = None
    address_last_updated: Optional[datetime] = None
    biography_research_interest: Optional[str] = None
    emails: Optional[List["EmailSchemaRelated"]] = None
    cross_references: Optional[List["PersonCrossReferenceSchemaRelated"]] = None
    names: Optional[List["PersonNameSchemaRelated"]] = None
    notes: Optional[List["PersonNoteSchemaRelated"]] = None
