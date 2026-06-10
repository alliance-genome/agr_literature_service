from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, ConfigDict, model_validator

from .base_schemas import AuditedObjectModelSchema
from .laboratory_cross_reference_schemas import (
    LaboratoryCrossReferenceSchemaCreate,
    LaboratoryCrossReferenceSchemaRelated,
)

# Controlled vocabularies enforced at the API (Pydantic) layer only — no DB constraint.
LaboratoryStatus = Literal["active", "closed", "unknown"]
EmailVisibility = Literal["public", "logged_in_user", "not_shown"]

# Fields that always carry a default, so supplying only these does not satisfy the
# "at least one substantive field" requirement on create.
_DEFAULTED_FIELDS = {"status", "lab_is_open", "email_visibility"}


class LaboratorySchemaPost(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    # curie is server-derived from laboratory_id, not accepted from clients
    name: Optional[str] = None
    strain_designation: Optional[str] = None
    institution: Optional[List[str]] = None
    webpage: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    street_address: Optional[str] = None
    email: Optional[List[str]] = None
    email_visibility: EmailVisibility = "not_shown"
    lab_is_open: bool = False
    status: LaboratoryStatus = "active"
    research_area: Optional[str] = None
    short_research_description: Optional[str] = None
    additional_information: Optional[str] = None
    private_note: Optional[str] = None
    cross_references: Optional[List["LaboratoryCrossReferenceSchemaCreate"]] = None

    @model_validator(mode="after")
    def _at_least_one_field_besides_status(self):
        # A laboratory must be created with at least one substantive field. Because
        # status/lab_is_open/email_visibility always carry defaults, only count
        # fields the caller explicitly supplied.
        supplied = self.model_fields_set - _DEFAULTED_FIELDS
        if not supplied:
            raise ValueError(
                "A laboratory must have at least one field besides status "
                "(name, strain_designation, institution, etc.)."
            )
        return self


# Back-compat alias
LaboratorySchemaCreate = LaboratorySchemaPost


class LaboratorySchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    name: Optional[str] = None
    strain_designation: Optional[str] = None
    institution: Optional[List[str]] = None
    webpage: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    street_address: Optional[str] = None
    email: Optional[List[str]] = None
    email_visibility: Optional[EmailVisibility] = None
    lab_is_open: Optional[bool] = None
    status: Optional[LaboratoryStatus] = None
    research_area: Optional[str] = None
    short_research_description: Optional[str] = None
    additional_information: Optional[str] = None
    private_note: Optional[str] = None


class LaboratorySchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    laboratory_id: int
    curie: Optional[str] = None
    name: Optional[str] = None
    strain_designation: Optional[str] = None
    institution: Optional[List[str]] = None
    webpage: Optional[List[str]] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    street_address: Optional[str] = None
    email: Optional[List[str]] = None
    email_visibility: Optional[str] = None
    lab_is_open: bool = False
    status: str = "active"
    research_area: Optional[str] = None
    short_research_description: Optional[str] = None
    additional_information: Optional[str] = None
    private_note: Optional[str] = None
    cross_references: Optional[List["LaboratoryCrossReferenceSchemaRelated"]] = None
