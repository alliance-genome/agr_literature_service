from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class PersonNameSchemaCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: str
    primary: Optional[bool] = None


class PersonNameSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    primary: Optional[bool] = None


class PersonNameSchemaShow(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_name_id: int
    person_id: int
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: str
    primary: Optional[bool] = None


class PersonNameSchemaRelated(AuditedObjectModelSchema):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_name_id: int
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: str
    primary: Optional[bool] = None
