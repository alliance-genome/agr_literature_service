from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class PersonNameSchemaCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: str
    primary: Optional[bool] = None

    @field_validator("last_name")
    @classmethod
    def _validate_last_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("last_name cannot be empty or whitespace")
        return v.strip()


class PersonNameSchemaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    primary: Optional[bool] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_null_last_name(cls, data: Any) -> Any:
        # last_name is NOT NULL in the DB — reject explicit null from clients.
        # Omitting the field is still fine (PATCH no-op for that field).
        if isinstance(data, dict) and "last_name" in data and data["last_name"] is None:
            raise ValueError("last_name cannot be null; omit the field to leave it unchanged")
        return data

    @field_validator("last_name")
    @classmethod
    def _validate_last_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not v.strip():
            raise ValueError("last_name cannot be empty or whitespace")
        return v.strip()


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
