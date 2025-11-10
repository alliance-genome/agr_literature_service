from __future__ import annotations

from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, field_validator

from .base_schemas import AuditedObjectModelSchema


class PersonSettingSchemaCreate(BaseModel):
    """Create payload for a new person_setting row."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_id: int
    component_name: str
    setting_name: str
    default_setting: bool = False
    json_settings: Dict[str, Any] = {}

    @field_validator("component_name", "setting_name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("must be a non-empty string")
        return v


class PersonSettingSchemaUpdate(BaseModel):
    """Partial update payload for person_setting."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    # Allow moving a setting to a different person if needed; set to None to forbid.
    person_id: Optional[int] = None
    component_name: Optional[str] = None
    setting_name: Optional[str] = None
    default_setting: Optional[bool] = None
    json_settings: Optional[Dict[str, Any]] = None

    @field_validator("component_name", "setting_name")
    @classmethod
    def _non_empty_optional(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip()
        if not v:
            raise ValueError("must be a non-empty string")
        return v


class PersonSettingSchemaShow(AuditedObjectModelSchema):
    """Full person_setting record for detail/list endpoints."""
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    person_setting_id: int
    person_id: int
    component_name: str
    setting_name: str
    default_setting: bool
    json_settings: Dict[str, Any]
