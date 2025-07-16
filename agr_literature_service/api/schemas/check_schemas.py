from typing import Any, Dict, List

from pydantic import BaseModel, Field, ConfigDict


class AteamApiSchemaShow(BaseModel):
    """Schema for returning A-team API checks."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    checks: List[Any] = Field(default_factory=list)


class DatabaseSchemaShow(BaseModel):
    """Schema for returning database details."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    db_details: Dict[str, Any] = Field(default_factory=dict)


class EnvironmentsSchemaShow(BaseModel):
    """Schema for returning environment mappings."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    envs: Dict[str, Any] = Field(default_factory=dict)
