from typing import Optional

from pydantic import BaseModel, ConfigDict


class AuditedObjectModelSchema(BaseModel):
    """Base schema providing audit fields."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # allow ORM object -> model
    )

    date_created: Optional[str] = None
    date_updated: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
