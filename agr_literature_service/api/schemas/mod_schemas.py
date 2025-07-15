```python
from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ModSchemaPost(BaseModel):
    """Schema for posting a new MOD entry."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # enable ORM->model initialization
    )

    abbreviation: str
    short_name: str
    full_name: str


class ModSchemaShow(AuditedObjectModelSchema):
    """Schema for showing MOD details with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    mod_id: int
    abbreviation: str
    short_name: str
    full_name: str
    taxon_ids: Optional[List[str]] = None


class ModSchemaUpdate(BaseModel):
    """Schema for updating MOD details."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    abbreviation: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None
    taxon_ids: Optional[List[str]] = None


class ModSchemaCreate(ModSchemaPost):
    """Alias for ModSchemaPost when creating a MOD."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )
```
