from typing import Optional

from pydantic import BaseModel, ConfigDict

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class ReferencefileModSchemaPost(BaseModel):
    """Schema for creating a referencefile-mod association."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # allow ORM->model initialization
    )

    referencefile_id: int
    mod_abbreviation: Optional[str] = None


class ReferencefileModSchemaShow(AuditedObjectModelSchema, ReferencefileModSchemaPost):
    """Schema for showing a referencefile-mod association with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_mod_id: int


class ReferencefileModSchemaUpdate(BaseModel):
    """Schema for updating a referencefile-mod association."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_id: Optional[int] = None
    mod_abbreviation: Optional[str] = None


class ReferencefileModSchemaRelated(AuditedObjectModelSchema):
    """Schema for related referencefile-mod entries."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    referencefile_mod_id: int
    mod_abbreviation: Optional[str] = None
