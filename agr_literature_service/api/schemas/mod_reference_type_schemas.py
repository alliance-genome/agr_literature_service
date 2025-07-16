from typing import Optional

from pydantic import BaseModel, ConfigDict


class ModReferenceTypeSchemaCreate(BaseModel):
    """Schema for creating a mod-reference-type association."""
    model_config = ConfigDict(
        extra='forbid',        # forbid unexpected fields
        from_attributes=True    # enable ORM->model initialization
    )

    reference_type: str
    mod_abbreviation: Optional[str] = None


class ModReferenceTypeSchemaPost(ModReferenceTypeSchemaCreate):
    """Schema for posting a mod-reference-type with reference context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: str


class ModReferenceTypeSchemaShow(ModReferenceTypeSchemaPost):
    """Schema for showing a mod-reference-type entry including its ID."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    mod_reference_type_id: int


class ModReferenceTypeSchemaRelated(ModReferenceTypeSchemaCreate):
    """Schema for related mod-reference-type entries."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    mod_reference_type_id: int


class ModReferenceTypeSchemaUpdate(BaseModel):
    """Schema for updating a mod-reference-type entry."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: Optional[str] = None
    reference_type: Optional[str] = None
    mod_abbreviation: Optional[str] = None
