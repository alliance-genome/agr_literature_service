from typing import Optional

from pydantic import BaseModel, ConfigDict


class MeshDetailSchemaCreate(BaseModel):
    """Schema for creating a Mesh detail."""
    model_config = ConfigDict(
        extra='forbid',        # no unexpected fields
        from_attributes=True    # allow ORM-style init
    )
    heading_term: str
    qualifier_term: Optional[str] = None


class MeshDetailSchemaPost(MeshDetailSchemaCreate):
    """Schema for posting a Mesh detail with reference context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )
    reference_curie: str


class MeshDetailSchemaShow(MeshDetailSchemaPost):
    """Schema for showing a Mesh detail."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )
    mesh_detail_id: int


class MeshDetailSchemaRelated(MeshDetailSchemaCreate):
    """Schema for related Mesh detail entries."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )
    mesh_detail_id: int


class MeshDetailSchemaUpdate(BaseModel):
    """Schema for updating a Mesh detail."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )
    reference_curie: Optional[str] = None
    heading_term: Optional[str] = None
    qualifier_term: Optional[str] = None
