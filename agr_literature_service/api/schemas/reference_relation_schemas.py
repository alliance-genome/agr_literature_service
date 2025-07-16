from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import ReferenceRelationType


class ReferenceRelationSchemaPost(BaseModel):
    """Schema for creating a reference relation."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    reference_curie_from: str
    reference_curie_to: str
    reference_relation_type: ReferenceRelationType

    @field_validator('reference_curie_from')
    def from_must_be_alliance_reference_curie(cls, v: str) -> str:
        if not v.startswith("AGRKB:101"):
            raise ValueError('reference_curie_from must start with "AGRKB:101"')
        return v

    @field_validator('reference_curie_to')
    def to_must_be_alliance_reference_curie(cls, v: str) -> str:
        if not v.startswith("AGRKB:101"):
            raise ValueError('reference_curie_to must start with "AGRKB:101"')
        return v


class ReferenceRelationSchemaShow(ReferenceRelationSchemaPost):
    """Schema for showing a reference relation with its ID."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    reference_relation_id: int


class ReferenceRelationSchemaPatch(BaseModel):
    """Schema for partially updating a reference relation."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    reference_curie_from: Optional[str] = None
    reference_curie_to: Optional[str] = None
    reference_relation_type: Optional[ReferenceRelationType] = None


class ReferenceRelationSchemaRelated(BaseModel):
    """Schema for related reference relation entries."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    reference_relation_id: Optional[int] = None
    reference_curie_from: Optional[str] = None
    reference_curie_to: Optional[str] = None
    reference_relation_type: Optional[ReferenceRelationType] = None
