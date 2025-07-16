from typing import List, Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class CrossReferencePageSchemaShow(BaseModel):
    """Schema for individual cross-reference pages."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    name: Optional[str] = None
    url: Optional[str] = None


class CrossReferenceSchemaRelated(AuditedObjectModelSchema):
    """Schema for related cross-reference details with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
        json_schema_extra={
            "example": {
                "curie": "MOD:curie",
                "pages": [
                    {"name": "page1", "url": "https://..."}
                ]
            }
        }
    )

    cross_reference_id: int
    curie: str
    curie_prefix: str
    url: Optional[str] = None
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    is_obsolete: Optional[bool] = None

    @field_validator('curie')
    def validate_curie(cls, v: str) -> str:
        if v.count(':') == 0:
            raise ValueError('curie must contain a single colon')
        return v


class CrossReferenceSchemaCreate(BaseModel):
    """Schema for creating a new cross-reference."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    curie: str
    pages: Optional[List[str]] = None
    is_obsolete: Optional[bool] = False


class CrossReferenceSchemaPost(CrossReferenceSchemaCreate):
    """Schema for posting cross-reference with resource and reference context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
        json_schema_extra={
            "example": {
                "curie": "MOD:curie",
                "pages": ["reference"],
                "reference_curie": "AGRKB:101"
            }
        }
    )

    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None


class CrossReferenceSchemaShow(AuditedObjectModelSchema):
    """Schema for showing cross-reference with all context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    cross_reference_id: int
    curie: str
    curie_prefix: str
    url: Optional[str] = None
    pages: Optional[List[CrossReferencePageSchemaShow]] = None
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None
    is_obsolete: Optional[bool] = None


class CrossReferenceSchemaUpdate(BaseModel):
    """Schema for updating cross-reference fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    curie: Optional[str] = None
    pages: Optional[List[str]] = None
    resource_curie: Optional[str] = None
    reference_curie: Optional[str] = None
    is_obsolete: Optional[bool] = None
