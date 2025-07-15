from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema


class EditorSchemaPost(BaseModel):
    """Schema for posting editor details."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
        json_schema_extra={
            "example": {
                "order": 1,
                "name": "string",
                "first_name": "string",
                "last_name": "string",
                "orcid": "ORCID:string"
            }
        }
    )

    order: Optional[int] = None
    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    @field_validator('orcid')
    def check_orcids(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith('ORCID:'):
            raise ValueError('Orcid ID must start with "ORCID:"')
        return v


class EditorSchemaShow(AuditedObjectModelSchema):
    """Schema for showing editor with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    editor_id: int
    order: Optional[int] = None
    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    orcid: Optional[str] = None


class EditorSchemaCreate(EditorSchemaPost):
    """Schema for creating an editor entry."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None
