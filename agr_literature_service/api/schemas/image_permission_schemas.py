from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agr_literature_service.api.schemas.base_schemas import AuditedObjectModelSchema


class ImagePermissionSchemaPost(BaseModel):
    """Schema for creating publisher image permission text."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    name: str
    permission_text: Optional[str] = None
    permission_url: Optional[str] = None
    can_display_images: bool = False

    @field_validator('name')
    def name_is_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('name cannot be blank')
        return v


class ImagePermissionSchemaUpdate(BaseModel):
    """Schema for updating publisher image permission text."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    name: Optional[str] = None
    permission_text: Optional[str] = None
    permission_url: Optional[str] = None
    can_display_images: Optional[bool] = None

    @field_validator('name')
    def name_is_not_blank(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError('name cannot be blank')
        return v


class ImagePermissionSchemaShow(AuditedObjectModelSchema):
    """Schema for showing publisher image permission text."""
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    image_permission_id: int
    name: str
    permission_text: Optional[str] = None
    permission_url: Optional[str] = None
    can_display_images: bool


class ResourceImagePermissionSchemaPost(BaseModel):
    """Schema for linking a resource to image permission text."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    resource_curie: str
    image_permission_id: int
    start_year: Optional[int] = Field(default=None, ge=0)
    end_year: Optional[int] = Field(default=None, ge=0)
    notes: Optional[str] = None

    @field_validator('resource_curie')
    def resource_curie_is_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('resource_curie cannot be blank')
        return v


class ResourceImagePermissionSchemaUpdate(BaseModel):
    """Schema for updating a resource/image-permission link."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    image_permission_id: Optional[int] = None
    start_year: Optional[int] = Field(default=None, ge=0)
    end_year: Optional[int] = Field(default=None, ge=0)
    notes: Optional[str] = None


class ResourceImagePermissionSchemaShow(AuditedObjectModelSchema):
    """Schema for showing a resource/image-permission link."""
    model_config = ConfigDict(extra='ignore', from_attributes=True)

    resource_image_permission_id: int
    resource_id: int
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    image_permission_id: int
    image_permission: Optional[ImagePermissionSchemaShow] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    notes: Optional[str] = None
