from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agr_literature_service.api.schemas import AuditedObjectModelSchema, EditorSchemaPost, EditorSchemaShow, CrossReferenceSchemaRelated
from agr_literature_service.api.schemas.cross_reference_schemas import CrossReferenceSchemaCreate


class ResourceSchemaPost(BaseModel):
    """Schema for creating a resource entry."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    title: str
    title_synonyms: List[str] = Field(default_factory=list)
    abbreviation_synonyms: List[str] = Field(default_factory=list)
    iso_abbreviation: Optional[str] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: List[str] = Field(default_factory=list)
    abstract: Optional[str] = None
    summary: Optional[str] = None
    cross_references: List[CrossReferenceSchemaCreate] = Field(default_factory=list)
    editors: List[EditorSchemaPost] = Field(default_factory=list)
    open_access: Optional[bool] = False

    @field_validator('title')
    def title_is_some(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('Cannot set title to None or blank string')
        return v


class ResourceSchemaUpdate(BaseModel):
    """Schema for updating resource fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    title: Optional[str] = None
    title_synonyms: List[str] = Field(default_factory=list)
    abbreviation_synonyms: List[str] = Field(default_factory=list)
    iso_abbreviation: Optional[str] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: List[str] = Field(default_factory=list)
    abstract: Optional[str] = None
    summary: Optional[str] = None
    open_access: Optional[bool] = False

    @field_validator('title')
    def title_is_some(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError('Cannot set title to blank string')
        return v


class ResourceSchemaShow(AuditedObjectModelSchema):
    """Schema for showing resource with audit fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    resource_id: int
    curie: Optional[str] = None
    title: str

    title_synonyms: List[str] = Field(default_factory=list)
    abbreviation_synonyms: List[str] = Field(default_factory=list)
    iso_abbreviation: Optional[str] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: List[str] = Field(default_factory=list)
    abstract: Optional[str] = None
    summary: Optional[str] = None
    cross_references: List[CrossReferenceSchemaRelated] = Field(default_factory=list)
    editors: List[EditorSchemaShow] = Field(default_factory=list)
    open_access: Optional[bool] = None
