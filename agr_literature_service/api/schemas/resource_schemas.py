from datetime import datetime
from typing import List, Optional

from pydantic import ConfigDict, BaseModel, field_validator

from agr_literature_service.api.schemas import (AuditedObjectModelSchema,
                                                CrossReferenceSchemaRelated, EditorSchemaPost,
                                                EditorSchemaShow)
from agr_literature_service.api.schemas.cross_reference_schemas import CrossReferenceSchemaCreate


class ResourceSchemaPost(BaseModel):
    title: str

    title_synonyms: Optional[List[str]] = None
    abbreviation_synonyms: Optional[List[str]] = None
    iso_abbreviation: Optional[str] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: Optional[List[str]] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    cross_references: Optional[List[CrossReferenceSchemaCreate]] = None
    editors: Optional[List[EditorSchemaPost]] = None
    open_access: Optional[bool] = False

    @field_validator('title')
    def title_is_some(cls, v):
        if not v:
            raise ValueError('Cannot set title to None')
        return v
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ResourceSchemaUpdate(BaseModel):
    title: Optional[str] = None

    title_synonyms: Optional[List[str]] = None
    abbreviation_synonyms: Optional[List[str]] = None
    iso_abbreviation: Optional[str] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: Optional[List[str]] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    open_access: Optional[bool] = False

    @field_validator('title')
    def title_is_some(cls, v):
        if not v:
            raise ValueError('Cannot set title to None')
        return v
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ResourceSchemaShow(AuditedObjectModelSchema):
    resource_id: int
    curie: Optional[str] = None
    title: str

    title_synonyms: Optional[List[str]] = None
    abbreviation_synonyms: Optional[List[str]] = None
    iso_abbreviation: Optional[str] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: Optional[List[str]] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    editors: Optional[List[EditorSchemaShow]] = None
    open_access: Optional[bool] = None
