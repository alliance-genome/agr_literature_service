from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas import BaseModelShow
from literature.schemas import AuthorSchemaPost
from literature.schemas import AuthorSchemaShow
from literature.schemas import EditorSchemaPost
from literature.schemas import EditorSchemaShow
from literature.schemas import CrossReferenceSchemaRelated
from literature.schemas import CrossReferenceSchema


class ResourceSchemaPost(BaseModel):
    title: str

    title_synonyms: Optional[List[str]] = None
    abbreviation_synonyms: Optional[List[str]] = None
    iso_abbreviation: Optional[str] = None
    abbreviation_synonyms: Optional[List[str]] = None
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
    authors: Optional[List[AuthorSchemaPost]] = None
    editors: Optional[List[EditorSchemaPost]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ResourceSchemaUpdate(BaseModel):
    title: Optional[str] = None

    title_synonyms: Optional[List[str]] = None
    abbreviation_synonyms: Optional[List[str]] = None
    iso_abbreviation: Optional[str] = None
    abbreviation_synonyms: Optional[List[str]] = None
    medline_abbreviation: Optional[str] = None
    copyright_date: Optional[datetime] = None
    publisher: Optional[str] = None
    print_issn: Optional[str] = None
    online_issn: Optional[str] = None
    pages: Optional[str] = None
    volumes: Optional[List[str]] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None

    @validator('title')
    def title_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set title to None')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class ResourceSchemaShow(BaseModelShow):
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
    cross_references: Optional[List[CrossReferenceSchema]] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None
