from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.schemas.base import BaseModelShow
from literature.schemas.author import AuthorSchemaPost
from literature.schemas.author import AuthorSchemaShow
from literature.schemas.editor import EditorSchemaPost
from literature.schemas.editor import EditorSchemaShow
from literature.schemas.cross_reference import CrossReferenceSchemaRelated


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


class ResourceSchemaShow(BaseModelShow):
    curie: str
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
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None

    class Config():
        orm_mode = True
        extra = "forbid"
