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
from literature.schemas.crossReference import CrossReferenceRelated


class ResourceSchemaPost(BaseModel):
    title: str

    titleSynonyms: Optional[List[str]] = None
    abbreviationSynonyms: Optional[List[str]] = None
    isoAbbreviation: Optional[str] = None
    abbreviationSynonyms: Optional[List[str]] = None
    medlineAbbreviation: Optional[str] = None
    copyrightDate: Optional[datetime] = None
    publisher: Optional[str] = None
    printISSN: Optional[str] = None
    onlineISSN: Optional[str] = None
    pages: Optional[str] = None
    volumes: Optional[List[str]] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    crossReferences: Optional[List[CrossReferenceRelated]] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    editors: Optional[List[EditorSchemaPost]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ResourceSchemaShow(BaseModelShow):
    curie: str
    title: str

    titleSynonyms: Optional[List[str]] = None
    abbreviationSynonyms: Optional[List[str]] = None
    isoAbbreviation: Optional[str] = None
    medlineAbbreviation: Optional[str] = None
    copyrightDate: Optional[datetime] = None
    publisher: Optional[str] = None
    printISSN: Optional[str] = None
    onlineISSN: Optional[str] = None
    pages: Optional[str] = None
    volumes: Optional[List[str]] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    crossReferences: Optional[List[CrossReferenceRelated]] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None

    class Config():
        orm_mode = True
        extra = "forbid"
