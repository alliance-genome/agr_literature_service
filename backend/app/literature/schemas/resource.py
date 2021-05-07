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


class ResourceSchemaPost(BaseModel):
    title: Optional[str] = None
    isoAbbreviation: Optional[str] = None
    medlineAbbreviation: Optional[str] = None
    copyrightDate: Optional[datetime] = None
    publisher: Optional[str] = None
    printISSN: Optional[str] = None
    onlineISSN: Optional[str] = None
    pages: Optional[int] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    editors: Optional[List[EditorSchemaPost]] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ResourceSchemaShow(BaseModelShow):
    curie: Optional[str] = None
    title: Optional[str] = None
    isoAbbreviation: Optional[str] = None
    medlineAbbreviation: Optional[str] = None
    copyrightDate: Optional[datetime] = None
    publisher: Optional[str] = None
    printISSN: Optional[str] = None
    onlineISSN: Optional[str] = None
    pages: Optional[int] = None
    abstract: Optional[str] = None
    summary: Optional[str] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None

    class Config():
        orm_mode = True
        extra = "forbid"
