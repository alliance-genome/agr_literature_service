from typing import List, Optional

from pydantic import BaseModel

from literature.schemas.base import BaseModelShow
from literature.schemas.author import AuthorSchemaPost
from literature.schemas.author import AuthorSchemaShow
from literature.schemas.editor import EditorSchemaPost
from literature.schemas.editor import EditorSchemaShow
from literature.schemas.resource import ResourceSchemaShow

class ReferenceSchemaPost(BaseModel):
    title: Optional[str] = None
    datePublished: Optional[str] = None
    dateArrivedInPubMed: Optional[str] = None
    dateLastModified: Optional[str] = None
    volume: Optional[str] = None
    abstract: Optional[str] = None
    citation: Optional[str] = None
    pubMedType: Optional[str] = None
    publisher: Optional[str] = None
    issueName: Optional[str] = None
    issueDate: Optional[str] = None
    resourceAbbreviation: Optional[str] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    editors: Optional[List[EditorSchemaPost]] = None
    resource: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaUpdate(BaseModel):
    title: Optional[str] = None
    datePublished: Optional[str] = None
    dateArrivedInPubMed: Optional[str] = None
    dateLastModified: Optional[str] = None
    volume: Optional[str] = None
    abstract: Optional[str] = None
    citation: Optional[str] = None
    pubMedType: Optional[str] = None
    publisher: Optional[str] = None
    issueName: Optional[str] = None
    issueDate: Optional[str] = None
    resourceAbbreviation: Optional[str] = None
    resource: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaShow(BaseModelShow):
    curie: Optional[str] = None
    title: Optional[str] = None
    datePublished: Optional[str] = None
    dateArrivedInPubMed: Optional[str] = None
    dateLastModified: Optional[str] = None
    volume: Optional[str] = None
    abstract: Optional[str] = None
    citation: Optional[str] = None
    pubMedType: Optional[str] = None
    publisher: Optional[str] = None
    issueName: Optional[str] = None
    issueDate: Optional[str] = None
    resource: Optional[ResourceSchemaShow] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None

    class Config():
        orm_mode = True
        extra = "forbid"
