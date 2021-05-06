from typing import List, Optional, Any
from datetime import datetime

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import validator

from literature.models import Reference
from literature.models import Resource


class BaseModelShow(BaseModel):
    dateCreated: Optional[datetime] = None
    dateUpdated: Optional[datetime] = None


class AuthorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    firstName: Optional[str] = None
    middleNames: Optional[List[str]] = None
    lastName: Optional[str] = None

    correspondingAuthor: Optional[bool] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class AuthorSchemaShow(AuthorSchemaPost):
    author_id: int

    class Config():
        orm_mode = True
        extra = "forbid"

class AuthorSchemaCreate(AuthorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class AuthorSchemaUpdate(AuthorSchemaShow):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaPost(BaseModel):
    order: Optional[int] = None

    name: Optional[str]  = None
    firstName: Optional[str] = None
    middleNames: Optional[List[str]] = None
    lastName: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaShow(EditorSchemaPost):
    editor_id: int

    class Config():
        orm_mode = True
        extra = "forbid"

class EditorSchemaCreate(EditorSchemaPost):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class EditorSchemaUpdate(EditorSchemaShow):
    reference_curie: Optional[str] = None
    resource_curie: Optional[str] = None

    class Config():
        orm_mode = True


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
















class User(BaseModel):
    name:str
    email:str
    password:str

class ShowUser(BaseModel):
    name:str
    email:str
    resources : List[ResourceSchemaShow] =[]
    class Config():
        orm_mode = True

class Login(BaseModel):
    username: str
    password:str


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: Optional[str] = None
