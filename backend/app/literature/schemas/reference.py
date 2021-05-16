from typing import List, Optional

from pydantic import BaseModel

from literature.schemas.base import BaseModelShow
from literature.schemas.author import AuthorSchemaPost
from literature.schemas.author import AuthorSchemaShow
from literature.schemas.editor import EditorSchemaPost
from literature.schemas.editor import EditorSchemaShow
from literature.schemas.resource import ResourceSchemaShow
from literature.schemas.referenceCategory import ReferenceCategory
from literature.schemas.modReferenceType import ModReferenceType
from literature.schemas.reference_tag import ReferenceTag
from literature.schemas.mesh_detail import MeshDetail
from literature.schemas.crossReference import CrossReferenceRelated


class ReferenceSchemaPost(BaseModel):
    title: str
    datePublished: str
    category: ReferenceCategory
    citation: str

    dateArrivedInPubMed: Optional[str] = None
    dateLastModified: Optional[str] = None
    volume: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubMedType: Optional[List[str]] = None
    modReferenceTypes: Optional[List[ModReferenceType]] = None
    publisher: Optional[str] = None
    issueName: Optional[str] = None
    issueDate: Optional[str] = None
    tags: Optional[List[ReferenceTag]] = None
    mesh_terms: Optional[List[MeshDetail]] = None
    cross_references: Optional[List[CrossReferenceRelated]] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    editors: Optional[List[EditorSchemaPost]] = None
    resource: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaUpdate(BaseModel):
    title: str
    datePublished: str
    category: ReferenceCategory
    citation: str

    dateArrivedInPubMed: Optional[str] = None
    dateLastModified: Optional[str] = None
    volume: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubMedType: Optional[List[str]] = None
    modReferenceTypes: Optional[List[ModReferenceType]] = None
    publisher: Optional[str] = None
    issueName: Optional[str] = None
    issueDate: Optional[str] = None
    tags: Optional[List[ReferenceTag]] = None
    mesh_terms: Optional[List[MeshDetail]] = None
    cross_references: Optional[List[CrossReferenceRelated]] = None
    resource: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaShow(BaseModelShow):
    curie: str = None
    title: str
    datePublished: str
    category: ReferenceCategory
    citation: str

    dateArrivedInPubMed: Optional[str] = None
    dateLastModified: Optional[str] = None
    volume: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubMedType: Optional[List[str]] = None
    modReferenceTypes: Optional[List[ModReferenceType]] = None
    publisher: Optional[str] = None
    issueName: Optional[str] = None
    issueDate: Optional[str] = None
    tags: Optional[List[ReferenceTag]] = None
    mesh_terms: Optional[List[MeshDetail]] = None
    cross_references: Optional[List[CrossReferenceRelated]] = None
    resource: Optional[ResourceSchemaShow] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None

    class Config():
        orm_mode = True
        extra = "forbid"
