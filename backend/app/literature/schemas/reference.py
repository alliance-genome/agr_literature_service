from typing import List, Optional

from pydantic import BaseModel

from literature.schemas.base import BaseModelShow
from literature.schemas.author import AuthorSchemaPost
from literature.schemas.author import AuthorSchemaShow
from literature.schemas.editor import EditorSchemaPost
from literature.schemas.editor import EditorSchemaShow
from literature.schemas.reference_category import ReferenceCategory
from literature.schemas.mod_reference_type import ModReferenceTypeSchemaCreate
from literature.schemas.mod_reference_type import ModReferenceTypeSchemaShow
from literature.schemas.reference_tag import ReferenceTag
from literature.schemas.reference_tag import ReferenceTagShow
from literature.schemas.mesh_detail import MeshDetailSchemaCreate
from literature.schemas.mesh_detail import MeshDetailSchemaShow
from literature.schemas.cross_reference import CrossReferenceSchemaRelated
from literature.schemas.cross_reference import CrossReferenceSchema


class ReferenceSchemaPost(BaseModel):
    title: str
    category: ReferenceCategory

    citation: Optional[str]
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified: Optional[str] = None
    volume: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubmed_type: Optional[List[str]] = None
    mod_reference_types: Optional[List[ModReferenceTypeSchemaCreate]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    issue_date: Optional[str] = None
    tags: Optional[List[ReferenceTag]] = None
    mesh_terms: Optional[List[MeshDetailSchemaCreate]] = None
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    editors: Optional[List[EditorSchemaPost]] = None
    resource: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaUpdate(BaseModel):
    title: str
    category: ReferenceCategory

    citation: Optional[str] = None
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified: Optional[str] = None
    volume: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubmed_type: Optional[List[str]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    issue_date: Optional[str] = None
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    resource: Optional[str] = None

    class Config():
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaShow(BaseModelShow):
    curie: str = None
    title: str
    category: ReferenceCategory

    citation: Optional[str] = None
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified: Optional[str] = None
    volume: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubmed_type: Optional[List[str]] = None
    mod_reference_types: Optional[List[ModReferenceTypeSchemaShow]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    issue_date: Optional[str] = None
    tags: Optional[List[ReferenceTagShow]] = None
    mesh_terms: Optional[List[MeshDetailSchemaShow]] = None
    cross_references: Optional[List[CrossReferenceSchema]] = None
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None

    class Config():
        orm_mode = True
        extra = "forbid"
