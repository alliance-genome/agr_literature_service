from typing import List, Optional

from pydantic import BaseModel
from pydantic import validator

from literature.schemas import BaseModelShow
from literature.schemas import AuthorSchemaPost
from literature.schemas import AuthorSchemaShow
from literature.schemas import EditorSchemaPost
from literature.schemas import EditorSchemaShow
from literature.schemas import ReferenceCategory
from literature.schemas import ModReferenceTypeSchemaCreate
from literature.schemas import ModReferenceTypeSchemaRelated
from literature.schemas import ReferenceTag
from literature.schemas import ReferenceTagShow
from literature.schemas import MeshDetailSchemaCreate
from literature.schemas import MeshDetailSchemaRelated
from literature.schemas import CrossReferenceSchemaRelated
from literature.schemas import CrossReferenceSchemaShow
from literature.schemas import ReferenceCommentAndCorrectionSchemaRelated


class ReferenceSchemaPost(BaseModel):
    title: str
    category: ReferenceCategory

    citation: Optional[str]
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified: Optional[str] = None
    volume: Optional[str] = None
    plain_language_abstract: Optional[str] = None
    pubmed_abstract_languages: Optional[List[str]] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    merged_into_reference_curie: Optional[str] = None
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
    title: Optional[str] = None
    category: Optional[ReferenceCategory] = None

    citation: Optional[str] = None
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified: Optional[str] = None
    volume: Optional[str] = None
    plain_language_abstract: Optional[str] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    merged_into_reference_curie: Optional[str] = None
    pubmed_type: Optional[List[str]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    issue_date: Optional[str] = None
    resource: Optional[str] = None

    @validator('title')
    def title_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set title to None')
        return v

    @validator('category')
    def category_is_some(cls, v):
        if v is None:
            raise ValueError('Cannot set catagory to None')
        return v

    class Config():
        orm_mode = True
        extra = "forbid"


class CommentAndCorrectionSchemaRelations(BaseModel):
    to_references: Optional[List[ReferenceCommentAndCorrectionSchemaRelated]] = None
    from_references: Optional[List[ReferenceCommentAndCorrectionSchemaRelated]] = None


class ReferenceSchemaShow(BaseModelShow):
    reference_id: int
    curie: str
    title: str
    category: ReferenceCategory

    resource_id: Optional[int] = None
    citation: Optional[str] = None
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified: Optional[str] = None
    volume: Optional[str] = None
    plain_language_abstract: Optional[str] = None
    pubmed_abstract_languages: Optional[List[str]] = None
    language: Optional[str] = None
    pages: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    merged_into_reference_curie: Optional[str] = None
    mergee_reference_curies: Optional[List[str]] = None
    pubmed_type: Optional[List[str]] = None
    mod_reference_types: Optional[List[ModReferenceTypeSchemaRelated]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    issue_date: Optional[str] = None
    tags: Optional[List[ReferenceTagShow]] = None
    mesh_terms: Optional[List[MeshDetailSchemaRelated]] = None
    cross_references: Optional[List[CrossReferenceSchemaShow]] = None
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    editors: Optional[List[EditorSchemaShow]] = None
    comment_and_corrections: CommentAndCorrectionSchemaRelations = None
