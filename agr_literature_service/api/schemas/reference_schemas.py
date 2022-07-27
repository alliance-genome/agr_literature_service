from typing import List, Optional

from pydantic import BaseModel, validator

from agr_literature_service.api.schemas import (AuthorSchemaPost, AuthorSchemaShow,
                                                BaseModelShow, CrossReferenceSchemaRelated,
                                                CrossReferenceSchemaShow,
                                                MeshDetailSchemaCreate,
                                                MeshDetailSchemaRelated,
                                                ModReferenceTypeSchemaCreate,
                                                ModReferenceTypeSchemaRelated,
                                                ModCorpusAssociationSchemaCreate,
                                                PubMedPublicationStatus, ReferenceCategory,
                                                ReferenceCommentAndCorrectionSchemaRelated,
                                                ModCorpusAssociationSchemaRelated,
                                                # WorkflowTagSchemaCreate,
                                                WorkflowTagSchemaRelated)


class ReferenceSchemaPost(BaseModel):
    title: str
    category: ReferenceCategory

    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified_in_pubmed: Optional[str] = None
    volume: Optional[str] = None
    plain_language_abstract: Optional[str] = None
    pubmed_abstract_languages: Optional[List[str]] = None
    pubmed_publication_status: Optional[PubMedPublicationStatus] = None
    language: Optional[str] = None
    page_range: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubmed_types: Optional[List[str]] = None
    mod_reference_types: Optional[List[ModReferenceTypeSchemaCreate]] = None
    mod_corpus_associations: Optional[List[ModCorpusAssociationSchemaCreate]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    mesh_terms: Optional[List[MeshDetailSchemaCreate]] = None
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    resource: Optional[str] = None
    open_access: Optional[bool] = None
    citation: Optional[str] = None
    ontologies: Optional[List[WorkflowTagSchemaRelated]] = None

    class Config():
        orm_mode = True
        extra = "forbid"

    @validator('title')
    def title_is_some(cls, v: str) -> str:
        if not v:
            raise ValueError('Cannot set title to None or blank string')
        return v

    @validator('category')
    def category_is_some(cls, v):
        if not v:
            raise ValueError('Cannot set catagory to None or blank string')
        return v


class ReferenceSchemaUpdate(BaseModel):
    title: Optional[str] = None
    category: Optional[ReferenceCategory] = None

    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified_in_pubmed: Optional[str] = None
    volume: Optional[str] = None
    plain_language_abstract: Optional[str] = None
    language: Optional[str] = None
    page_range: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubmed_types: Optional[List[str]] = None
    pubmed_abstract_languages: Optional[List[str]] = None
    pubmed_publication_status: Optional[PubMedPublicationStatus] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    resource: Optional[str] = None
    open_access: Optional[bool] = False

    @validator('title')
    def title_is_some(cls, v: str) -> str:
        if not v:
            raise ValueError('Cannot set title to None or blank string')
        return v

    @validator('category')
    def category_is_some(cls, v):
        if not v:
            raise ValueError('Cannot set catagory to None or blank string')
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
    date_published: Optional[str] = None
    date_arrived_in_pubmed: Optional[str] = None
    date_last_modified_in_pubmed: Optional[str] = None
    volume: Optional[str] = None
    plain_language_abstract: Optional[str] = None
    pubmed_abstract_languages: Optional[List[str]] = None
    pubmed_publication_status: Optional[PubMedPublicationStatus] = None
    language: Optional[str] = None
    page_range: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[List[str]] = None
    pubmed_types: Optional[List[str]] = None
    mod_reference_types: Optional[List[ModReferenceTypeSchemaRelated]] = None
    mod_corpus_associations: Optional[List[ModCorpusAssociationSchemaRelated]] = None
    obsolete_references: Optional[List[str]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    mesh_terms: Optional[List[MeshDetailSchemaRelated]] = None
    cross_references: Optional[List[CrossReferenceSchemaShow]] = None
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    comment_and_corrections: CommentAndCorrectionSchemaRelations
    open_access: Optional[bool] = None
    citation: Optional[str] = None
    ontologies: Optional[List[WorkflowTagSchemaRelated]] = None


class ReferenceSchemaNeedReviewShow(BaseModel):
    curie: str
    title: str
    abstract: Optional[str] = None
    mod_corpus_association_id: int
    resource_title: Optional[str] = None
    cross_references: Optional[List[CrossReferenceSchemaShow]]
