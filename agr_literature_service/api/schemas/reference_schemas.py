from typing import List, Optional

from pydantic import BaseModel, validator

from agr_literature_service.api.schemas import (AuthorSchemaPost, AuthorSchemaShow,
                                                AuditedObjectModelSchema, CrossReferenceSchemaRelated,
                                                CrossReferenceSchemaShow,
                                                MeshDetailSchemaCreate,
                                                MeshDetailSchemaRelated,
                                                ModReferenceTypeSchemaCreate,
                                                ModReferenceTypeSchemaRelated,
                                                ModCorpusAssociationSchemaCreate,
                                                PubMedPublicationStatus, ReferenceCategory,
                                                ReferenceCommentAndCorrectionSchemaRelated,
                                                ReferencefileSchemaRelated,
                                                ModCorpusAssociationSchemaRelated)
from agr_literature_service.api.schemas.cross_reference_schemas import CrossReferenceSchemaCreate
from agr_literature_service.api.schemas.workflow_tag_schemas import WorkflowTagSchemaCreate, WorkflowTagSchemaRelated
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaCreate


class ReferenceSchemaPost(BaseModel):
    title: Optional[str] = None
    category: Optional[ReferenceCategory] = None
    date_published: Optional[str] = None
    date_published_start: Optional[str] = None
    date_published_end: Optional[str] = None
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
    cross_references: Optional[List[CrossReferenceSchemaCreate]] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    resource: Optional[str] = None
    workflow_tags: Optional[List[WorkflowTagSchemaCreate]] = None
    topic_entity_tags: Optional[List[TopicEntityTagSchemaCreate]] = None

    class Config:
        orm_mode = True
        extra = "forbid"


class ReferenceSchemaUpdate(BaseModel):
    title: Optional[str] = None
    category: Optional[ReferenceCategory] = None
    date_published: Optional[str] = None
    date_published_start: Optional[str] = None
    date_published_end: Optional[str] = None
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


class ReferenceSchemaShow(AuditedObjectModelSchema):
    reference_id: int
    curie: str

    title: Optional[str] = None
    category: Optional[ReferenceCategory] = None
    resource_id: Optional[int] = None
    copyright_license_id: Optional[int] = None
    date_published: Optional[str] = None
    date_published_start: Optional[str] = None
    date_published_end: Optional[str] = None
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
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    copyright_license_name: Optional[str] = None
    copyright_license_url: Optional[str] = None
    copyright_license_description: Optional[str] = None
    copyright_license_open_access: Optional[bool] = None
    copyright_license_last_updated_by: Optional[str] = None
    invalid_cross_reference_ids: Optional[List[str]] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    comment_and_corrections: CommentAndCorrectionSchemaRelations
    citation: Optional[str] = None
    citation_short: Optional[str] = None
    citation_id: Optional[int] = None
    workflow_tags: Optional[List[WorkflowTagSchemaRelated]] = None


class ReferenceSchemaNeedReviewShow(BaseModel):
    curie: str
    title: Optional[str] = None
    category: Optional[str] = None
    abstract: Optional[str] = None
    mod_corpus_association_id: int
    resource_title: Optional[str] = None
    cross_references: Optional[List[CrossReferenceSchemaShow]]
    workflow_tags: Optional[List] = []
    copyright_license_name: Optional[str] = None
    copyright_license_url: Optional[str] = None
    copyright_license_description: Optional[str] = None
    copyright_license_open_access: Optional[str] = None
    referencefiles: Optional[List[ReferencefileSchemaRelated]]
