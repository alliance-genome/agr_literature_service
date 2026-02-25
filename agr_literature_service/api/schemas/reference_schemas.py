from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agr_literature_service.api.schemas import (
    AuthorSchemaPost,
    AuthorSchemaShow,
    ModSchemaShow,
    AuditedObjectModelSchema,
    CrossReferenceSchemaRelated,
    CrossReferenceSchemaShow,
    MeshDetailSchemaCreate,
    MeshDetailSchemaRelated,
    ModReferenceTypeSchemaCreate,
    ModReferenceTypeSchemaRelated,
    ModCorpusAssociationSchemaCreate,
    ModCorpusAssociationSchemaRelated,
    PubMedPublicationStatus,
    ReferenceCategory,
    ReferenceRelationSchemaRelated,
    ReferencefileSchemaRelated,
)
from agr_literature_service.api.schemas.cross_reference_schemas import CrossReferenceSchemaCreate
from agr_literature_service.api.schemas.workflow_tag_schemas import WorkflowTagSchemaCreate, WorkflowTagSchemaRelated
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaCreate


class ReferenceSchemaPost(BaseModel):
    """Schema for creating or posting a reference record."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

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
    retraction_status: Optional[str] = None
    mesh_terms: Optional[List[MeshDetailSchemaCreate]] = None
    cross_references: Optional[List[CrossReferenceSchemaCreate]] = None
    authors: Optional[List[AuthorSchemaPost]] = None
    resource: Optional[str] = None
    prepublication_pipeline: Optional[bool] = False
    workflow_tags: Optional[List[WorkflowTagSchemaCreate]] = None
    topic_entity_tags: Optional[List[TopicEntityTagSchemaCreate]] = None


class ReferenceSchemaUpdate(BaseModel):
    """Schema for updating reference fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

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
    retraction_status: Optional[str] = None
    resource: Optional[str] = None
    prepublication_pipeline: Optional[bool] = False

    @field_validator('title')
    def title_is_some(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v.strip() == '':
            raise ValueError('Cannot set title to blank string')
        return v

    @field_validator('category')
    def category_is_some(cls, v: Optional[ReferenceCategory]) -> Optional[ReferenceCategory]:
        if v is None:
            raise ValueError('Cannot set category to None')
        return v


class ReferenceSchemaAddPmid(BaseModel):
    """Schema for adding a PubMed ID to a reference."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    pubmed_id: str
    mod_mca: str
    mod_curie: Optional[str] = ''

    @field_validator('mod_curie')
    def mod_curie_is_valid(cls, v: Optional[str]) -> Optional[str]:
        if v and v != '':
            if v.count(':') != 1:
                raise ValueError('Malformed MOD curie, must have single colon')
            prefix, ident = v.split(':')
            if not prefix or not ident:
                raise ValueError('Malformed MOD curie, prefix and identifier required')
        return v


class ReferenceRelationSchemaRelations(BaseModel):
    """Schema grouping related references by relationship type."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    to_references: Optional[List[ReferenceRelationSchemaRelated]] = None
    from_references: Optional[List[ReferenceRelationSchemaRelated]] = None


class ReferenceEmailSchemaRelated(BaseModel):
    """Schema for emails associated with a reference via reference_email."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    reference_email_id: int
    email_id: int
    email_address: str
    person_id: Optional[int] = None
    primary: Optional[bool] = None
    date_invalidated: Optional[str] = None


class ReferenceSchemaShow(AuditedObjectModelSchema):
    """Schema for showing full reference details with relationships and metadata."""
    model_config = ConfigDict(
        extra='ignore',
        from_attributes=True,
    )

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
    resources_for_curation: Optional[List[Dict[str, str]]] = None
    publisher: Optional[str] = None
    issue_name: Optional[str] = None
    retraction_status: Optional[str] = None
    # mesh_term: Optional[List[MeshDetailSchemaRelated]] = None  # allow singular mesh_term
    mesh_terms: Optional[List[MeshDetailSchemaRelated]] = None
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    prepublication_pipeline: Optional[bool] = None
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    copyright_license_name: Optional[str] = None
    copyright_license_url: Optional[str] = None
    copyright_license_description: Optional[str] = None
    copyright_license_open_access: Optional[bool] = None
    copyright_license_last_updated_by: Optional[str] = None
    invalid_cross_reference_ids: Optional[List[str]] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    emails: Optional[List[ReferenceEmailSchemaRelated]] = None
    reference_relations: ReferenceRelationSchemaRelations = Field(default_factory=ReferenceRelationSchemaRelations)
    citation: Optional[str] = None
    citation_short: Optional[str] = None
    citation_id: Optional[int] = None
    workflow_tags: Optional[List[WorkflowTagSchemaRelated]] = None
    mod: Optional[ModSchemaShow] = None


class ReferenceSchemaNeedReviewShow(BaseModel):
    """Schema for showing references needing review with essential fields."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True,
    )

    @field_validator('copyright_license_open_access', mode='before')
    def _convert_open_access(cls, v):
        if v in (None, ''):
            return False
        return v

    curie: str
    title: Optional[str] = None
    category: Optional[str] = None
    pubmed_publication_status: Optional[PubMedPublicationStatus] = None
    abstract: Optional[str] = None
    mod_corpus_association_id: int
    mod_corpus_association_corpus: Optional[bool] = None
    prepublication_pipeline: Optional[bool] = None
    resource_title: Optional[str] = None
    cross_references: Optional[List[CrossReferenceSchemaShow]] = None
    workflow_tags: Optional[List[Any]] = Field(default_factory=list)
    copyright_license_name: Optional[str] = None
    copyright_license_url: Optional[str] = None
    copyright_license_description: Optional[str] = None
    copyright_license_open_access: Optional[bool] = False
    authors: Optional[List[AuthorSchemaShow]] = None
    referencefiles: Optional[List[ReferencefileSchemaRelated]] = None
