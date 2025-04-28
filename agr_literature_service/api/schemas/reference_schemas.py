from typing import List, Optional, Dict
from pydantic import ConfigDict, BaseModel, field_validator

from agr_literature_service.api.schemas import (AuthorSchemaPost, AuthorSchemaShow,
                                                AuditedObjectModelSchema, CrossReferenceSchemaRelated,
                                                CrossReferenceSchemaShow,
                                                MeshDetailSchemaCreate,
                                                MeshDetailSchemaRelated,
                                                ModReferenceTypeSchemaCreate,
                                                ModReferenceTypeSchemaRelated,
                                                ModCorpusAssociationSchemaCreate,
                                                PubMedPublicationStatus, ReferenceCategory,
                                                ReferenceRelationSchemaRelated,
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
    prepublication_pipeline: Optional[bool] = False
    workflow_tags: Optional[List[WorkflowTagSchemaCreate]] = None
    topic_entity_tags: Optional[List[TopicEntityTagSchemaCreate]] = None
    model_config = ConfigDict(from_attributes=True, extra="forbid")


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
    prepublication_pipeline: Optional[bool] = False

    @field_validator('title')
    def title_is_some(cls, v: str) -> str:
        if not v:
            raise ValueError('Cannot set title to None or blank string')
        return v

    @field_validator('category')
    def category_is_some(cls, v):
        if not v:
            raise ValueError('Cannot set category to None or blank string')
        return v
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class ReferenceSchemaAddPmid(BaseModel):
    pubmed_id: str
    mod_mca: str
    mod_curie: Optional[str] = ''

    @field_validator('mod_curie')
    def mod_curie_is_valid(cls, v):
        if v:
            if v == '':
                return v
            if v.count(":") != 1:
                raise ValueError('Malformed MOD curie, must have colon')
            mod_curie_prefix, mod_curie_id = v.split(":")
            if len(mod_curie_prefix) == 0 or len(mod_curie_id) == 0:
                raise ValueError('Malformed MOD curie, must have prefix and identifier')
        return v


class ReferenceRelationSchemaRelations(BaseModel):
    to_references: Optional[List[ReferenceRelationSchemaRelated]] = None
    from_references: Optional[List[ReferenceRelationSchemaRelated]] = None


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
    pubmed_publication_status: Optional[PubMedPublicationStatus]
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
    mesh_terms: Optional[List[MeshDetailSchemaRelated]] = None
    cross_references: Optional[List[CrossReferenceSchemaRelated]] = None
    prepublication_pipeline: Optional[bool]
    resource_curie: Optional[str] = None
    resource_title: Optional[str] = None
    copyright_license_name: Optional[str] = None
    copyright_license_url: Optional[str] = None
    copyright_license_description: Optional[str] = None
    copyright_license_open_access: Optional[bool] = None
    copyright_license_last_updated_by: Optional[str] = None
    invalid_cross_reference_ids: Optional[List[str]] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    reference_relations: ReferenceRelationSchemaRelations
    citation: Optional[str] = None
    citation_short: Optional[str] = None
    citation_id: Optional[int] = None
    workflow_tags: Optional[List[WorkflowTagSchemaRelated]] = None


class ReferenceSchemaNeedReviewShow(BaseModel):
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
    workflow_tags: Optional[List] = []
    copyright_license_name: Optional[str] = None
    copyright_license_url: Optional[str] = None
    copyright_license_description: Optional[str] = None
    copyright_license_open_access: Optional[str] = None
    authors: Optional[List[AuthorSchemaShow]] = None
    referencefiles: Optional[List[ReferencefileSchemaRelated]] = None
