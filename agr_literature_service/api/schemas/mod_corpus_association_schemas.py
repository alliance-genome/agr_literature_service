from typing import Optional, List

from pydantic import BaseModel, ConfigDict, field_validator

from agr_literature_service.api.schemas import ModCorpusSortSourceType, AuditedObjectModelSchema


class ModCorpusAssociationSchemaCreate(BaseModel):
    """Schema for creating a mod-corpus association."""
    model_config = ConfigDict(
        extra='forbid',        # no unexpected fields
        from_attributes=True    # allow ORM->model initialization
    )

    mod_abbreviation: str
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    corpus: Optional[bool] = None


class ModCorpusAssociationSchemaShowID(BaseModel):
    """Schema for showing association by mod and reference IDs."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    mod_abbreviation: str
    reference_curie: str


class ModCorpusAssociationSchemaPost(ModCorpusAssociationSchemaCreate):
    """Schema for posting a mod-corpus association with reference context."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: str


class ModCorpusAssociationSchemaShow(AuditedObjectModelSchema):
    """Schema for showing full mod-corpus association with audit fields."""
    model_config = ConfigDict(
        extra='ignore',
        from_attributes=True
    )

    mod_corpus_association_id: int
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: str
    reference_curie: str
    corpus: Optional[bool] = None


class ModCorpusAssociationSchemaRelated(AuditedObjectModelSchema):
    """Schema for related mod-corpus association entries."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    mod_corpus_association_id: int
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    mod_abbreviation: str
    corpus: Optional[bool] = None


class ModCorpusAssociationSchemaUpdate(BaseModel):
    """Schema for updating a mod-corpus association."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    reference_curie: Optional[str] = None
    mod_corpus_sort_source: Optional[ModCorpusSortSourceType] = None
    corpus: Optional[bool] = None
    index_wft_id: Optional[str] = None
    force_out: Optional[bool] = None


class ModCorpusAssociationSchemaBatchUpdate(BaseModel):
    """Schema for batch updating mod-corpus associations."""
    model_config = ConfigDict(
        extra='forbid',
        from_attributes=True
    )

    mod_corpus_association_ids: List[int]
    corpus: bool
    force_out: Optional[bool] = False

    @field_validator('mod_corpus_association_ids')
    @classmethod
    def validate_ids_not_empty(cls, v):
        if not v:
            raise ValueError('mod_corpus_association_ids cannot be empty')
        if len(v) > 1000:
            raise ValueError('Cannot update more than 1000 associations at once')
        return v


class ModCorpusAssociationBatchResultItem(BaseModel):
    """Result for a single item in batch update."""
    mod_corpus_association_id: int
    success: bool
    message: str
    reference_curie: Optional[str] = None


class ModCorpusAssociationSchemaBatchResponse(BaseModel):
    """Response schema for batch update."""
    total_requested: int
    successful: int
    failed: int
    results: List[ModCorpusAssociationBatchResultItem]
