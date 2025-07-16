from typing import Optional

from pydantic import BaseModel, ConfigDict

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
        extra='forbid',
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
